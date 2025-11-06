# acc_suite.py — ACC logger "light" + summary scheduler (2 min)
# - Eventi NDJSON: session_start/session_end, stint_start/stint_end, sector_split, lap_complete, wx, hb
# - Summary JSON (ogni 120s e a chiusura): struttura minimale richiesta
#
# Uso:
#   python acc_suite.py --hz 5 --wx-every 2.0 --hb-every 0.5 --summary-interval 120
#   (5 Hz ≈ 200 ms; il JSON si aggiorna ogni 120s)
#
# File prodotti (in ./logs):
#   acc_YYYYmmdd_HHMMSS.ndjson
#   summary_YYYYmmdd_HHMMSS.json  (aggiornato periodicamente)

import argparse, json, os, sys, time, threading, signal
from datetime import datetime, timezone
from statistics import mean
from collections import Counter

# ========= util path/file =========

def base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

DEFAULT_OUT_DIR = os.path.join(base_dir(), "logs")
DEFAULT_SUMMARY_DIR = os.path.join(base_dir(), "logs_json")

def read_summary_dir():
    """
    Legge la prima riga di config.txt (nella stessa cartella di acc_suite.py)
    e la usa come cartella di output per i summary JSON.
    Se mancante o vuota, usa DEFAULT_SUMMARY_DIR.
    """
    cfg = os.path.join(base_dir(), "config.txt")
    try:
        if os.path.exists(cfg):
            with open(cfg, "r", encoding="utf-8") as f:
                p = f.readline().strip().strip('"')
                if p:
                    return p
    except:
        pass
    return DEFAULT_SUMMARY_DIR


def ensure_dir(d): os.makedirs(d, exist_ok=True)

def iso_now():
    dt = datetime.now(timezone.utc)
    return dt.replace(microsecond=int(dt.microsecond/1000)*1000).isoformat().replace("+00:00","Z")

def open_out(out_dir, prefix):
    ensure_dir(out_dir)
    fname = os.path.join(out_dir, f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.ndjson")
    fh = open(fname, "w", encoding="utf-8")
    print(f"[FILE] NDJSON: {fname}")
    return fname, fh

def write_line(fh, obj):
    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
    fh.flush()

def r1(x):
    try:
        return None if x is None else round(float(x), 1)
    except: return None

SENTINEL_LAST_BEST_MS = 2147483647
def as_int_ms(x):
    try:
        if x is None: return None
        v = int(round(float(x)))
        if v <= 0 or v >= 100*60*1000: return None
        if v == SENTINEL_LAST_BEST_MS: return None
        return v
    except: return None

def clean_str(s):
    if s is None: return None
    return s.split("\x00",1)[0] if isinstance(s,str) else str(s)

# ========= logger (SHM event-based) =========

class ACCLogger:
    def __init__(self, out_dir, out_prefix, hz, wx_every, hb_every, wx_delta):
        self.out_dir = out_dir
        self.out_prefix = out_prefix
        self.hz = hz
        self.wx_every = wx_every
        self.hb_every = hb_every
        self.wx_delta = wx_delta

        self.asm = None
        self.ndjson_path = None
        self.fh = None

        # stato
        self.prev_session_key = None
        self.have_session_open = False

        self.prev_in_pit_eff = None
        self.prev_sector_idx = None
        self.prev_completed = None
        self.lap_valid_acc = None
        self.lap_start_fuel = None

        self.car_model = None
        self.ballast = None
        self.restrictor = None

        self.last_wx_emit = 0.0
        self.last_hb_emit = 0.0
        self.last_wx_vals = {"air":None,"track":None,"grip":None,"rain":None,"clouds":None,"wind_spd":None,"wind_dir":None}

        self.stop_flag = False
        self.current_file_token = None  # per il scheduler summary

    def _open_new_file(self):
        if self.fh:
            try:
                write_line(self.fh, {"ts": iso_now(), "event": "session_end"})
            except: pass
            self.fh.close()
        self.ndjson_path, self.fh = open_out(self.out_dir, self.out_prefix)
        # token per abbinare summary -> questo ndjson
        self.current_file_token = os.path.splitext(os.path.basename(self.ndjson_path))[0].split("_",1)[1]

    def start(self):
        try:
            from pyaccsharedmemory import accSharedMemory
        except Exception:
            print("[ERRORE] Manca 'pyaccsharedmemory'. Installa:  pip install pyaccsharedmemory")
            raise

        self.asm = accSharedMemory()
        self._open_new_file()

        interval = max(0.01, 1.0/max(1.0, self.hz))
        print(f"[SHM] Lettura a {self.hz:.1f} Hz (wx_every={self.wx_every}s, hb_every={self.hb_every}s)")

        while not self.stop_flag:
            sm = self.asm.read_shared_memory()
            if sm is None:
                time.sleep(interval); continue

            phys = getattr(sm, "Physics", None)
            graf = getattr(sm, "Graphics", None)
            stat = getattr(sm, "Static", None)
            if not graf or not phys or not stat:
                time.sleep(interval); continue

            # ---- letture base
            session_type = clean_str(getattr(graf,"session_type",None))
            track        = clean_str(getattr(stat,"track",None))
            sector_count = getattr(stat,"sector_count",None)

            current_ms = as_int_ms(getattr(graf,"current_time",None))
            last_ms    = as_int_ms(getattr(graf,"last_time",None))
            split_ms   = as_int_ms(getattr(graf,"last_sector_time",None))

            amb_g = getattr(graf,"ambient_temp",None); trk_g = getattr(graf,"road_temp",None)
            amb_p = getattr(phys,"air_temp",None);     trk_p = getattr(phys,"road_temp",None)
            air = r1(amb_g if amb_g is not None else amb_p)
            track_t = r1(trk_g if trk_g is not None else trk_p)

            grip = clean_str(getattr(graf,"track_grip_status",None)) or clean_str(getattr(graf,"track_grip",None))
            rain_int = clean_str(getattr(graf,"rain_intensity",None))
            clouds = r1(getattr(graf,"cloud_level",None))
            wind_spd = r1(getattr(graf,"wind_speed",None))
            wind_dir = r1(getattr(graf,"wind_direction",None))

            is_in_pit = bool(getattr(graf,"is_in_pit",False))
            is_in_pit_lane = bool(getattr(graf,"is_in_pit_lane",False))
            in_pit_eff = is_in_pit or is_in_pit_lane
            cur_sector_idx = getattr(graf,"current_sector_index",None)
            is_valid_flag = getattr(graf,"is_valid_lap",None)
            if is_valid_flag is not None: is_valid_flag = bool(is_valid_flag)

            fuel_now = r1(getattr(phys,"fuel",None))
            speed_kmh = r1(getattr(phys,"speed_kmh",None))
            completed_lap = getattr(graf,"completed_lap",None)

            # static+graf info (auto/BOP)
            self.car_model = clean_str(getattr(stat,"car_model",None)) or self.car_model
            self.ballast   = getattr(graf,"ballast",None) if getattr(graf,"ballast",None) is not None else self.ballast
            self.restrictor= getattr(graf,"restrictor",None) if getattr(graf,"restrictor",None) is not None else self.restrictor

            now_iso = iso_now()
            now_mono = time.monotonic()

            # ---- rotazione file a cambio sessione
            session_key = (session_type, track)
            if self.prev_session_key is None:
                # prima volta: session_start e nuovo file NDJSON
                self._open_new_file()
                write_line(self.fh, {"ts": now_iso,"event":"session_start",
                                     "session_type": session_type, "track": track,
                                     "sector_count": sector_count,
                                     "car_model": self.car_model})
                self.have_session_open = True
            elif session_key != self.prev_session_key:
                # chiudi sessione corrente e apri nuovo file NDJSON
                if self.have_session_open:
                    write_line(self.fh, {"ts": now_iso,"event":"session_end"})
                    self.have_session_open = False
                self._open_new_file()
                write_line(self.fh, {"ts": now_iso,"event":"session_start",
                                     "session_type": session_type, "track": track,
                                     "sector_count": sector_count,
                                     "car_model": self.car_model})
                self.have_session_open = True
                # reset stato
                self.prev_in_pit_eff = None
                self.prev_sector_idx = None
                self.prev_completed  = None
                self.lap_valid_acc   = None
                self.lap_start_fuel  = None

            self.prev_session_key = session_key

            # ---- transizioni pit (stint start/end)
            if self.prev_in_pit_eff is None:
                self.prev_in_pit_eff = in_pit_eff
                if in_pit_eff is False:
                    write_line(self.fh, {"ts": now_iso,"event":"stint_start",
                                         "fuel_start": fuel_now, "in_pit_lane": False,
                                         "completed_lap": completed_lap,
                                         "car_model": self.car_model})
                    self.prev_completed = completed_lap
                    self.lap_valid_acc = None
                    self.lap_start_fuel = fuel_now
                    self.prev_sector_idx = cur_sector_idx
            else:
                if self.prev_in_pit_eff and (in_pit_eff is False):
                    write_line(self.fh, {"ts": now_iso,"event":"stint_start",
                                         "fuel_start": fuel_now, "in_pit_lane": False,
                                         "completed_lap": completed_lap,
                                         "car_model": self.car_model})
                    self.prev_completed = completed_lap
                    self.lap_valid_acc = None
                    self.lap_start_fuel = fuel_now
                    self.prev_sector_idx = cur_sector_idx
                if (self.prev_in_pit_eff is False) and in_pit_eff:
                    write_line(self.fh, {"ts": now_iso,"event":"stint_end",
                                         "fuel_end": fuel_now, "in_pit_lane": True})
                self.prev_in_pit_eff = in_pit_eff

            # ---- validità giro (AND)
            if is_valid_flag is not None:
                if self.lap_valid_acc is None: self.lap_valid_acc = bool(is_valid_flag)
                else: self.lap_valid_acc = self.lap_valid_acc and bool(is_valid_flag)

            # ---- sector split (0->1 / 1->2)
            if cur_sector_idx is not None and self.prev_sector_idx is not None and cur_sector_idx != self.prev_sector_idx:
                if (self.prev_sector_idx == 0 and cur_sector_idx == 1) or (self.prev_sector_idx == 1 and cur_sector_idx == 2):
                    if split_ms and split_ms > 0:
                        write_line(self.fh, {"ts": now_iso,"event":"sector_split",
                                             "lap": completed_lap, "sector_index": int(self.prev_sector_idx),
                                             "split_ms": int(split_ms)})
                self.prev_sector_idx = cur_sector_idx
            elif self.prev_sector_idx is None and cur_sector_idx is not None:
                self.prev_sector_idx = cur_sector_idx

            # ---- lap complete (incremento completed_lap)
            if completed_lap is not None and self.prev_completed is not None and completed_lap > self.prev_completed:
                lap_ms = last_ms if last_ms is not None else None
                write_line(self.fh, {"ts": now_iso,"event":"lap_complete",
                                     "lap": int(completed_lap),
                                     "lap_ms": (int(lap_ms) if lap_ms is not None else None),
                                     "is_valid": (bool(self.lap_valid_acc) if self.lap_valid_acc is not None else None),
                                     "fuel_start": self.lap_start_fuel,
                                     "fuel_end": fuel_now})
                self.prev_completed = completed_lap
                self.lap_valid_acc = None
                self.lap_start_fuel = fuel_now
            elif self.prev_completed is None:
                self.prev_completed = completed_lap

            # ---- meteo/grip (wx)
            if self.wx_every > 0:
                def changed(a,b,thr=0.5):
                    if a is None and b is None: return False
                    if a is None or b is None: return True
                    return abs(a-b) >= thr
                need = False
                if changed(self.last_wx_vals["air"], air, self.wx_delta) or changed(self.last_wx_vals["track"], track_t, self.wx_delta): need = True
                if grip != self.last_wx_vals["grip"] or rain_int != self.last_wx_vals["rain"]: need = True
                if self.last_wx_vals["wind_spd"] != wind_spd or self.last_wx_vals["wind_dir"] != wind_dir: need = True
                if (time.monotonic() - self.last_wx_emit) >= self.wx_every: need = True
                if need:
                    write_line(self.fh, {"ts": now_iso,"event":"wx",
                                         "air_temp": air, "track_temp": track_t,
                                         "grip": grip, "rain_intensity": rain_int, "cloud_level": clouds,
                                         "wind_speed": wind_spd, "wind_direction": wind_dir,
                                         "ballast": self.ballast, "restrictor": self.restrictor,
                                         "car_model": self.car_model})
                    self.last_wx_emit = time.monotonic()
                    self.last_wx_vals = {"air":air,"track":track_t,"grip":grip,"rain":rain_int,"clouds":clouds,
                                         "wind_spd":wind_spd,"wind_dir":wind_dir}

            # ---- heartbeat (hb)
            if self.hb_every > 0 and (time.monotonic()-self.last_hb_emit) >= self.hb_every:
                write_line(self.fh, {"ts": now_iso,"event":"hb",
                                     "fuel": fuel_now, "speed_kmh": speed_kmh,
                                     "is_valid_lap": (bool(is_valid_flag) if is_valid_flag is not None else None)})
                self.last_hb_emit = time.monotonic()

            time.sleep(interval)

        # uscita loop
        try:
            if self.have_session_open:
                write_line(self.fh, {"ts": iso_now(),"event":"session_end"})
        except: pass

        try:
            if self.asm: self.asm.close()
        except: pass
        if self.fh: self.fh.close()

# ========= summary (parser + writer) =========

FUEL_QUALY_THRESHOLD = 20.0  # >20 = race ; <=20 = qualy

def parse_iso(ts):
    try:
        if ts and ts.endswith("Z"): ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts) if ts else None
    except: return None

def fmt_hms(ms):
    if ms is None: return None
    ms = int(ms)
    total_sec = ms/1000.0
    m = int(total_sec//60); s = total_sec - m*60
    return f"{m}:{s:06.3f}"

def load_events(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: yield json.loads(line)
            except: continue

def build_sessions(path):
    sessions=[]
    cur=None
    last_ts=None
    for ev in load_events(path):
        ts=parse_iso(ev.get("ts")); 
        if ts: last_ts=ts
        et=ev.get("event")
        if et=="session_start":
            if cur is not None:
                cur["end"]=cur.get("end") or last_ts
                sessions.append(cur)
            cur={"start":ts,"end":None,"type":ev.get("session_type"),
                 "track":ev.get("track"),
                 "stints":[],
                 "wx_air":[],"wx_grip":[],
                 "car_model": ev.get("car_model"),
                 "bop_ballast": None, "bop_restrictor": None}
        elif et=="session_end":
            if cur is not None:
                # CHIUDI eventuali stint rimasti aperti
                if cur.get("stints"):
                    for st in cur["stints"]:
                        if st.get("end") is None:
                            st["end"] = ts or last_ts  # chiusura al timestamp di fine sessione (o last_ts)
                # chiudi e appendi la sessione
                cur["end"] = ts or last_ts
                sessions.append(cur)
                cur = None

        elif et=="stint_start":
            if cur is not None:
                cur["stints"].append({"start":ts,"end":None,"fuel_start":ev.get("fuel_start"),
                                      "laps":[]})
                if ev.get("car_model"): cur["car_model"]=ev.get("car_model")
        elif et=="stint_end":
            if cur and cur["stints"]:
                st=cur["stints"][-1]
                if st["end"] is None: st["end"]=ts
        elif et=="lap_complete":
            if cur is not None:
                tgt=None
                if cur["stints"]:
                    last=cur["stints"][-1]
                    if last["end"] is None or (ts and last["start"] and (last["end"] or ts)>=ts):
                        tgt=last
                if tgt is None:
                    cur["stints"].append({"start":ts,"end":ts,"fuel_start":None,"laps":[]})
                    tgt=cur["stints"][-1]
                tgt["laps"].append({"lap":ev.get("lap"),
                                    "lap_ms":ev.get("lap_ms"),
                                    "is_valid":ev.get("is_valid"),
                                    "fuel_start":ev.get("fuel_start"),
                                    "fuel_end":ev.get("fuel_end")})
        elif et=="wx":
            if cur is not None:
                air=ev.get("air_temp")
                if isinstance(air,(int,float)): cur["wx_air"].append(float(air))
                g=ev.get("grip")
                if g is not None: cur["wx_grip"].append(str(g))
                if ev.get("ballast") is not None: cur["bop_ballast"]=ev.get("ballast")
                if ev.get("restrictor") is not None: cur["bop_restrictor"]=ev.get("restrictor")

    if cur is not None:
        # CHIUDI eventuali stint rimasti aperti
        if cur.get("stints"):
            for st in cur["stints"]:
                if st.get("end") is None:
                    st["end"] = last_ts  # fallback sicuro al last_ts visto
        # chiudi e appendi la sessione
        cur["end"] = cur.get("end") or last_ts
        sessions.append(cur)
    return sessions


def sessions_to_summary(path):
    sess = build_sessions(path)
    out = {"source_file": path.replace("\\", "/"), "sessions": []}

    for S in sess:
        # tempo guida = somma (end-start) degli stints chiusi
        drive_s = 0.0
        for st in S["stints"]:
            s0, s1 = st.get("start"), st.get("end")
            if isinstance(s0, datetime) and isinstance(s1, datetime):
                drive_s += (s1 - s0).total_seconds()

        # grip più frequente (normalizzato "ACC_*")
        from collections import Counter
        grip_mode = Counter(S["wx_grip"]).most_common(1)[0][0] if S["wx_grip"] else None
        if grip_mode:
            g = str(grip_mode).upper().replace(" ", "_")
            if not g.startswith("ACC_"):
                g = "ACC_" + g
            grip_mode = g

        # meteo medio
        air_avg = round(mean(S["wx_air"]), 1) if S["wx_air"] else None

        # laps totali chiusi
        total_closed = 0

        # best per race/qualy
        bests = {"race": None, "qualy": None}

        # Per aggiungere avg_time del best race dobbiamo sapere in quale stint è avvenuto
        best_race_stint_idx = None

        def pick_best(cur_best, cand_s, fuel_pitout):
            if cand_s is None:
                return cur_best
            if cur_best is None:
                return {
                    "lap_time_s": round(cand_s, 3),
                    "lap_time_hms": fmt_hms(int(cand_s * 1000)),
                    "fuel_pitout_l": r1(fuel_pitout)
                }
            if cand_s < cur_best["lap_time_s"]:
                cur_best.update({
                    "lap_time_s": round(cand_s, 3),
                    "lap_time_hms": fmt_hms(int(cand_s * 1000)),
                    "fuel_pitout_l": r1(fuel_pitout)
                })
            return cur_best

        # 1) Scansione stints/laps per total_closed e best (race/qualy)
        for idx, st in enumerate(S["stints"]):
            f0 = st.get("fuel_start")
            cls = "race" if (f0 is not None and float(f0) > FUEL_QUALY_THRESHOLD) else "qualy"
            for L in st.get("laps", []):
                lt_ms = L.get("lap_ms")
                if lt_ms is None:
                    continue
                total_closed += 1
                if L.get("is_valid") is True:
                    lt_s = round(int(lt_ms) / 1000.0, 3)
                    prev_best = bests[cls]
                    bests[cls] = pick_best(prev_best, lt_s, f0)
                    # Se abbiamo aggiornato il best race, ricordiamo lo stint
                    if cls == "race" and bests[cls] is not None:
                        # se abbiamo migliorato, o se prima era None
                        if (prev_best is None) or (bests[cls]["lap_time_s"] == lt_s and prev_best is None) or (lt_s < prev_best["lap_time_s"]):
                            best_race_stint_idx = idx

        # 2) Se esiste un best race, calcoliamo la media dei tempi nello stesso stint (tutti i giri chiusi)
        if bests["race"] is not None and best_race_stint_idx is not None:
            laps_ms = [
                L.get("lap_ms")
                for L in S["stints"][best_race_stint_idx].get("laps", [])
                if L.get("lap_ms") is not None
            ]
            if laps_ms:
                avg_ms = int(mean([int(x) for x in laps_ms]))
                bests["race"]["avg_time"] = fmt_hms(avg_ms)
            else:
                bests["race"]["avg_time"] = None  # nessun giro chiuso nello stint (caso limite)

        # costruisci blocco sessione
        start_local = (S["start"].astimezone().isoformat() if isinstance(S["start"], datetime) else None)
        sess_block = {
            "type": (S.get("type") if (S.get("type") and str(S["type"]).startswith("ACC_")) else f"ACC_{(S.get('type') or '').upper()}"),
            "track": S.get("track"),
            "time": {
                "start_local": start_local,
                "driving_duration_min": round(drive_s / 60.0, 3)
            },
            "grip": {"most_frequent": grip_mode},
            "weather": {"air_temp_c": {"avg": air_avg} if air_avg is not None else None},
            "laps": {"total_closed": total_closed},
            "bests": bests
        }
        out["sessions"].append(sess_block)

    return out


# ========= scheduler =========

class SummaryScheduler:
    def __init__(self, logger: ACCLogger, interval_s: int, summary_dir: str):
        self.logger = logger
        self.interval_s = max(30, int(interval_s))  # minimo 30s
        self.summary_dir = summary_dir              # <— nuova destinazione JSON
        self.stop_flag = False
        self.thread = threading.Thread(target=self._run, daemon=True)


    def start(self): self.thread.start()
    def stop(self): self.stop_flag = True; self.thread.join(timeout=3)

    def _run(self):
        # gira finché l'app è attiva
        while not self.stop_flag:
            time.sleep(self.interval_s)
            try:
                nd_path = self.logger.ndjson_path
                token = self.logger.current_file_token
                if not nd_path or not token: continue
                summary = sessions_to_summary(nd_path)
                ensure_dir(self.summary_dir)
                out_path = os.path.join(self.summary_dir, f"summary_{token}.json")

                with open(out_path,"w",encoding="utf-8") as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)
                print(f"[SUMMARY] Aggiornato: {out_path}")
            except Exception as e:
                print(f"[SUMMARY] Errore aggiornamento: {e}")

    def flush_now(self):
        try:
            nd_path = self.logger.ndjson_path
            token = self.logger.current_file_token
            if not nd_path or not token: return
            summary = sessions_to_summary(nd_path)
            ensure_dir(self.summary_dir)
            out_path = os.path.join(self.summary_dir, f"summary_{token}.json")

            with open(out_path,"w",encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            print(f"[SUMMARY] Flush finale: {out_path}")
        except Exception as e:
            print(f"[SUMMARY] Errore flush finale: {e}")

# ========= main =========

def main():
    ap = argparse.ArgumentParser(description="ACC Suite: logger light + summary scheduler")
    ap.add_argument("--hz", type=float, default=5.0, help="Frequenza lettura SHM (Hz)")
    ap.add_argument("--wx-every", type=float, default=2.0, help="Secondi tra eventi meteo (0=off)")
    ap.add_argument("--hb-every", type=float, default=0.5, help="Secondi tra heartbeat (0=off)")
    ap.add_argument("--wx-delta", type=float, default=0.5, help="Soglia variazione °C per forzare wx")
    ap.add_argument("--summary-interval", type=int, default=120, help="Aggiornamento summary (secondi)")
    ap.add_argument("--out-prefix", default="acc")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    ensure_dir(args.out_dir)
    # Leggi la cartella per i summary JSON da config.txt (o default)
    summary_dir = read_summary_dir()
    ensure_dir(summary_dir)
    print(f"[CONFIG] Summary JSON dir: {summary_dir}")


    logger = ACCLogger(out_dir=args.out_dir, out_prefix=args.out_prefix,
                       hz=args.hz, wx_every=args.wx_every, hb_every=args.hb_every, wx_delta=args.wx_delta)
    sched = SummaryScheduler(logger, interval_s=args.summary_interval, summary_dir=summary_dir)


    # graceful shutdown
    def handle_sig(signum, frame):
        print("\n[STOP] Chiusura richiesta...")
        sched.stop_flag = True
        logger.stop_flag = True
    signal.signal(signal.SIGINT, handle_sig)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_sig)

    try:
        sched.start()
        logger.start()
    finally:
        # flush finale del summary
        try:
            sched.flush_now()
        except: pass

if __name__ == "__main__":
    main()
