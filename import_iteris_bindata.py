"""
Convert Iteris Vantage exported bin-data text files into CSV.

Primary output matches typical TMC summary layout: Time (HHMM), EBL..SBR, Veh Total,
plus a Total row (same shape as Vision-style intersection summaries).

Uses direction bins only (zones 501–512: LT / Through / RT per sensor). Map each
sensor (C1–C4) to EB/WB/NB/SB via the Directions dialog or defaults.

Also writes *_detail.csv (long tidy rows) for all zone types.

For live pull from the camera via MQTT (/iteris/count/), see import_iteris_mqtt.py.
"""

from __future__ import annotations

import csv
import datetime
import re
import sys
from collections import defaultdict
from pathlib import Path

# TMC summary columns (match Vision / standard count sheet layout).
_CARDINALS = ("EB", "WB", "NB", "SB")
_LTR = ("L", "T", "R")
SUMMARY_FIELDNAMES = (
    ["Time"]
    + [f"{c}{m}" for c in _CARDINALS for m in _LTR]
    + ["Veh Total"]
)

_MOVEMENT_TO_COL_SUFFIX = {"LT": "L", "Through": "T", "RT": "R"}

# Column order for CSV output (remaining keys are appended alphabetically).
_CSV_KEY_ORDER = [
    "record_type",
    "source_file",
    "zone_number",
    "record_category",
    "movement",
    "sensor",
    "timestamp",
    "volume",
    "occupancy_pct",
    "avg_speed",
    "avg_ped_speed",
    "max_ped_speed",
    "min_ped_speed",
    "green_time_sec",
    "green_or_walk_sec",
    "volume_small_class",
    "volume_medium_class",
    "volume_large_class",
    "unused_1",
    "unused_2",
    "unused_3",
    "unused_4",
    "unused_5",
    "unused_6",
    "field_0",
    "field_1",
    "field_2",
    "field_3",
    "field_4",
    "field_5",
    "field_6",
    "video_status",
    "device_label",
    "field_count",
    "raw_line",
]

# --- Zone semantics: Iteris user guide §10.1 automatic bin zone numbers ---

DIR_LT = {501, 504, 507, 510}
DIR_T = {502, 505, 508, 511}
DIR_RT = {503, 506, 509, 512}

PED_LR = {601, 603, 605, 607}
PED_RL = {602, 604, 606, 608}

VECTOR_AS = {651, 652, 653, 654}

# Per-sensor auto counts: 701–710 cam1, 711–720 cam2, 721–730 cam3, 731–740 cam4
AUTO_MOVEMENT = {
    1: "R1",
    2: "R2",
    3: "T1",
    4: "T2",
    5: "T3",
    6: "T4",
    7: "T5",
    8: "L1",
    9: "L2",
    10: "L3",
}


def sensor_from_direction_zone(zone: int) -> int | None:
    if zone in (501, 502, 503):
        return 1
    if zone in (504, 505, 506):
        return 2
    if zone in (507, 508, 509):
        return 3
    if zone in (510, 511, 512):
        return 4
    return None


def sensor_from_ped_zone(zone: int) -> int | None:
    m = {601: 1, 602: 1, 603: 2, 604: 2, 605: 3, 606: 3, 607: 4, 608: 4}
    return m.get(zone)


def sensor_from_vector_zone(zone: int) -> int | None:
    return {651: 1, 652: 2, 653: 3, 654: 4}.get(zone)


def sensor_from_auto_zone(zone: int) -> int | None:
    if 701 <= zone <= 740:
        return (zone - 701) // 10 + 1
    return None


def auto_movement_code(zone: int) -> str | None:
    if 701 <= zone <= 740:
        idx = (zone - 701) % 10 + 1
        return AUTO_MOVEMENT.get(idx)
    return None


def classify_zone(zone: int) -> tuple[str, str]:
    """Return (record_category, short_movement_name)."""
    if zone in DIR_LT:
        return "direction_lt_through_rt", "LT"
    if zone in DIR_T:
        return "direction_lt_through_rt", "Through"
    if zone in DIR_RT:
        return "direction_lt_through_rt", "RT"
    if zone in PED_LR:
        return "pedtrax", "Ped_L_to_R"
    if zone in PED_RL:
        return "pedtrax", "Ped_R_to_L"
    if zone in VECTOR_AS:
        return "vector_tripline", "AS_near_trip"
    if 701 <= zone <= 740:
        return "auto_lane", auto_movement_code(zone) or "unknown"
    return "other", "unknown"


def parse_bindata_line(line: str) -> dict | None:
    """
    Parse one bin-data row. Returns None for empty lines.
    Log lines (timestamp first, no zone) are returned with record_type 'log'.
    """
    raw = line.rstrip("\n\r")
    s = raw.strip()
    if not s:
        return None

    # §10.6 log: yyyy-mm-dd hh:mm:ss text
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s", s) and not re.match(
        r"^\d+,\s*\d{4}-\d{2}-\d{2}", s
    ):
        return {"record_type": "log", "raw_line": raw}

    parts = [p.strip() for p in s.split(",")]
    if len(parts) < 11:
        return {
            "record_type": "malformed",
            "raw_line": raw,
            "field_count": len(parts),
        }

    try:
        zone = int(parts[0])
    except ValueError:
        return {"record_type": "malformed", "raw_line": raw}

    ts = parts[1]
    fields = parts[2:9]
    video_status = parts[9]
    label = parts[10]

    cat, movement = classify_zone(zone)
    sensor = (
        sensor_from_direction_zone(zone)
        or sensor_from_ped_zone(zone)
        or sensor_from_vector_zone(zone)
        or sensor_from_auto_zone(zone)
    )

    row: dict = {
        "record_type": "bin",
        "zone_number": zone,
        "record_category": cat,
        "movement": movement,
        "sensor": sensor,
        "timestamp": ts,
        "video_status": video_status,
        "device_label": label,
        "raw_line": raw,
    }

    # Typed metrics by category (user guide §10.2–10.5)
    def fnum(i: int) -> float | None:
        if i >= len(fields):
            return None
        t = fields[i].strip()
        if t == "":
            return None
        try:
            return float(t) if "." in t else int(t)
        except ValueError:
            return None

    row["volume"] = fnum(0)

    if cat == "direction_lt_through_rt":
        row["occupancy_pct"] = fnum(2)
        row["unused_1"] = fnum(1)
        row["unused_3"] = fnum(3)
        row["unused_4"] = fnum(4)
        row["unused_5"] = fnum(5)
        row["unused_6"] = fnum(6)
    elif cat == "pedtrax":
        row["unused_1"] = fnum(1)
        row["unused_2"] = fnum(2)
        row["avg_ped_speed"] = fnum(3)
        row["max_ped_speed"] = fnum(4)
        row["min_ped_speed"] = fnum(5)
        row["green_or_walk_sec"] = fnum(6)
    elif cat == "vector_tripline":
        row["avg_speed"] = fnum(1)
        row["occupancy_pct"] = fnum(2)
        row["volume_small_class"] = fnum(3)
        row["volume_medium_class"] = fnum(4)
        row["volume_large_class"] = fnum(5)
        row["green_time_sec"] = fnum(6)
    elif cat == "auto_lane":
        row["unused_1"] = fnum(1)
        row["unused_2"] = fnum(2)
        row["unused_3"] = fnum(3)
        row["unused_4"] = fnum(4)
        row["unused_5"] = fnum(5)
        row["green_time_sec"] = fnum(6)
    else:
        for i in range(7):
            row[f"field_{i}"] = fnum(i)

    return row


def parse_iteris_count_date(iso_s: str) -> datetime.datetime | None:
    """Parse the Vantage count stream \"date\" field (ISO with optional ±HHMM suffix)."""
    if not iso_s or not isinstance(iso_s, str):
        return None
    s = iso_s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if re.search(r"[+-]\d{4}$", s) and not re.search(r"[+-]\d{2}:\d{2}$", s):
        s = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", s)
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return None


def floor_datetime_to_bin(dt: datetime.datetime, bin_minutes: int) -> datetime.datetime:
    """Floor wall time to bin start (e.g. 15 min). Preserves tzinfo if present."""
    if bin_minutes <= 1:
        return dt.replace(second=0, microsecond=0)
    total_min = dt.hour * 60 + dt.minute
    floored = (total_min // bin_minutes) * bin_minutes
    h, m = divmod(floored, 60)
    return dt.replace(hour=h, minute=m, second=0, microsecond=0)


def synthetic_direction_rows_from_mqtt_payload(
    payload: dict,
    *,
    target_date: str | None = None,
    bin_minutes: int = 15,
) -> list[dict]:
    """
    One /iteris/count/ JSON object (Rev B) → synthetic rows for build_tmc_summary.

    Count values are \"in the last 60 seconds\" per the API doc; multiple messages
    in the same 15-minute wall bin are summed when passed through build_tmc_summary.
    """
    rows: list[dict] = []
    dt = parse_iteris_count_date(payload.get("date") or "")
    if dt is None:
        return rows
    d_str = dt.strftime("%Y-%m-%d")
    if target_date is not None and d_str != target_date:
        return rows
    dt_bin = floor_datetime_to_bin(dt, bin_minutes)
    ts_str = dt_bin.strftime("%Y-%m-%d %H:%M:%S")

    for zone in range(501, 513):
        key = f"Z{zone}"
        if key not in payload:
            continue
        raw_v = payload[key]
        try:
            vol = int(raw_v)
        except (TypeError, ValueError):
            continue
        if vol == 0:
            continue
        cat, movement = classify_zone(zone)
        if cat != "direction_lt_through_rt":
            continue
        sensor = sensor_from_direction_zone(zone)
        if sensor is None:
            continue
        rows.append(
            {
                "record_type": "bin",
                "zone_number": zone,
                "record_category": cat,
                "movement": movement,
                "sensor": sensor,
                "timestamp": ts_str,
                "volume": vol,
                "source_file": "mqtt:/iteris/count/",
            }
        )
    return rows


def parse_bindata_file(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            rec = parse_bindata_line(line)
            if rec is not None:
                rec["source_file"] = path.name
                rows.append(rec)
    return rows


def timestamp_to_hhmm(ts: str) -> int | None:
    """Bin start time as integer HHMM (e.g. 0, 15, 1545) like standard TMC CSV."""
    ts = (ts or "").strip()
    m = re.match(r"^\d{4}-\d{2}-\d{2}\s+(\d{2}):(\d{2}):\d{2}$", ts)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    return h * 100 + mi


def build_tmc_summary(
    rows: list[dict],
    sensor_to_cardinal: dict[int, str],
) -> list[dict]:
    """
    Aggregate direction-bin volumes (501–512) into one row per time bin.
    sensor_to_cardinal: e.g. {1: \"EB\", 2: \"WB\", 3: \"NB\", 4: \"SB\"}.
    """
    acc: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for r in rows:
        if r.get("record_type") != "bin":
            continue
        if r.get("record_category") != "direction_lt_through_rt":
            continue
        sensor = r.get("sensor")
        if sensor not in sensor_to_cardinal:
            continue
        card = sensor_to_cardinal[sensor]
        if card not in _CARDINALS:
            continue
        suf = _MOVEMENT_TO_COL_SUFFIX.get(r.get("movement") or "")
        if not suf:
            continue
        vol = r.get("volume")
        if vol is None:
            continue
        try:
            v = int(round(float(vol)))
        except (TypeError, ValueError):
            continue
        tkey = timestamp_to_hhmm(r.get("timestamp") or "")
        if tkey is None:
            continue
        acc[tkey][f"{card}{suf}"] += v

    out: list[dict] = []
    for tkey in sorted(acc.keys()):
        row: dict = {"Time": tkey}
        veh = 0
        for c in _CARDINALS:
            for m in _LTR:
                k = f"{c}{m}"
                val = acc[tkey].get(k, 0)
                row[k] = val
                veh += val
        row["Veh Total"] = veh
        out.append(row)

    if out:
        total: dict = {"Time": "Total"}
        for c in _CARDINALS:
            for m in _LTR:
                k = f"{c}{m}"
                total[k] = sum(int(r.get(k, 0) or 0) for r in out)
        total["Veh Total"] = sum(int(r.get("Veh Total", 0) or 0) for r in out)
        out.append(total)

    return out


def write_tmc_summary_csv(path: Path, summary_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(SUMMARY_FIELDNAMES), extrasaction="ignore")
        w.writeheader()
        for r in summary_rows:
            w.writerow(
                {k: ("" if r.get(k) is None else r.get(k, "")) for k in SUMMARY_FIELDNAMES}
            )


def _csv_fieldnames(rows: list[dict]) -> list[str]:
    all_keys: set[str] = set()
    for r in rows:
        all_keys.update(r.keys())
    ordered = [k for k in _CSV_KEY_ORDER if k in all_keys]
    for k in sorted(all_keys):
        if k not in ordered:
            ordered.append(k)
    return ordered


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k, "")) for k in fieldnames})


def write_outputs(
    rows: list[dict],
    out_base: Path,
    sensor_to_cardinal: dict[int, str],
) -> tuple[Path, Path | None, Path | None, list[dict]]:
    """
    Writes:
      {out_base}.csv — TMC summary (Time, EBL..SBR, Veh Total, Total row)
      {out_base}_detail.csv — long tidy rows (all zone types)
      {out_base}_skipped_or_log.csv — optional diagnostics
    Returns (summary_path, detail_path, diag_path, summary_rows).
    """
    summary_rows = build_tmc_summary(rows, sensor_to_cardinal)
    summary_path = out_base.with_suffix(".csv")
    write_tmc_summary_csv(summary_path, summary_rows)

    detail_path = out_base.with_name(out_base.stem + "_detail.csv")
    fieldnames = _csv_fieldnames(rows)
    _write_csv(detail_path, rows, fieldnames)

    extra = [r for r in rows if r.get("record_type") in ("malformed", "log")]
    diag_path = None
    if extra:
        diag_path = out_base.with_name(out_base.stem + "_skipped_or_log.csv")
        _write_csv(diag_path, extra, _csv_fieldnames(extra))

    return summary_path, detail_path, diag_path, summary_rows


class App:
    def __init__(self, root) -> None:
        import tkinter as tk
        import tkinter.font as tkFont
        from tkinter import filedialog, messagebox

        self._tk = tk
        self._messagebox = messagebox
        self._filedialog = filedialog

        # Sensor index (1–4) → cardinal for TMC columns (must match your intersection).
        self.sensor_to_cardinal: dict[int, str] = {
            1: "EB",
            2: "WB",
            3: "NB",
            4: "SB",
        }

        root.title("Iteris Vantage bin-data → CSV")
        w, h = 520, 268
        sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
        root.geometry(f"{w}x{h}+{int((sw - w) / 2)}+{int((sh - h) / 2)}")
        root.resizable(False, False)
        ft = tkFont.Font(family="Times", size=11)

        self.input_path = tk.StringVar()
        self.output_path = tk.StringVar()
        self.status = tk.StringVar(value="Choose a bindata .txt file.")

        tk.Label(root, font=ft, text="Input (.txt):").place(x=12, y=12, height=24)
        tk.Entry(root, font=ft, textvariable=self.input_path, width=52).place(
            x=12, y=38, width=400, height=26
        )
        tk.Button(root, font=ft, text="Browse…", command=self.browse_in).place(
            x=420, y=36, width=80, height=28
        )

        tk.Label(root, font=ft, text="Output CSV base (main + _detail.csv):").place(
            x=12, y=68, height=24
        )
        tk.Entry(root, font=ft, textvariable=self.output_path, width=52).place(
            x=12, y=94, width=400, height=26
        )
        tk.Button(root, font=ft, text="Browse…", command=self.browse_out).place(
            x=420, y=92, width=80, height=28
        )

        tk.Button(root, font=ft, text="Directions…", command=self.show_direction_dialog).place(
            x=12, y=130, width=88, height=28
        )
        tk.Label(
            root,
            font=ft,
            justify="left",
            text="Map C1–C4 sensors to EB/WB/NB/SB for the summary sheet.",
        ).place(x=108, y=128, width=392, height=32)

        tk.Button(root, font=ft, text="Convert", command=self.convert).place(
            x=12, y=168, width=100, height=32
        )
        tk.Label(root, font=ft, textvariable=self.status, anchor="w", relief="sunken").place(
            x=12, y=212, width=488, height=28
        )

    def show_direction_dialog(self) -> None:
        import tkinter.font as tkFont

        tk = self._tk
        d = tk.Toplevel()
        d.title("Sensor → approach (TMC columns)")
        w, h = 320, 220
        sw, sh = d.winfo_screenwidth(), d.winfo_screenheight()
        d.geometry(f"{w}x{h}+{int((sw - w) / 2)}+{int((sh - h) / 2)}")
        ft = tkFont.Font(family="Times", size=11)
        choices = ["EB", "WB", "NB", "SB"]
        entries: dict[int, tk.Entry] = {}
        y = 16
        for i in range(1, 5):
            tk.Label(d, font=ft, text=f"Sensor {i} (C{i}):").place(x=20, y=y, width=100, height=24)
            e = tk.Entry(d, font=ft, width=6)
            e.place(x=130, y=y, width=56, height=24)
            e.insert(0, self.sensor_to_cardinal.get(i, "EB"))
            entries[i] = e
            y += 36

        def save() -> None:
            m: dict[int, str] = {}
            for i in range(1, 5):
                v = entries[i].get().strip().upper()
                if v not in choices:
                    self._messagebox.showerror(
                        "Invalid",
                        f"Sensor {i} must be one of: {', '.join(choices)}",
                    )
                    return
                m[i] = v
            self.sensor_to_cardinal = m
            d.destroy()

        tk.Button(d, font=ft, text="Save", command=save).place(x=80, y=180, width=60, height=28)
        tk.Button(d, font=ft, text="Cancel", command=d.destroy).place(x=160, y=180, width=60, height=28)

    def browse_in(self) -> None:
        p = self._filedialog.askopenfilename(
            title="Bin data text file",
            filetypes=[("Text", "*.txt"), ("All", "*.*")],
        )
        if p:
            self.input_path.set(p)
            if not self.output_path.get().strip():
                self.output_path.set(str(Path(p).with_suffix("")) + "_clean")

    def browse_out(self) -> None:
        p = self._filedialog.asksaveasfilename(
            title="Output CSV base name",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
        )
        if p:
            self.output_path.set(str(Path(p).with_suffix("")))

    def convert(self) -> None:
        inp = self.input_path.get().strip()
        if not inp:
            self._messagebox.showwarning("Missing file", "Select an input .txt file.")
            return
        src = Path(inp)
        if not src.is_file():
            self._messagebox.showerror("Not found", f"File does not exist:\n{src}")
            return

        out = self.output_path.get().strip()
        if out:
            out_base = Path(out)
        else:
            out_base = src.with_name(src.stem + "_clean")

        try:
            rows = parse_bindata_file(src)
        except OSError as e:
            self._messagebox.showerror("Read error", str(e))
            return

        if not rows:
            self.status.set("No rows parsed.")
            self._messagebox.showinfo("Done", "No rows found in file.")
            return

        summary_p, detail_p, diag, summ = write_outputs(
            rows, out_base, self.sensor_to_cardinal
        )
        n_bin = sum(1 for r in rows if r.get("record_type") == "bin")
        n_intervals = max(0, len(summ) - 1) if summ else 0
        msg = (
            f"Summary: {summary_p.name} ({n_intervals} time bins + Total). "
            f"Detail: {detail_p.name} ({n_bin} bin rows)."
        )
        if not summ:
            msg = (
                f"No direction bins (501–512) for summary—wrote header only. "
                f"Detail: {detail_p.name}. Check sensor→EB/WB/NB/SB mapping."
            )
        if diag:
            msg += f" Diagnostics: {diag.name}."
        self.status.set(msg)
        self._messagebox.showinfo("Done", msg)


def main_cli(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "Usage: python import_iteris_bindata.py <bindata.txt> [output_base]\n"
            "  Writes <output_base>.csv (TMC summary: Time, EBL..SBR, Veh Total)\n"
            "  and <output_base>_detail.csv (long format). Default output_base: <stem>_clean\n"
            "  Sensor→EB/WB/NB/SB mapping defaults to 1=EB, 2=WB, 3=NB, 4=SB (GUI to edit).",
            file=sys.stderr,
        )
        return 2
    src = Path(argv[1])
    out = Path(argv[2]) if len(argv) > 2 else src.with_name(src.stem + "_clean")
    if not src.is_file():
        print(f"Not found: {src}", file=sys.stderr)
        return 1
    rows = parse_bindata_file(src)
    default_cardinals = {1: "EB", 2: "WB", 3: "NB", 4: "SB"}
    summary_p, detail_p, diag, summ = write_outputs(rows, out, default_cardinals)
    print(f"Wrote {summary_p} (TMC summary, {len(summ)} rows incl. Total if any)")
    print(f"Wrote {detail_p} (all zones, long format)")
    if diag:
        print(f"Wrote {diag}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        raise SystemExit(main_cli(sys.argv))
    import tkinter as tk

    root = tk.Tk()
    App(root)
    root.mainloop()
