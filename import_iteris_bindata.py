"""
Convert Iteris Vantage exported bin-data text files into tidy CSV.

Input format matches the user guide (zone, timestamp, seven numeric fields,
video status, label) — e.g. bindata exports like *bindata-YYYY-MM-DD.txt.

MQTT live streams (/iteris/count/) are JSON and not the same as this file format;
use this tool for exported bindata logs.
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

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


def parse_bindata_file(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            rec = parse_bindata_line(line)
            if rec is not None:
                rec["source_file"] = path.name
                rows.append(rec)
    return rows


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


def write_outputs(rows: list[dict], out_base: Path) -> tuple[Path, Path | None]:
    """Write tidy CSV; if malformed/log rows exist, write a second diagnostics CSV."""
    tidy_path = out_base.with_suffix(".csv")
    fieldnames = _csv_fieldnames(rows)
    _write_csv(tidy_path, rows, fieldnames)

    extra = [r for r in rows if r.get("record_type") in ("malformed", "log")]
    diag_path = None
    if extra:
        diag_path = out_base.with_name(out_base.stem + "_skipped_or_log.csv")
        _write_csv(diag_path, extra, _csv_fieldnames(extra))
    return tidy_path, diag_path


class App:
    def __init__(self, root) -> None:
        import tkinter as tk
        import tkinter.font as tkFont
        from tkinter import filedialog, messagebox

        self._tk = tk
        self._messagebox = messagebox
        self._filedialog = filedialog

        root.title("Iteris Vantage bin-data → CSV")
        w, h = 520, 220
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

        tk.Label(root, font=ft, text="Output CSV (optional; default = same folder + .csv):").place(
            x=12, y=72, height=24
        )
        tk.Entry(root, font=ft, textvariable=self.output_path, width=52).place(
            x=12, y=98, width=400, height=26
        )
        tk.Button(root, font=ft, text="Browse…", command=self.browse_out).place(
            x=420, y=96, width=80, height=28
        )

        tk.Button(root, font=ft, text="Convert", command=self.convert).place(
            x=12, y=140, width=100, height=32
        )
        tk.Label(root, font=ft, textvariable=self.status, anchor="w", relief="sunken").place(
            x=12, y=182, width=488, height=28
        )

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

        tidy, diag = write_outputs(rows, out_base)
        n_bin = sum(1 for r in rows if r.get("record_type") == "bin")
        n_other = len(rows) - n_bin
        msg = f"Wrote {tidy.name} ({len(rows)} rows, {n_bin} bin rows)."
        if diag:
            msg += f" See also {diag.name} ({n_other} log/malformed)."
        self.status.set(msg)
        self._messagebox.showinfo("Done", msg)


def main_cli(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "Usage: python import_iteris_bindata.py <bindata.txt> [output_base]\n"
            "  output_base: path without extension for CSV (default: <input_stem>_clean)",
            file=sys.stderr,
        )
        return 2
    src = Path(argv[1])
    out = Path(argv[2]) if len(argv) > 2 else src.with_name(src.stem + "_clean")
    if not src.is_file():
        print(f"Not found: {src}", file=sys.stderr)
        return 1
    rows = parse_bindata_file(src)
    tidy, diag = write_outputs(rows, out)
    print(f"Wrote {tidy}")
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
