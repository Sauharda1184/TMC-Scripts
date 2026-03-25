#!/usr/bin/env python3
"""
Live Iteris Vantage count data over MQTT (Detection API Rev B, topic /iteris/count/).

Connects to the broker on the camera IP, subscribes for a chosen duration, sums
60-second count messages into 15-minute bins, and writes the same TMC summary CSV
(Time, EBL..SBR, Veh Total, Total row) as import_iteris_bindata.py.

TLS with ca/client certs matches the Iteris doc example; use --plain for non-TLS
MQTT on port 1883 if your site allows it.

Historical days are not available over this API—you must run the collector while
the intersection is counting (or keep using exported bindata .txt for archives).
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import threading
import time
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError as e:  # pragma: no cover - runtime env
    raise ImportError(
        "import_iteris_mqtt requires the 'paho-mqtt' package. "
        "Install with: pip install paho-mqtt"
    ) from e

from import_iteris_bindata import (
    build_tmc_summary,
    write_tmc_summary_csv,
    synthetic_direction_rows_from_mqtt_payload,
)

COUNT_TOPIC = "/iteris/count/"


def _make_client() -> mqtt.Client:
    proto = getattr(mqtt, "MQTTv311", getattr(mqtt, "MQTTv31", None))
    kwargs: dict = {"client_id": "tmc_iteris_collect"}
    if proto is not None:
        kwargs["protocol"] = proto
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, **kwargs)
    except (AttributeError, TypeError):
        return mqtt.Client(**kwargs)


def run_mqtt_collect(
    *,
    host: str,
    port: int,
    duration_sec: float,
    target_date: str | None,
    bin_minutes: int,
    cafile: Path | None,
    certfile: Path | None,
    keyfile: Path | None,
    plain: bool,
    insecure: bool,
    mqtt_user: str | None,
    mqtt_password: str | None,
) -> tuple[list[dict], int, int]:
    """
    Returns (synthetic_rows, messages_received, json_errors).
    """
    rows: list[dict] = []
    lock = threading.Lock()
    connect_done = threading.Event()
    state = {
        "rows": rows,
        "lock": lock,
        "target_date": target_date,
        "bin_minutes": bin_minutes,
        "n_msg": 0,
        "n_bad": 0,
        "connect_rc": None,
        "connect_done": connect_done,
    }

    def on_connect(client, userdata, flags, rc, *args, **kwargs):
        userdata["connect_rc"] = rc
        if rc == 0:
            client.subscribe(COUNT_TOPIC, qos=0)
        ev: threading.Event = userdata["connect_done"]
        ev.set()

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            with userdata["lock"]:
                userdata["n_bad"] += 1
            return
        new = synthetic_direction_rows_from_mqtt_payload(
            payload,
            target_date=userdata["target_date"],
            bin_minutes=userdata["bin_minutes"],
        )
        with userdata["lock"]:
            userdata["rows"].extend(new)
            userdata["n_msg"] += 1

    client = _make_client()
    client.user_data_set(state)
    client.on_connect = on_connect
    client.on_message = on_message

    if not plain:
        if insecure:
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
        else:
            ca = str(cafile) if cafile else None
            cert = str(certfile) if certfile else None
            key = str(keyfile) if keyfile else None
            client.tls_set(ca_certs=ca, certfile=cert, keyfile=key)

    if mqtt_user is not None:
        client.username_pw_set(mqtt_user, mqtt_password or "")

    try:
        client.connect(host, port, keepalive=60)
    except Exception:
        connect_done.set()
        raise

    client.loop_start()
    try:
        if not connect_done.wait(timeout=15.0):
            raise TimeoutError(
                "MQTT broker did not respond within 15 s (check IP, port, firewall, TLS options)."
            )
        rc = state.get("connect_rc")
        if rc is None:
            raise ConnectionError("MQTT connect finished without a return code.")
        try:
            rc_int = int(rc)
        except (TypeError, ValueError):
            rc_int = -1
        if rc_int != 0:
            raise ConnectionError(
                f"MQTT connect failed (return code {rc!r}). "
                "Check TLS certificates (--cafile, --cert, --key), --plain, or broker logs."
            )
        remain = max(0.0, float(duration_sec))
        time.sleep(remain)
    finally:
        client.loop_stop()
        client.disconnect()

    return rows, state["n_msg"], state["n_bad"]


def main_cli() -> int:
    p = argparse.ArgumentParser(
        description="Subscribe to Iteris /iteris/count/ and write TMC summary CSV."
    )
    p.add_argument("--host", required=True, help="Camera / broker IP (e.g. 192.168.1.20)")
    p.add_argument(
        "--port",
        type=int,
        default=None,
        help="MQTT port (default: 1883 with --plain, else 8883)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=3600.0,
        help="How long to subscribe (seconds). Default 3600 (1 hour).",
    )
    p.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Only include messages on this calendar date (camera/reporting clock).",
    )
    p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output CSV path (TMC summary).",
    )
    p.add_argument("--cafile", type=Path, help="CA bundle for TLS (recommended)")
    p.add_argument("--cert", type=Path, help="Client certificate for TLS")
    p.add_argument("--key", type=Path, help="Client private key for TLS")
    p.add_argument(
        "--plain",
        action="store_true",
        help="Non-TLS MQTT (typically port 1883). Do not use on untrusted networks.",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="TLS but skip certificate verification (LAN debugging only).",
    )
    p.add_argument(
        "--bin-minutes",
        type=int,
        default=15,
        help="Bucket size for TMC rows (default 15 to match bindata exports).",
    )
    p.add_argument("--mqtt-user", default=None, help="Optional MQTT username")
    p.add_argument("--mqtt-password", default=None, help="Optional MQTT password")
    args = p.parse_args()

    port = args.port if args.port is not None else (1883 if args.plain else 8883)

    if not args.plain and not args.insecure and not args.cafile:
        print(
            "TLS is enabled but --cafile was not set. If the connection fails, "
            "provide --cafile (and usually --cert / --key) per the Vantage API doc, "
            "or use --plain on a trusted LAN if your device allows it.",
            file=sys.stderr,
        )

    sensor_map = {1: "EB", 2: "WB", 3: "NB", 4: "SB"}

    try:
        rows, n_msg, n_bad = run_mqtt_collect(
            host=args.host,
            port=port,
            duration_sec=args.duration,
            target_date=args.date,
            bin_minutes=args.bin_minutes,
            cafile=args.cafile,
            certfile=args.cert,
            keyfile=args.key,
            plain=args.plain,
            insecure=args.insecure,
            mqtt_user=args.mqtt_user,
            mqtt_password=args.mqtt_password,
        )
    except OSError as e:
        print(f"Connection error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    summary = build_tmc_summary(rows, sensor_map)
    write_tmc_summary_csv(args.out, summary)
    print(
        f"Wrote {args.out} — MQTT messages: {n_msg}, JSON errors: {n_bad}, "
        f"summary rows (incl. Total): {len(summary)}"
    )
    if not summary:
        print(
            "No direction data (Z501–Z512) was aggregated. Check date filter, "
            "duration, and that the site publishes automatic direction zones.",
            file=sys.stderr,
        )
    return 0


def _launch_gui() -> None:
    import tkinter as tk
    import tkinter.font as tkFont
    from tkinter import filedialog, messagebox

    class Win:
        def __init__(self, root: tk.Tk) -> None:
            self.root = root
            root.title("Iteris MQTT → TMC CSV")
            w, h = 540, 420
            sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
            root.geometry(f"{w}x{h}+{int((sw - w) / 2)}+{int((sh - h) / 2)}")
            root.resizable(False, False)
            ft = tkFont.Font(family="Times", size=10)

            self.sensor_to_cardinal: dict[int, str] = {
                1: "EB",
                2: "WB",
                3: "NB",
                4: "SB",
            }

            self.host = tk.StringVar(value="192.168.")
            self.port = tk.StringVar(value="")
            self.duration = tk.StringVar(value="3600")
            self.target_date = tk.StringVar(value="")
            self.out_path = tk.StringVar()
            self.plain = tk.IntVar(value=0)
            self.insecure = tk.IntVar(value=0)
            self.cafile = tk.StringVar()
            self.cert = tk.StringVar()
            self.key = tk.StringVar()
            self.mqtt_user = tk.StringVar()
            self.mqtt_pass = tk.StringVar()
            self.status = tk.StringVar(value="Enter camera IP and output path, then Collect.")

            y = 8
            tk.Label(root, font=ft, text="Broker / camera IP:").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.host, width=28).place(
                x=160, y=y, width=200, height=24
            )
            tk.Label(root, font=ft, text="Port (blank=auto):").place(x=370, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.port, width=8).place(
                x=480, y=y, width=48, height=24
            )
            y += 32
            tk.Label(root, font=ft, text="Duration (sec):").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.duration, width=10).place(
                x=160, y=y, width=72, height=24
            )
            tk.Label(root, font=ft, text="Date filter YYYY-MM-DD (optional):").place(
                x=250, y=y, height=22
            )
            tk.Entry(root, font=ft, textvariable=self.target_date, width=12).place(
                x=460, y=y, width=72, height=24
            )
            y += 32
            tk.Label(root, font=ft, text="Output CSV:").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.out_path, width=40).place(
                x=160, y=y, width=300, height=24
            )
            tk.Button(root, font=ft, text="Browse…", command=self._browse_out).place(
                x=470, y=y - 2, width=60, height=28
            )
            y += 34
            tk.Checkbutton(
                root,
                font=ft,
                text="Plain MQTT (no TLS, port 1883)",
                variable=self.plain,
            ).place(x=12, y=y)
            tk.Checkbutton(
                root,
                font=ft,
                text="TLS insecure (no CA verify)",
                variable=self.insecure,
            ).place(x=260, y=y)
            y += 30
            tk.Label(root, font=ft, text="CA file:").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.cafile, width=50).place(
                x=80, y=y, width=380, height=24
            )
            tk.Button(root, font=ft, text="…", command=lambda: self._pick(self.cafile)).place(
                x=470, y=y - 2, width=28, height=28
            )
            y += 30
            tk.Label(root, font=ft, text="Client cert:").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.cert, width=50).place(
                x=100, y=y, width=360, height=24
            )
            tk.Button(root, font=ft, text="…", command=lambda: self._pick(self.cert)).place(
                x=470, y=y - 2, width=28, height=28
            )
            y += 30
            tk.Label(root, font=ft, text="Client key:").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.key, width=50).place(
                x=100, y=y, width=360, height=24
            )
            tk.Button(root, font=ft, text="…", command=lambda: self._pick(self.key)).place(
                x=470, y=y - 2, width=28, height=28
            )
            y += 32
            tk.Label(root, font=ft, text="MQTT user (opt):").place(x=12, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.mqtt_user, width=16).place(
                x=120, y=y, width=120, height=24
            )
            tk.Label(root, font=ft, text="Password:").place(x=250, y=y, height=22)
            tk.Entry(root, font=ft, textvariable=self.mqtt_pass, width=16, show="*").place(
                x=330, y=y, width=120, height=24
            )
            y += 36
            tk.Button(root, font=ft, text="Directions…", command=self._dir_dialog).place(
                x=12, y=y, width=88, height=28
            )
            tk.Button(root, font=ft, text="Collect", command=self._collect).place(
                x=120, y=y, width=80, height=28
            )
            y += 40
            tk.Label(root, font=ft, textvariable=self.status, anchor="w", relief="sunken").place(
                x=12, y=y, width=516, height=56
            )

        def _pick(self, var: tk.StringVar) -> None:
            p = filedialog.askopenfilename(title="Select file")
            if p:
                var.set(p)

        def _browse_out(self) -> None:
            p = filedialog.asksaveasfilename(
                title="TMC summary CSV",
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
            )
            if p:
                self.out_path.set(p)

        def _dir_dialog(self) -> None:
            d = tk.Toplevel(self.root)
            d.title("Sensor → approach")
            w, h = 300, 200
            sw, sh = d.winfo_screenwidth(), d.winfo_screenheight()
            d.geometry(f"{w}x{h}+{int((sw - w) / 2)}+{int((sh - h) / 2)}")
            ft = tkFont.Font(family="Times", size=11)
            choices = ["EB", "WB", "NB", "SB"]
            entries: dict[int, tk.Entry] = {}
            y = 16
            for i in range(1, 5):
                tk.Label(d, font=ft, text=f"Sensor {i}:").place(x=20, y=y, width=80, height=24)
                e = tk.Entry(d, font=ft, width=6)
                e.place(x=120, y=y, width=56, height=24)
                e.insert(0, self.sensor_to_cardinal.get(i, "EB"))
                entries[i] = e
                y += 36

            def save() -> None:
                m: dict[int, str] = {}
                for i in range(1, 5):
                    v = entries[i].get().strip().upper()
                    if v not in choices:
                        messagebox.showerror("Invalid", f"Sensor {i}: use EB, WB, NB, or SB")
                        return
                    m[i] = v
                self.sensor_to_cardinal = m
                d.destroy()

            tk.Button(d, font=ft, text="Save", command=save).place(x=80, y=170, width=60, height=28)
            tk.Button(d, font=ft, text="Cancel", command=d.destroy).place(x=160, y=170, width=60, height=28)

        def _collect(self) -> None:
            host = self.host.get().strip()
            if not host or host == "192.168.":
                messagebox.showwarning("Host", "Enter the full camera / broker IP address.")
                return
            outp = self.out_path.get().strip()
            if not outp:
                messagebox.showwarning("Output", "Choose an output CSV path.")
                return
            try:
                dur = float(self.duration.get().strip())
            except ValueError:
                messagebox.showerror("Duration", "Duration must be a number (seconds).")
                return
            port_s = self.port.get().strip()
            plain = self.plain.get() == 1
            insecure = self.insecure.get() == 1
            port = int(port_s) if port_s else (1883 if plain else 8883)

            td = self.target_date.get().strip() or None
            if td and not re.match(r"^\d{4}-\d{2}-\d{2}$", td):
                messagebox.showerror("Date", "Use YYYY-MM-DD or leave blank.")
                return

            ca = Path(self.cafile.get()) if self.cafile.get().strip() else None
            cert = Path(self.cert.get()) if self.cert.get().strip() else None
            key = Path(self.key.get()) if self.key.get().strip() else None
            smap = dict(self.sensor_to_cardinal)

            self.status.set("Collecting in background… UI stays responsive.")

            def work() -> None:
                try:
                    rows, n_msg, n_bad = run_mqtt_collect(
                        host=host,
                        port=port,
                        duration_sec=dur,
                        target_date=td,
                        bin_minutes=15,
                        cafile=ca,
                        certfile=cert,
                        keyfile=key,
                        plain=plain,
                        insecure=insecure,
                        mqtt_user=self.mqtt_user.get().strip() or None,
                        mqtt_password=self.mqtt_pass.get() or None,
                    )
                    summary = build_tmc_summary(rows, smap)
                    write_tmc_summary_csv(Path(outp), summary)
                except Exception as e:
                    self.root.after(0, lambda err=str(e): self._collect_failed(err))
                    return

                msg = (
                    f"Wrote {outp}\nMessages: {n_msg}, JSON errors: {n_bad}, "
                    f"rows (incl. Total): {len(summary)}"
                )
                self.root.after(0, lambda m=msg: self._collect_done(m))

            threading.Thread(target=work, daemon=True).start()

        def _collect_failed(self, err: str) -> None:
            messagebox.showerror("Error", err)
            self.status.set("Failed.")

        def _collect_done(self, msg: str) -> None:
            self.status.set(msg)
            messagebox.showinfo("Done", msg)

    root = tk.Tk()
    Win(root)
    root.mainloop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        raise SystemExit(main_cli())
    _launch_gui()
