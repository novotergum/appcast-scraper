import calendar
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright
import requests  # für den Make-Webhook

BASE_URL = "https://appcast-de.appcast.io"
LOGIN_URL = f"{BASE_URL}/cc/user-sessions/login"
DEFAULT_EMPLOYER_ID = "27620"

# Zustände, die du auch in den URLs hattest
STATUSES = ["sponsored", "unsponsored", "expired", "aggregated", "suspended"]

# Frühestes Datum, ab dem Tagesdaten verfügbar sind
EARLIEST_DAILY_DATE = datetime(2025, 11, 17).date()

# Optional: zusätzlich zu "flat_rows" im by_day Payload (empfohlen für Make Iterator)
INCLUDE_FLAT_ROWS = True


def localize_decimals_for_de(obj):
    """
    Konvertiert alle int/float-Werte in Strings mit deutschem Dezimaltrennzeichen.
    Beispiel: 5.83 -> "5,83"
    """
    if isinstance(obj, dict):
        return {k: localize_decimals_for_de(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [localize_decimals_for_de(v) for v in obj]
    if isinstance(obj, (int, float)):
        s = f"{obj:.2f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s.replace(".", ",")
    return obj


def current_month_yyyy_mm() -> str:
    """Gibt den aktuellen Monat im Format YYYY-MM zurück (basierend auf UTC)."""
    today = datetime.utcnow()
    return f"{today.year}-{today.month:02d}"


def month_start_end(selected_month: str) -> tuple[str, str]:
    """Ermittelt ersten und letzten Tag des Monats im Format YYYY-MM-DD."""
    year, month = map(int, selected_month.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end = f"{year}-{month:02d}-{last_day:02d}"
    return start, end


def last_calendar_week_range() -> tuple[str, str]:
    """
    Liefert die letzte vollständige Kalenderwoche (Montag–Sonntag)
    relativ zu heute (UTC) als (start_date, end_date) im Format YYYY-MM-DD.
    """
    today = datetime.utcnow().date()
    this_monday = today - timedelta(days=today.weekday())  # 0 = Montag
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)
    return last_monday.strftime("%Y-%m-%d"), last_sunday.strftime("%Y-%m-%d")


def get_config():
    email = os.getenv("APPCAST_EMAIL")
    password = os.getenv("APPCAST_PASSWORD")

    if not email or not password:
        raise RuntimeError(
            "APPCAST_EMAIL und/oder APPCAST_PASSWORD sind nicht gesetzt. "
            "Bitte beide als GitHub Secrets hinterlegen."
        )

    employer_id = os.getenv("APPCAST_EMPLOYER_ID", DEFAULT_EMPLOYER_ID)
    selected_month = current_month_yyyy_mm()

    job_board_ids_raw = os.getenv("APPCAST_JOB_BOARD_IDS", "")
    job_board_ids = [jb.strip() for jb in job_board_ids_raw.split(",") if jb.strip()]

    tiles_job_board_id = os.getenv("APPCAST_TILES_JOB_BOARD_ID", "")

    return {
        "email": email,
        "password": password,
        "employer_id": employer_id,
        "selected_month": selected_month,
        "job_board_ids": job_board_ids,
        "tiles_job_board_id": tiles_job_board_id,
    }


def build_common_report_params() -> dict:
    """Gemeinsame Parameter für by_month / by_day / by_week / by_dynamic_field."""
    return {
        "devise": "all",
        "job_group_stats_source": "data",
        "traffic": "all_wo_organic",
        "sort": "date-desc",
        "publisher_type": "all",
        "account_manager_id": "all",
        "job_group_status": "data",
        "tier": "",
        "selected_certified_filter": "all_sponsored",
        "boomerang": "all",
        "sales_manager_id": "all",
        "salesforce_name": "all",
        "status[]": STATUSES,
    }


def login_with_playwright(pw, cfg):
    """
    Zweistufiger Login:
    1) E-Mail eingeben, Log In klicken
    2) Passwortfeld abwarten, Passwort eingeben, erneut Log In klicken
    """
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    print(f"Öffne Login-Seite: {LOGIN_URL}")
    page.goto(LOGIN_URL, wait_until="networkidle")

    print("Fülle E-Mail-Feld …")
    page.fill("#user_session_email", cfg["email"])

    print("Klicke ersten 'Log In' …")
    page.click("button.btn-login")

    print("Warte auf Passwortfeld …")
    page.wait_for_selector("#user_session_password", timeout=30_000)

    print("Fülle Passwort-Feld …")
    page.fill("#user_session_password", cfg["password"])

    print("Klicke zweiten 'Log In' …")
    page.click("button.btn-login")

    def is_logged_in(response):
        try:
            return "/api/info/user" in response.url and response.status == 200
        except Exception:
            return False

    print("Warte auf erfolgreiche /api/info/user-Response …")
    context.wait_for_event("response", predicate=is_logged_in, timeout=30_000)
    print("Login erfolgreich.")

    return browser, context


def fetch_and_save(api_context, url_path: str, params: dict, out_file: Path, postprocess=None):
    """
    Hilfsfunktion: Request bauen, GET ausführen, JSON (optional transformiert) speichern.
    Gibt die (ggf. postprozessierten) Daten zurück.
    """
    query = urlencode(params, doseq=True)
    full_url = f"{url_path}?{query}" if query else url_path

    print(f"GET {full_url}")
    resp = api_context.get(full_url)
    if not resp.ok:
        text = resp.text()
        raise RuntimeError(f"Request fehlgeschlagen: {resp.status} {resp.status_text()}\n{text}")

    data = resp.json()

    if postprocess is not None:
        data = postprocess(data)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Gespeichert unter: {out_file.resolve()}")

    return data


def filter_tiles_by_day_from_earliest(data):
    """Filtert tiles_by_day-Daten so, dass nur Einträge mit date >= EARLIEST_DAILY_DATE übrig bleiben."""
    def parse_date(value: str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    if isinstance(data, list):
        filtered = []
        for item in data:
            if isinstance(item, dict) and "date" in item:
                d = parse_date(item.get("date", ""))
                if d and d >= EARLIEST_DAILY_DATE:
                    filtered.append(item)
            else:
                filtered.append(item)
        print(f"tiles_by_day: Filter auf >= {EARLIEST_DAILY_DATE}, {len(data)} → {len(filtered)} Einträge")
        return filtered

    if isinstance(data, dict):
        modified = False
        for key, val in list(data.items()):
            if isinstance(val, list) and val:
                sample = next((v for v in val if isinstance(v, dict)), None)
                if sample and "date" in sample:
                    original_len = len(val)
                    new_list = []
                    for item in val:
                        if isinstance(item, dict) and "date" in item:
                            d = parse_date(item.get("date", ""))
                            if d and d >= EARLIEST_DAILY_DATE:
                                new_list.append(item)
                        else:
                            new_list.append(item)
                    data[key] = new_list
                    modified = True
                    print(
                        f"tiles_by_day[{key}]: Filter auf >= {EARLIEST_DAILY_DATE}, "
                        f"{original_len} → {len(new_list)} Einträge"
                    )
        if modified:
            return data

    return data


def get_appcast_hook_url() -> str | None:
    """Ermittelt die Webhook-URL aus der Umgebung: 'appcast_hook' oder 'APPCAST_HOOK'."""
    env_url = os.getenv("appcast_hook") or os.getenv("APPCAST_HOOK")
    if env_url:
        return env_url.strip()
    return None


def send_report_to_webhook(
    employer_id: str,
    selected_month: str,
    start_date: str,
    end_date: str,
    report_type: str,
    report: dict,
    **extra_meta,
):
    """
    Webhook-Sender für range-basierte Reporttypen (by_week, by_dynamic_field, ...).
    """
    hook_url = get_appcast_hook_url()
    if not hook_url:
        print("Kein appcast_hook / APPCAST_HOOK gesetzt – Webhook wird übersprungen.")
        return

    localized_report = localize_decimals_for_de(report)

    payload = {
        "employer_id": employer_id,
        "selected_month": selected_month,
        "start_date": start_date,
        "end_date": end_date,
        "report_type": report_type,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "report": localized_report,
    }
    payload.update(extra_meta)

    print(f"Sende Report '{report_type}' an Webhook {hook_url} …")
    try:
        resp = requests.post(hook_url, json=payload, timeout=20)
        resp.raise_for_status()
        print(f"Webhook erfolgreich: HTTP {resp.status_code}")
    except Exception as e:
        print(f"Fehler beim Senden an Webhook: {e}")


# ---- by_day: strikt "date" als Metric-Key ----

def extract_by_day_rows(by_day_data) -> list[dict]:
    """
    Extrahiert aus by_day die rows (strict):
    { "rows": [ { "date": "YYYY-MM-DD", "job_boards": [...] }, ... ] }
    """
    if isinstance(by_day_data, dict) and isinstance(by_day_data.get("rows"), list):
        return [r for r in by_day_data["rows"] if isinstance(r, dict) and "date" in r]

    if isinstance(by_day_data, list):
        return [x for x in by_day_data if isinstance(x, dict) and "date" in x]

    return []


def filter_by_day_rows_to_range(rows: list[dict], start_date: str, end_date: str) -> list[dict]:
    """Sicherheitsfilter: nur rows innerhalb [start_date, end_date] behalten."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        return rows

    def _parse(d):
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    out = []
    for r in rows:
        d = _parse(r.get("date", ""))
        if d and start_dt <= d <= end_dt:
            out.append(r)
    return out


def flatten_by_day_rows(rows: list[dict]) -> list[dict]:
    """
    Flatten für Make/Sheets: eine Zeile pro (date × job_board).
    """
    flat = []

    def pick_metric(obj, prefix: str):
        if not isinstance(obj, dict):
            return {}
        out = {}
        if "value" in obj:
            out[f"{prefix}_value"] = obj.get("value")
        info = obj.get("info")
        if isinstance(info, dict):
            if "paid" in info:
                out[f"{prefix}_paid"] = info.get("paid")
            if "unpaid" in info:
                out[f"{prefix}_unpaid"] = info.get("unpaid")
            if "total" in info:
                out[f"{prefix}_total"] = info.get("total")
        return out

    metric_keys = [
        "actual_spend",
        "clicks",
        "applies",
        "cpc",
        "cpa",
        "cta",
        "apply_starts",
        "cpas",
        "apply_clickouts",
        "cpac",
        "qualified",
        "hired",
    ]

    for r in rows:
        date = r.get("date")
        job_boards = r.get("job_boards") or []
        if not isinstance(job_boards, list) or not job_boards:
            flat.append({"date": date})
            continue

        for jb in job_boards:
            if not isinstance(jb, dict):
                continue
            row = {
                "date": date,
                "job_board_id": jb.get("id"),
                "job_board_name": jb.get("name"),
                "currency": jb.get("currency"),
                "timezone": jb.get("timezone"),
                "px": jb.get("px"),
            }
            for k in metric_keys:
                if k in jb:
                    row.update(pick_metric(jb.get(k), k))
            flat.append(row)

    return flat


def send_by_day_aggregate_to_webhook(
    employer_id: str,
    selected_month: str,
    by_day_data,
    daily_start: str,
    daily_end: str,
):
    """
    by_day wird im gleichen Turnus wie der Run gesendet (z.B. wöchentlich),
    aber tagesgranular in EINEM Payload.

    Vorgabe umgesetzt:
    - Keine start_date/end_date Felder im Payload.
    - metric_key ist "date".
    - Iterator in Make kann auf payload["rows"] oder payload["flat_rows"] laufen.
    """
    hook_url = get_appcast_hook_url()
    if not hook_url:
        print("Kein appcast_hook / APPCAST_HOOK gesetzt – Webhook wird übersprungen.")
        return

    rows = extract_by_day_rows(by_day_data)
    if not rows:
        print("by_day: Keine rows gefunden – Webhook wird nicht gesendet.")
        return

    rows = filter_by_day_rows_to_range(rows, daily_start, daily_end)
    rows = sorted(rows, key=lambda r: r.get("date", ""))

    payload = {
        "employer_id": employer_id,
        "selected_month": selected_month,
        "report_type": "by_day",
        "metric_key": "date",
        "granularity": "day",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "rows": localize_decimals_for_de(rows),
    }

    if INCLUDE_FLAT_ROWS:
        payload["flat_rows"] = localize_decimals_for_de(flatten_by_day_rows(rows))

    print(f"Sende by_day (tagesgranular, 1 Payload) an Webhook {hook_url} …")
    try:
        resp = requests.post(hook_url, json=payload, timeout=20)
        resp.raise_for_status()
        print(f"Webhook erfolgreich: HTTP {resp.status_code}")
    except Exception as e:
        print(f"Fehler beim Senden an Webhook: {e}")


def fetch_all_reports(cfg, period_start: str, period_end: str):
    """
    Holt alle Reports für einen beliebigen Datumsbereich period_start/period_end (YYYY-MM-DD).
    Typischer Use Case: letzte Kalenderwoche (Mo–So).
    hero_metrics / tiles_by_day bleiben monatsbasiert.
    """
    selected_month = cfg["selected_month"]
    employer_id = cfg["employer_id"]

    # Jahr anhand des Enddatums bestimmen (für Jahres-Reports)
    year = period_end.split("-")[0]
    year_start = f"{year}-1-1"
    year_end = f"{year}-12-31"

    period_label = f"{period_start}_to_{period_end}"

    with sync_playwright() as pw:
        browser, context = login_with_playwright(pw, cfg)

        state = context.storage_state()
        api_context = pw.request.new_context(
            base_url=BASE_URL,
            storage_state=state,
        )

        out_dir = Path("data")

        # 1) hero_metrics (monatsbasiert, weiterhin aktueller Monat)
        hero_params = {
            "selected_month": selected_month,
            "devise": "all",
            "publisher_type": "all",
            "traffic": "all_wo_organic",
            "channel_type": "programmatic",
            "job_group_stats_source": "data",
        }
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/hero_metrics",
            hero_params,
            out_dir / f"hero_metrics_{selected_month}.json",
        )

        common = build_common_report_params()

        # 2) by_month (Jahresübersicht für das Jahr des Enddatums)
        by_month_params = {**common, "start_month": year_start, "end_month": year_end}
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_month",
            by_month_params,
            out_dir / f"by_month_{year}.json",
        )

        # 3) by_dynamic_field (tagged_category_id)
        by_dyn_params = {
            **common,
            "pjg": "false",
            "start_month": year_start,
            "end_month": year_end,
            "dynamic_field": "tagged_category_id",
            "start_date": period_start,
            "end_date": period_end,
            "per_page": 100,
        }
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_dynamic_field",
            by_dyn_params,
            out_dir / f"by_dynamic_field_tagged_category_{period_label}.json",
        )

        # 3b) by_dynamic_field (title)
        by_dyn_title_params = {
            **common,
            "pjg": "false",
            "selected_month": selected_month,
            "dynamic_field": "title",
            "start_date": period_start,
            "end_date": period_end,
            "per_page": 100,
            "job_group_status": "all",
            "sort": "spent-desc",
        }
        by_dyn_title_data = fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_dynamic_field",
            by_dyn_title_params,
            out_dir / f"by_dynamic_field_title_{period_label}.json",
        )

        # 3c) by_dynamic_field (city)
        by_dyn_city_params = {
            **common,
            "pjg": "false",
            "selected_month": selected_month,
            "dynamic_field": "city",
            "start_date": period_start,
            "end_date": period_end,
            "per_page": 100,
            "job_group_status": "all",
            "sort": "spent-desc",
        }
        by_dyn_city_data = fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_dynamic_field",
            by_dyn_city_params,
            out_dir / f"by_dynamic_field_city_{period_label}.json",
        )

        # 4) by_week
        by_week_params = {**common, "start_date": period_start, "end_date": period_end}
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_week",
            by_week_params,
            out_dir / f"by_week_{period_label}.json",
        )

        # 5) by_day (Zeitraum period_start–period_end, aber frühestens ab EARLIEST_DAILY_DATE)
        period_start_dt = datetime.strptime(period_start, "%Y-%m-%d").date()
        period_end_dt = datetime.strptime(period_end, "%Y-%m-%d").date()

        daily_start_dt = max(period_start_dt, EARLIEST_DAILY_DATE)
        daily_end_dt = period_end_dt

        if daily_start_dt <= daily_end_dt:
            daily_start = daily_start_dt.strftime("%Y-%m-%d")
            daily_end = daily_end_dt.strftime("%Y-%m-%d")
            daily_label = f"{daily_start}_to_{daily_end}"

            by_day_params = {**common, "start_date": daily_start, "end_date": daily_end}
            by_day_path = out_dir / f"by_day_{daily_label}.json"
            by_day_data = fetch_and_save(
                api_context,
                f"/api/reports/employer/{employer_id}/by_day",
                by_day_params,
                by_day_path,
            )

            # Webhook: by_day (tagesgranular, 1 Payload), metric_key="date", ohne start/end im Payload
            send_by_day_aggregate_to_webhook(
                employer_id=employer_id,
                selected_month=selected_month,
                by_day_data=by_day_data,
                daily_start=daily_start,
                daily_end=daily_end,
            )
        else:
            print(
                f"Überspringe by_day: Zeitraum {period_start} bis {period_end} "
                f"liegt vollständig vor dem Startdatum für Tagesdaten ({EARLIEST_DAILY_DATE})."
            )

        # Webhook für by_dynamic_field(title)
        send_report_to_webhook(
            employer_id=employer_id,
            selected_month=selected_month,
            start_date=period_start,
            end_date=period_end,
            report_type="by_dynamic_field",
            report=by_dyn_title_data,
            dynamic_field="title",
        )

        # Webhook für by_dynamic_field(city)
        send_report_to_webhook(
            employer_id=employer_id,
            selected_month=selected_month,
            start_date=period_start,
            end_date=period_end,
            report_type="by_dynamic_field",
            report=by_dyn_city_data,
            dynamic_field="city",
        )

        # 6) by_source_index
        source_params = {
            "start_date": period_start,
            "end_date": period_end,
            "status[]": STATUSES,
            "traffic": "all",
            "job_group_stats_source": "data",
        }
        if cfg["job_board_ids"]:
            source_params["job_boards[]"] = cfg["job_board_ids"]

        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_source_index",
            source_params,
            out_dir / f"by_source_index_{period_label}.json",
        )

        # 7) tiles_by_day (monatsbasiert)
        tiles_params = {"selected_month": selected_month, "job_board_id": cfg["tiles_job_board_id"]}
        fetch_and_save(
            api_context,
            f"/api/dashboards/employer/{employer_id}/tiles_by_day",
            tiles_params,
            out_dir / f"tiles_by_day_{selected_month}.json",
            postprocess=filter_tiles_by_day_from_earliest,
        )

        api_context.dispose()
        browser.close()


def main():
    cfg = get_config()
    period_start, period_end = last_calendar_week_range()
    print(
        f"Starte Appcast-Scraper für Employer {cfg['employer_id']} "
        f"für Zeitraum {period_start} bis {period_end} (letzte Kalenderwoche Mo–So)…"
    )
    fetch_all_reports(cfg, period_start, period_end)


if __name__ == "__main__":
    main()
