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


def localize_decimals_for_de(obj):
    """
    Konvertiert alle int/float-Werte in Strings mit deutschem Dezimaltrennzeichen.
    Beispiel: 5.83 -> "5,83"

    Wichtig:
    - Struktur (Dicts/Listen) bleibt erhalten.
    - Nur Zahlen werden verändert, alle anderen Typen bleiben unverändert.
    """
    if isinstance(obj, dict):
        return {k: localize_decimals_for_de(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [localize_decimals_for_de(v) for v in obj]
    if isinstance(obj, (int, float)):
        # 2 Nachkommastellen, Trailing-Nullen optional abschneiden
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

    Beispiel: Aufruf am Montag, 2025-12-08
    → Ergebnis: 2025-12-01 (Mo) bis 2025-12-07 (So).
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
    # Für hero_metrics / tiles_by_day weiterhin ein Monats-Parameter,
    # alle date-basierten Reports werden über period_start/period_end gesteuert.
    selected_month = current_month_yyyy_mm()

    job_board_ids_raw = os.getenv("APPCAST_JOB_BOARD_IDS", "")
    job_board_ids = [
        jb.strip() for jb in job_board_ids_raw.split(",") if jb.strip()
    ]

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

    # Schritt 1: E-Mail
    print("Fülle E-Mail-Feld …")
    page.fill("#user_session_email", cfg["email"])

    print("Klicke ersten 'Log In' …")
    page.click("button.btn-login")

    # Schritt 2: Passwortfeld abwarten
    print("Warte auf Passwortfeld …")
    page.wait_for_selector("#user_session_password", timeout=30_000)

    print("Fülle Passwort-Feld …")
    page.fill("#user_session_password", cfg["password"])

    print("Klicke zweiten 'Log In' …")
    page.click("button.btn-login")

    # Warten, bis /api/info/user mit 200 kommt → sicher eingeloggt
    def is_logged_in(response):
        try:
            return "/api/info/user" in response.url and response.status == 200
        except Exception:
            return False

    print("Warte auf erfolgreiche /api/info/user-Response …")
    context.wait_for_event("response", predicate=is_logged_in, timeout=30_000)
    print("Login erfolgreich.")

    return browser, context


def fetch_and_save(
    api_context,
    url_path: str,
    params: dict,
    out_file: Path,
    postprocess=None,
):
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
        raise RuntimeError(
            f"Request fehlgeschlagen: {resp.status} {resp.status_text()}\n{text}"
        )

    data = resp.json()

    if postprocess is not None:
        data = postprocess(data)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Gespeichert unter: {out_file.resolve()}")

    return data


def filter_tiles_by_day_from_earliest(data):
    """
    Filtert tiles_by_day-Daten so, dass nur Einträge mit date >= EARLIEST_DAILY_DATE
    übrig bleiben.
    """

    def parse_date(value: str):
        try:
            # Schneide ggf. Zeitanteil ab
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    # Fall 1: Top-Level-Liste
    if isinstance(data, list):
        filtered = []
        for item in data:
            if isinstance(item, dict) and "date" in item:
                d = parse_date(item.get("date", ""))
                if d and d >= EARLIEST_DAILY_DATE:
                    filtered.append(item)
            else:
                filtered.append(item)
        print(
            f"tiles_by_day: Filter auf >= {EARLIEST_DAILY_DATE}, "
            f"{len(data)} → {len(filtered)} Einträge"
        )
        return filtered

    # Fall 2: Top-Level-Dict mit Liste(n)
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
    """
    Ermittelt die Webhook-URL aus der Umgebung:
    - 'appcast_hook' (klein) oder
    - 'APPCAST_HOOK' (groß)
    Kein Fallback im Code.
    """
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
    Generischer Webhook-Sender für verschiedene Reporttypen.

    Beispiele:
    - report_type="by_day"
    - report_type="by_dynamic_field", dynamic_field="title"
    - report_type="by_dynamic_field", dynamic_field="city"

    Alle Zahlen werden vor dem Senden in deutsche Schreibweise konvertiert
    (5.83 → "5,83"), damit sie in Google Sheets / Make als Strings mit Komma ankommen.
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


def fetch_all_reports(cfg, period_start: str, period_end: str):
    """
    Holt alle Reports für einen beliebigen Datumsbereich period_start/period_end
    (YYYY-MM-DD). Typischer Use Case hier: letzte Kalenderwoche (Mo–So).
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

        # Gemeinsame Basis für weitere Reports
        common = build_common_report_params()

        # 2) by_month (Jahresübersicht für das Jahr des Enddatums)
        by_month_params = {
            **common,
            "start_month": year_start,
            "end_month": year_end,
        }
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_month",
            by_month_params,
            out_dir / f"by_month_{year}.json",
        )

        # 3) by_dynamic_field (tagged_category_id, Zeitraum period_start–period_end)
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
            out_dir
            / f"by_dynamic_field_tagged_category_{period_label}.json",
        )

        # 3b) by_dynamic_field (title, Zeitraum period_start–period_end, sortiert nach Spend)
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

        # 3c) by_dynamic_field (city, Zeitraum period_start–period_end, sortiert nach Spend)
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

        # 4) by_week (Zeitraum period_start–period_end, typischerweise eine Woche)
        by_week_params = {
            **common,
            "start_date": period_start,
            "end_date": period_end,
        }
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

            by_day_params = {
                **common,
                "start_date": daily_start,
                "end_date": daily_end,
            }
            by_day_path = out_dir / f"by_day_{daily_label}.json"
            by_day_data = fetch_and_save(
                api_context,
                f"/api/reports/employer/{employer_id}/by_day",
                by_day_params,
                by_day_path,
            )

            # Webhook mit by_day-Report (mit DE-Lokalisierung) triggern
            send_report_to_webhook(
                employer_id=employer_id,
                selected_month=selected_month,
                start_date=daily_start,
                end_date=daily_end,
                report_type="by_day",
                report=by_day_data,
            )
        else:
            print(
                f"Überspringe by_day: Zeitraum {period_start} bis {period_end} "
                f"liegt vollständig vor dem Startdatum für Tagesdaten "
                f"({EARLIEST_DAILY_DATE})."
            )

        # Webhook für by_dynamic_field(title) – mit lokalisierter Dezimalschreibweise
        send_report_to_webhook(
            employer_id=employer_id,
            selected_month=selected_month,
            start_date=period_start,
            end_date=period_end,
            report_type="by_dynamic_field",
            report=by_dyn_title_data,
            dynamic_field="title",
        )

        # Webhook für by_dynamic_field(city) – mit lokalisierter Dezimalschreibweise
        send_report_to_webhook(
            employer_id=employer_id,
            selected_month=selected_month,
            start_date=period_start,
            end_date=period_end,
            report_type="by_dynamic_field",
            report=by_dyn_city_data,
            dynamic_field="city",
        )

        # 6) by_source_index (job_board-spezifisch, Zeitraum period_start–period_end)
        source_params = {
            "start_date": period_start,
            "end_date": period_end,
            "status[]": STATUSES,
            "traffic": "all",
            "job_group_stats_source": "data",
        }
        if cfg["job_board_ids"]:
            # job_boards[]=ac-571&job_boards[]=...
            source_params["job_boards[]"] = cfg["job_board_ids"]

        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_source_index",
            source_params,
            out_dir / f"by_source_index_{period_label}.json",
        )

        # 7) tiles_by_day (Dashboard-Kacheln pro Tag – weiterhin monatsbasiert)
        tiles_params = {
            "selected_month": selected_month,
            "job_board_id": cfg["tiles_job_board_id"],
        }
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
