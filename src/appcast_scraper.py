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


def get_config():
    email = os.getenv("APPCAST_EMAIL")
    password = os.getenv("APPCAST_PASSWORD")

    if not email or not password:
        raise RuntimeError(
            "APPCAST_EMAIL und/oder APPCAST_PASSWORD sind nicht gesetzt. "
            "Bitte beide als GitHub Secrets hinterlegen."
        )

    employer_id = os.getenv("APPCAST_EMPLOYER_ID", DEFAULT_EMPLOYER_ID)
    # Immer aktueller Monat, kein Override per Umgebungsvariable
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
    übrig bleiben. Wir fassen die Struktur möglichst generisch an:
    - Wenn Top-Level eine Liste von Dicts mit 'date' ist → Liste filtern.
    - Wenn Top-Level ein Dict mit einer List von Dicts mit 'date' ist → diese Liste filtern.
    Andernfalls wird data unverändert zurückgegeben.
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
                # falls doch andere Strukturen vorkommen, behalten wir sie
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
                # Prüfe, ob es eine Liste von Dicts mit 'date' ist
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

    # Fallback: nichts verändert
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


def send_by_day_to_webhook(
    employer_id: str,
    selected_month: str,
    start_date: str,
    end_date: str,
    report: dict,
):
    """
    Schickt den by_day-Report als JSON an den Make-Webhook.
    Wenn kein Webhook gesetzt ist, wird stillschweigend übersprungen.
    Fehler beim Aufruf brechen den Scraper nicht ab.
    """
    hook_url = get_appcast_hook_url()
    if not hook_url:
        print("Kein appcast_hook / APPCAST_HOOK gesetzt – Webhook wird übersprungen.")
        return

    payload = {
        "employer_id": employer_id,
        "selected_month": selected_month,
        "start_date": start_date,
        "end_date": end_date,
        "report_type": "by_day",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "report": report,
    }

    print(f"Sende by_day-Report an Webhook {hook_url} …")
    try:
        resp = requests.post(hook_url, json=payload, timeout=20)
        resp.raise_for_status()
        print(f"Webhook erfolgreich: HTTP {resp.status_code}")
    except Exception as e:
        print(f"Fehler beim Senden an Webhook: {e}")


def fetch_all_reports(cfg):
    selected_month = cfg["selected_month"]
    employer_id = cfg["employer_id"]
    month_start, month_end = month_start_end(selected_month)
    year = selected_month.split("-")[0]
    year_start = f"{year}-1-1"
    year_end = f"{year}-12-31"

    with sync_playwright() as pw:
        browser, context = login_with_playwright(pw, cfg)

        state = context.storage_state()
        api_context = pw.request.new_context(
            base_url=BASE_URL,
            storage_state=state,
        )

        out_dir = Path("data")

        # 1) hero_metrics (wie bisher)
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

        # 2) by_month (Jahresübersicht)
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

        # 3) by_dynamic_field (tagged_category_id, kompletter Monat)
        by_dyn_params = {
            **common,
            "pjg": "false",
            "start_month": year_start,
            "end_month": year_end,
            "dynamic_field": "tagged_category_id",
            "start_date": month_start,
            "end_date": month_end,
            "per_page": 100,
        }
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_dynamic_field",
            by_dyn_params,
            out_dir / f"by_dynamic_field_tagged_category_{selected_month}.json",
        )

        # 4) by_week (Monatszeitraum)
        by_week_params = {
            **common,
            "start_date": month_start,
            "end_date": month_end,
        }
        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_week",
            by_week_params,
            out_dir / f"by_week_{selected_month}.json",
        )

        # 5) by_day (Monatszeitraum, aber frühestens ab EARLIEST_DAILY_DATE)
        month_start_dt = datetime.strptime(month_start, "%Y-%m-%d").date()
        month_end_dt = datetime.strptime(month_end, "%Y-%m-%d").date()

        daily_start_dt = max(month_start_dt, EARLIEST_DAILY_DATE)
        daily_end_dt = month_end_dt

        if daily_start_dt <= daily_end_dt:
            daily_start = daily_start_dt.strftime("%Y-%m-%d")
            daily_end = daily_end_dt.strftime("%Y-%m-%d")

            by_day_params = {
                **common,
                "start_date": daily_start,
                "end_date": daily_end,
            }
            by_day_path = out_dir / f"by_day_{selected_month}.json"
            by_day_data = fetch_and_save(
                api_context,
                f"/api/reports/employer/{employer_id}/by_day",
                by_day_params,
                by_day_path,
            )

            # Webhook mit by_day-Report triggern
            send_by_day_to_webhook(
                employer_id=employer_id,
                selected_month=selected_month,
                start_date=daily_start,
                end_date=daily_end,
                report=by_day_data,
            )
        else:
            print(
                f"Überspringe by_day: Monat {selected_month} liegt vollständig vor "
                f"dem Startdatum für Tagesdaten ({EARLIEST_DAILY_DATE})."
            )

        # 6) by_source_index (job_board-spezifisch, Monatszeitraum)
        source_params = {
            "start_date": month_start,
            "end_date": month_end,
            "status[]": STATUSES,
            "traffic": "all",
            "job_group_stats_source": "data",
        }
        if cfg["job_board_ids"]:
            # job_boards[]=ac-571&job_boards[]=...
            source_params["job_boords[]"] = cfg["job_board_ids"]

        fetch_and_save(
            api_context,
            f"/api/reports/employer/{employer_id}/by_source_index",
            source_params,
            out_dir / f"by_source_index_{selected_month}.json",
        )

        # 7) tiles_by_day (Dashboard-Kacheln pro Tag, clientseitig ab EARLIEST_DAILY_DATE gefiltert)
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
    print(
        f"Starte Appcast-Scraper für Employer {cfg['employer_id']} "
        f"und Monat {cfg['selected_month']} …"
    )
    fetch_all_reports(cfg)


if __name__ == "__main__":
    main()
