import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright


BASE_URL = "https://appcast-de.appcast.io"
LOGIN_URL = f"{BASE_URL}/cc/user-sessions/login"

# Fallback-Employer-ID, kann per Secret überschrieben werden
DEFAULT_EMPLOYER_ID = "27620"


def previous_month_yyyy_mm() -> str:
    """Gibt den Vormonat im Format YYYY-MM zurück."""
    today = datetime.utcnow()
    first_of_this_month = today.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    return f"{last_day_prev_month.year}-{last_day_prev_month.month:02d}"


def get_config():
    email = os.getenv("APPCAST_EMAIL")
    password = os.getenv("APPCAST_PASSWORD")
    if not email or not password:
        raise RuntimeError(
            "APPCAST_EMAIL und/oder APPCAST_PASSWORD sind nicht gesetzt. "
            "Bitte als GitHub Secrets hinterlegen."
        )

    employer_id = os.getenv("APPCAST_EMPLOYER_ID", DEFAULT_EMPLOYER_ID)
    selected_month = os.getenv("APPCAST_SELECTED_MONTH") or previous_month_yyyy_mm()

    return {
        "email": email,
        "password": password,
        "employer_id": employer_id,
        "selected_month": selected_month,
    }


def login_with_playwright(pw, config):
    """
    Öffnet headless den Login, füllt E-Mail und Passwort und wartet,
    bis /api/info/user erfolgreich zurückkommt.
    Gibt den BrowserContext zurück.
    """
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    page.goto(LOGIN_URL, wait_until="networkidle")

    # E-Mail
    page.fill('input[name="email"]', config["email"])

    # Passwort – hier Annahme: es gibt ein klassisches Passwort-Feld
    try:
        page.fill('input[type="password"]', config["password"])
    except Exception:
        # Wenn es wirklich kein Passwort-Feld gibt, ist das ein Problem
        raise RuntimeError(
            "Kein Passwort-Feld gefunden. Wenn der Login nur über Magic-Link läuft, "
            "ist vollautomatisches Login im CI nicht möglich."
        )

    # Login-Button klicken
    page.click('button[type="submit"]')

    # Warten, bis /api/info/user kommt und 200 liefert
    def is_logged_in(response):
        return "/api/info/user" in response.url and response.status == 200

    page.wait_for_response(is_logged_in, timeout=30_000)
    return browser, context


def fetch_hero_metrics(config):
    """
    Nutzt den eingeloggten Context, um hero_metrics via API zu holen,
    und speichert das JSON in data/hero_metrics_YYYY-MM.json.
    """
    selected_month = config["selected_month"]
    employer_id = config["employer_id"]

    with sync_playwright() as pw:
        browser, context = login_with_playwright(pw, config)

        # Storage-State aus dem Browserkontext holen
        state = context.storage_state()

        # API-Request-Kontext mit denselben Cookies
        api_context = pw.request.new_context(
            base_url=BASE_URL,
            storage_state=state,
        )

        params = {
            "selected_month": selected_month,
            "devise": "all",
            "publisher_type": "all",
            "traffic": "all_wo_organic",
            "channel_type": "programmatic",
            "job_group_stats_source": "data",
        }

        url = f"/api/reports/employer/{employer_id}/hero_metrics"
        print(f"Rufe {url} mit params={params} auf …")

        resp = api_context.get(url, params=params)
        if not resp.ok:
            text = resp.text()
            raise RuntimeError(
                f"hero_metrics fehlgeschlagen: {resp.status} {resp.status_text()}\n{text}"
            )

        data = resp.json()

        # Ausgabeordner
        out_dir = Path("data")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"hero_metrics_{selected_month}.json"

        out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"hero_metrics gespeichert unter: {out_file.resolve()}")

        api_context.dispose()
        browser.close()


def main():
    config = get_config()
    print(
        f"Starte Appcast-Scraper für Employer {config['employer_id']} "
        f"und Monat {config['selected_month']} …"
    )
    fetch_hero_metrics(config)


if __name__ == "__main__":
    main()
