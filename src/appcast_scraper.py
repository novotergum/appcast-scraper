import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "https://appcast-de.appcast.io"
LOGIN_URL = f"{BASE_URL}/cc/user-sessions/login"
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
            "Bitte beide als GitHub Secrets hinterlegen."
        )

    employer_id = os.getenv("APPCAST_EMPLOYER_ID", DEFAULT_EMPLOYER_ID)
    selected_month = os.getenv("APPCAST_SELECTED_MONTH") or previous_month_yyyy_mm()

    return {
        "email": email,
        "password": password,
        "employer_id": employer_id,
        "selected_month": selected_month,
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


def fetch_hero_metrics(cfg):
    selected_month = cfg["selected_month"]
    employer_id = cfg["employer_id"]

    with sync_playwright() as pw:
        browser, context = login_with_playwright(pw, cfg)

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

        out_dir = Path("data")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"hero_metrics_{selected_month}.json"
        out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

        print(f"hero_metrics gespeichert unter: {out_file.resolve()}")

        api_context.dispose()
        browser.close()


def main():
    cfg = get_config()
    print(
        f"Starte Appcast-Scraper für Employer {cfg['employer_id']} "
        f"und Monat {cfg['selected_month']} …"
    )
    fetch_hero_metrics(cfg)


if __name__ == "__main__":
    main()
