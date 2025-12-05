import fetch from "node-fetch";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import cheerio from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Basis-Konfiguration über ENV
const BASE_URL = process.env.APPCAST_BASE_URL || "https://appcast-de.appcast.io";
const EMPLOYER_ID = process.env.APPCAST_EMPLOYER_ID; // z.B. "27620"
const EMAIL = process.env.APPCAST_EMAIL;
const PASSWORD = process.env.APPCAST_PASSWORD;

// Datumslogik: Standard = letzte 30 Tage
function getDefaultDates() {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - 30);

  const fmt = (d) => d.toISOString().slice(0, 10);
  return { start: fmt(start), end: fmt(end) };
}

const { start: DEFAULT_START, end: DEFAULT_END } = getDefaultDates();

const START_DATE = process.env.APPCAST_START_DATE || DEFAULT_START;
const END_DATE = process.env.APPCAST_END_DATE || DEFAULT_END;

// Hilfsfunktion: einfache Fehlermeldung + Exit
function assertEnv(varName, value) {
  if (!value) {
    console.error(`[ERROR] Umgebungsvariable ${varName} ist nicht gesetzt.`);
    process.exit(1);
  }
}

assertEnv("APPCAST_EMPLOYER_ID", EMPLOYER_ID);
assertEnv("APPCAST_EMAIL", EMAIL);
assertEnv("APPCAST_PASSWORD", PASSWORD);

async function fetchLoginPage() {
  const url = `${BASE_URL}/user_sessions/new`;
  console.log(`[INFO] Lade Login-Seite: ${url}`);
  const res = await fetch(url, {
    method: "GET",
    headers: {
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
  });

  if (!res.ok) {
    throw new Error(`Login-Seite konnte nicht geladen werden (Status ${res.status})`);
  }

  const html = await res.text();
  const setCookieHeaders = res.headers.raw()["set-cookie"] || [];
  return { html, cookies: setCookieHeaders };
}

function extractAuthenticityToken(html) {
  const $ = cheerio.load(html);
  const token = $('input[name="authenticity_token"]').attr("value");
  if (!token) {
    throw new Error("authenticity_token nicht in Login-HTML gefunden");
  }
  return token;
}

function buildCookieHeader(existingSetCookies) {
  // Rohes Zusammenbauen aus Set-Cookie-Headern
  return existingSetCookies
    .map((c) => c.split(";")[0])
    .join("; ");
}

function extractCsrfTokenFromCookies(setCookieHeaders) {
  // Suche nach "csrf-token=..."
  for (const c of setCookieHeaders) {
    const match = c.match(/csrf-token=([^;]+)/);
    if (match) return match[1];
  }
  return null;
}

async function login(authenticityToken, initialCookies) {
  const url = `${BASE_URL}/user_sessions`;
  console.log(`[INFO] Führe Login durch: ${url}`);

  const body = new URLSearchParams();
  body.append("authenticity_token", authenticityToken);
  body.append("new_ui", "true");
  body.append("email", EMAIL);
  body.append("password", PASSWORD);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "content-type": "application/x-www-form-urlencoded",
      "cookie": buildCookieHeader(initialCookies)
    },
    body
  });

  if (!res.ok && res.status !== 302) {
    // Einige Rails-/SPA-Logins antworten mit 302 Redirect bei Erfolg
    throw new Error(`Login fehlgeschlagen (Status ${res.status})`);
  }

  const setCookieHeaders = res.headers.raw()["set-cookie"] || [];
  const allCookies = [...initialCookies, ...setCookieHeaders];

  const cookieHeader = buildCookieHeader(allCookies);
  const csrfToken = extractCsrfTokenFromCookies(allCookies);

  console.log("[INFO] Login erfolgreich, Cookies & CSRF-Token extrahiert.");
  return { cookieHeader, csrfToken };
}

function buildApiUrl() {
  const params = new URLSearchParams({
    "search[account_manager_id]": "all",
    "search[boomerang]": "all",
    "search[publisher_type]": "all",
    "search[devise]": "all",
    "search[job_group_stats_source]": "jobs",
    "search[job_group_status]": "data",
    "search[job_board_id][]": "ac-571",
    "search[page]": "1",
    "search[risk][type]": "all",
    "search[sales_manager_id]": "all",
    "search[salesforce_name]": "all",
    "search[searchable_column][field]": "title",
    "search[aggregate_expansion_jobs]": "true",
    "search[job_status]": "with_stats_plus_active",
    "search[start]": START_DATE,
    "search[end]": END_DATE,
    "search[organic]": "all_wo_organic",
    "sort": "spent-desc",
    "page": "1"
  });

  return `${BASE_URL}/api/employer/${EMPLOYER_ID}/jobs/total?${params.toString()}`;
}

async function fetchJobs(cookieHeader, csrfToken) {
  const url = buildApiUrl();
  console.log(`[INFO] Rufe API auf: ${url}`);
  const headers = {
    "accept": "application/json, text/plain, */*",
    "cookie": cookieHeader
  };
  if (csrfToken) {
    headers["x-csrf-token"] = csrfToken;
  }

  const res = await fetch(url, {
    method: "GET",
    headers
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API-Request fehlgeschlagen (Status ${res.status}): ${text.slice(0, 500)}`);
  }

  const json = await res.json();
  return json;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function saveJson(json) {
  const outDir = path.join(__dirname, "..", "data");
  ensureDir(outDir);
  const filename = `appcast_raw_${START_DATE}_to_${END_DATE}.json`;
  const outPath = path.join(outDir, filename);
  fs.writeFileSync(outPath, JSON.stringify(json, null, 2), "utf-8");
  console.log(`[INFO] Rohdaten gespeichert: ${outPath}`);
  return outPath;
}

function saveCsv(json) {
  // Sehr generischer Versuch: Jobs-Array finden
  const jobs =
    (Array.isArray(json) && json) ||
    json.jobs ||
    json.data ||
    json.job_groups ||
    [];

  if (!Array.isArray(jobs) || jobs.length === 0) {
    console.warn("[WARN] Konnte kein Jobs-Array in der Antwort erkennen. CSV wird übersprungen.");
    return null;
  }

  const outDir = path.join(__dirname, "..", "data");
  ensureDir(outDir);
  const filename = `appcast_jobs_${START_DATE}_to_${END_DATE}.csv`;
  const outPath = path.join(outDir, filename);

  const header = [
    "id",
    "title",
    "clicks",
    "applies",
    "cpc",
    "cpa",
    "spent"
  ];

  const lines = [header.join(",")];

  for (const job of jobs) {
    const row = [
      job.id ?? "",
      job.title ?? job.name ?? "",
      job.clicks ?? job.stats?.clicks ?? "",
      job.applies ?? job.stats?.applies ?? "",
      job.cpc ?? job.stats?.cpc ?? "",
      job.cpa ?? job.stats?.cpa ?? "",
      job.spent ?? job.stats?.spent ?? ""
    ];
    // CSV escaping minimal
    lines.push(
      row
        .map((v) => {
          const s = String(v ?? "");
          if (s.includes(",") || s.includes('"')) {
            return `"${s.replace(/"/g, '""')}"`;
          }
          return s;
        })
        .join(",")
    );
  }

  fs.writeFileSync(outPath, lines.join("\n"), "utf-8");
  console.log(`[INFO] CSV gespeichert: ${outPath}`);
  return outPath;
}

async function main() {
  try {
    console.log(`[INFO] Starte Appcast-Scraper für ${START_DATE} bis ${END_DATE}`);

    // 1. Login-Seite laden + Token holen
    const { html, cookies: initialCookies } = await fetchLoginPage();
    const authenticityToken = extractAuthenticityToken(html);

    // 2. Login durchführen
    const { cookieHeader, csrfToken } = await login(authenticityToken, initialCookies);

    // 3. API aufrufen
    const json = await fetchJobs(cookieHeader, csrfToken);

    // 4. Speichern
    const jsonPath = saveJson(json);
    const csvPath = saveCsv(json);

    console.log("[INFO] Scraper erfolgreich beendet.");
    console.log(`[INFO] Output JSON: ${jsonPath}`);
    if (csvPath) console.log(`[INFO] Output CSV: ${csvPath}`);

  } catch (err) {
    console.error("[FATAL] Scraper fehlgeschlagen:");
    console.error(err);
    process.exit(1);
  }
}

main();
