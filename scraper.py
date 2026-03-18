"""
scraper.py
----------
Fetches RSS headlines from six major Mexican news sources, translates them
from Spanish to English, categorises each story, and writes/updates a
rolling JSON file (mexico_news.json) capped at 20 stories per category.
Stories older than 7 days are automatically dropped.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests
from dateutil import parser as dateutil_parser
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_FILE = Path("mexico_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# RSS feeds: (source_name, feed_url)
# Multiple feeds per outlet increase coverage for section-specific content.
RSS_FEEDS = [
    # El Universal — main feed + politics section
    ("El Universal",      "https://www.eluniversal.com.mx/rss.xml"),
    ("El Universal",      "https://www.eluniversal.com.mx/nation/rss.xml"),
    # Animal Político — WordPress default feed
    ("Animal Político",   "https://animalpolitico.com/feed"),
    ("Animal Político",   "https://animalpolitico.com/politica/feed"),
    # Milenio — main feed + politics section
    ("Milenio",           "https://www.milenio.com/feed"),
    ("Milenio",           "https://www.milenio.com/politica/rss"),
    # Aristegui Noticias — confirmed feeds
    ("Aristegui Noticias","https://aristeguinoticias.com/feed/"),
    ("Aristegui Noticias","https://aristeguinoticias.com/category/mexico/feed/"),
    # Sin Embargo — confirmed feed
    ("Sin Embargo",       "https://www.sinembargo.mx/feed"),
    ("Sin Embargo",       "https://www.sinembargo.mx/politica/feed"),
    # La Jornada — confirmed edition feed + politics
    ("La Jornada",        "https://www.jornada.com.mx/rss/edicion.xml"),
    ("La Jornada",        "https://www.jornada.com.mx/rss/politica.xml"),
]

# Keyword lists used for rule-based categorisation (lowercase, Spanish + English)
CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacia", "relaciones exteriores", "cancillería", "canciller",
        "embajada", "embajador", "tratado", "acuerdo internacional",
        "onu", "oea", "cumbre", "bilateral", "multilateral",
        "política exterior", "sanciones", "diplomático",
        "diplomacy", "foreign affairs", "ambassador", "embassy",
        "treaty", "international agreement", "sanctions", "summit",
        "secretaría de relaciones exteriores", "sre",
        "estados unidos", "usa", "trump", "aranceles", "tariff",
        "migración", "migrante", "deportación", "deportado",
        "frontera", "border",
    ],
    "Military": [
        "ejército", "fuerzas armadas", "sedena", "semar", "marina",
        "guardia nacional", "militar", "operativo", "cartel",
        "crimen organizado", "narcotráfico", "narco", "sicario",
        "balacera", "enfrentamiento", "detención", "captura",
        "liderazgo criminal", "cjng", "sinaloa", "zetas",
        "military", "army", "navy", "national guard", "operation",
        "cartel", "organized crime", "drug trafficking", "gunfight",
        "arrests", "capture", "criminal leader",
        "seguridad", "violencia", "homicidio", "extorsión",
    ],
    "Energy": [
        "pemex", "cfe", "comisión federal de electricidad",
        "petróleo", "gas natural", "energía renovable", "solar",
        "eólica", "hidroeléctrica", "refinería", "ducto",
        "gasolina", "precio combustible", "electricidad", "red eléctrica",
        "sener", "secretaría de energía", "litio", "minería",
        "energy", "oil", "gas", "petroleum", "refinery", "pipeline",
        "electricity", "renewable", "solar", "wind", "hydroelectric",
        "fuel price", "lithium", "mining",
    ],
    "Economy": [
        "economía", "pib", "inflación", "banco de méxico", "banxico",
        "tipo de cambio", "peso", "dólar", "inversión", "nearshoring",
        "exportaciones", "importaciones", "comercio", "presupuesto",
        "shcp", "hacienda", "deuda", "finanzas", "desempleo", "empleo",
        "salario mínimo", "reforma fiscal", "impuesto",
        "economy", "gdp", "inflation", "central bank", "exchange rate",
        "investment", "exports", "imports", "trade", "budget",
        "debt", "finance", "unemployment", "employment", "wage",
        "tax reform", "fiscal",
        "imf", "fmi", "world bank", "banco mundial",
    ],
    "Local Events": [
        "ciudad de méxico", "cdmx", "estado de méxico", "jalisco",
        "nuevo león", "veracruz", "chiapas", "oaxaca", "puebla",
        "guanajuato", "michoacán", "guerrero", "tamaulipas", "sonora",
        "sismo", "terremoto", "inundación", "huracán", "tormenta",
        "protesta", "manifestación", "marcha", "alcaldía", "municipio",
        "gobernador", "alcalde", "presidente municipal",
        "local", "state", "municipal", "earthquake", "flood",
        "hurricane", "protest", "demonstration", "march", "mayor",
        "governor",
    ],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

translator = GoogleTranslator(source="es", target="en")


def safe_translate(text: str, max_retries: int = 3) -> str:
    """Translate text from Spanish to English with retries and fallback."""
    if not text or not text.strip():
        return text
    for attempt in range(max_retries):
        try:
            translated = translator.translate(text[:4999])  # API limit
            return translated if translated else text
        except Exception as exc:
            log.warning(f"Translation attempt {attempt + 1} failed: {exc}")
            time.sleep(2 ** attempt)
    log.error(f"All translation attempts failed; returning original text.")
    return text


def parse_date(entry) -> datetime | None:
    """Try to extract a timezone-aware datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        struct = getattr(entry, attr, None)
        if struct:
            try:
                return datetime(*struct[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                dt = dateutil_parser.parse(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    return None


def categorise(title: str, summary: str) -> str | None:
    """
    Assign the most likely category based on keyword frequency in the
    combined Spanish title + summary text.  Returns None if no keywords match.
    """
    combined = (title + " " + (summary or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}

    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                scores[cat] += 1

    best_cat, best_score = max(scores.items(), key=lambda x: x[1])
    return best_cat if best_score > 0 else None


def fetch_feed(source: str, url: str) -> list[dict]:
    """Parse a single RSS feed and return a list of raw story dicts."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; MexicoNewsBot/1.0; "
            "+https://github.com/your-org/your-repo)"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning(f"[{source}] Could not fetch {url}: {exc}")
        return []

    if feed.bozo and not feed.entries:
        log.warning(f"[{source}] Feed parse error ({url}): {feed.bozo_exception}")
        return []

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    stories = []

    for entry in feed.entries:
        pub_date = parse_date(entry)

        # Skip entries we cannot date or that are too old
        if pub_date is None:
            log.debug(f"[{source}] Skipping undated entry: {entry.get('title', '')}")
            continue
        if pub_date < cutoff:
            continue

        title_es = entry.get("title", "").strip()
        summary_es = entry.get("summary", "").strip()
        link = entry.get("link", "").strip()

        if not title_es or not link:
            continue

        category = categorise(title_es, summary_es)
        if category is None:
            log.debug(f"[{source}] No category matched: {title_es}")
            continue

        stories.append({
            "title_es": title_es,
            "summary_es": summary_es,
            "source": source,
            "url": link,
            "published_date": pub_date.isoformat(),
            "category": category,
        })

    log.info(f"[{source}] {len(stories)} relevant stories from {url}")
    return stories


def translate_stories(stories: list[dict]) -> list[dict]:
    """Add English 'title' field to each story dict in-place."""
    for story in stories:
        story["title"] = safe_translate(story.pop("title_es"))
        story.pop("summary_es", None)  # Not included in final output
        time.sleep(0.15)               # Light throttle to respect API rate limits
    return stories


def load_existing() -> dict[str, list[dict]]:
    """Load the existing JSON output, returning an empty structure on failure."""
    if OUTPUT_FILE.exists():
        try:
            with OUTPUT_FILE.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
                # Validate structure
                if isinstance(data, dict) and all(
                    cat in data for cat in CATEGORIES
                ):
                    return data
        except Exception as exc:
            log.warning(f"Could not load existing JSON ({exc}); starting fresh.")
    return {cat: [] for cat in CATEGORIES}


def save_output(data: dict[str, list[dict]]) -> None:
    with OUTPUT_FILE.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    total = sum(len(v) for v in data.values())
    log.info(f"Saved {OUTPUT_FILE} with {total} stories across {len(CATEGORIES)} categories.")


# ---------------------------------------------------------------------------
# Core update logic
# ---------------------------------------------------------------------------

def update_category(
    existing: list[dict],
    new_stories: list[dict],
    cutoff: datetime,
) -> list[dict]:
    """
    Merge new stories into the existing list for one category:
      1. Drop stories older than MAX_AGE_DAYS.
      2. Deduplicate by URL.
      3. Add new stories, oldest-first (so we replace oldest entries when > cap).
      4. If over cap, remove oldest entries first.
    Returns the updated list sorted newest-first.
    """
    # Remove expired entries
    existing = [
        s for s in existing
        if dateutil_parser.parse(s["published_date"]) >= cutoff
    ]

    existing_urls = {s["url"] for s in existing}
    truly_new = [s for s in new_stories if s["url"] not in existing_urls]

    merged = existing + truly_new

    # Sort newest-first, then trim oldest entries to stay within cap
    merged.sort(key=lambda s: s["published_date"], reverse=True)
    return merged[:MAX_PER_CATEGORY]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    log.info("=== Mexico News Scraper starting ===")
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    # 1. Collect all raw stories from every feed
    raw_by_category: dict[str, list[dict]] = {cat: [] for cat in CATEGORIES}
    seen_urls: set[str] = set()

    for source, url in RSS_FEEDS:
        stories = fetch_feed(source, url)
        for story in stories:
            if story["url"] not in seen_urls:
                seen_urls.add(story["url"])
                raw_by_category[story["category"]].append(story)

    total_raw = sum(len(v) for v in raw_by_category.values())
    log.info(f"Fetched {total_raw} unique categorised stories before translation.")

    # 2. Translate titles
    for cat in CATEGORIES:
        if raw_by_category[cat]:
            log.info(f"Translating {len(raw_by_category[cat])} stories in [{cat}]…")
            raw_by_category[cat] = translate_stories(raw_by_category[cat])

    # 3. Load existing data and merge
    existing_data = load_existing()

    updated_data: dict[str, list[dict]] = {}
    for cat in CATEGORIES:
        updated_data[cat] = update_category(
            existing_data.get(cat, []),
            raw_by_category[cat],
            cutoff,
        )
        log.info(f"[{cat}] {len(updated_data[cat])} stories after merge.")

    # 4. Persist
    save_output(updated_data)
    log.info("=== Scraper finished ===")


if __name__ == "__main__":
    main()
