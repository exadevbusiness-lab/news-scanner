import re
import html
import time
import sqlite3
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

import requests
import feedparser
import pandas as pd
import streamlit as st


DB_PATH = "news_cache.db"
USER_AGENT = "Mozilla/5.0 (compatible; NewsScanner/1.0)"


RSS_SOURCES = {
    "SVT Nyheter": "https://www.svt.se/nyheter/rss.xml",
    "Sveriges Radio Ekot": "https://feeds.sr.se/podcast/rss.xml?programid=83",
    "Dagens Nyheter": "https://www.dn.se/arc/outboundfeeds/rss/?outputType=xml",
    "Expressen": "https://feeds.expressen.se/nyheter/",
    "Aftonbladet": "https://rss.aftonbladet.se/rss2/small/pages/sections/senastenytt/",
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Reuters World": "https://feeds.reuters.com/Reuters/worldNews",
    "The Guardian World": "https://www.theguardian.com/world/rss",
    "CNN World": "http://rss.cnn.com/rss/edition_world.rss",
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            title TEXT,
            url TEXT UNIQUE,
            published TEXT,
            summary TEXT,
            matched_keywords TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def save_articles(articles):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for article in articles:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO articles
                (source, title, url, published, summary, matched_keywords, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article.get("source", ""),
                    article.get("title", ""),
                    article.get("url", ""),
                    article.get("published", ""),
                    article.get("summary", ""),
                    article.get("matched_keywords", ""),
                    article.get("fetched_at", ""),
                ),
            )
        except sqlite3.Error:
            pass

    conn.commit()
    conn.close()


def load_articles():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM articles ORDER BY published DESC", conn)
    conn.close()
    return df


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_date(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def parse_feed_date(entry):
    for key in ["published", "updated", "created"]:
        if key in entry:
            try:
                dt = parsedate_to_datetime(entry[key])
                return normalize_date(dt)
            except Exception:
                continue
    return None


def keyword_match(text, keywords):
    text_lower = text.lower()
    matched = [kw for kw in keywords if kw.lower() in text_lower]
    return matched


def fetch_rss(selected_sources, keywords, from_dt, to_dt):
    headers = {"User-Agent": USER_AGENT}
    results = []

    for source_name in selected_sources:
        rss_url = RSS_SOURCES.get(source_name)
        if not rss_url:
            continue

        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            for entry in feed.entries:
                title = clean_text(entry.get("title", ""))
                summary = clean_text(entry.get("summary", ""))
                url = entry.get("link", "")
                published_dt = parse_feed_date(entry)

                if not published_dt:
                    continue

                if from_dt and published_dt < from_dt:
                    continue
                if to_dt and published_dt > to_dt:
                    continue

                searchable_text = f"{title} {summary}"
                matched = keyword_match(searchable_text, keywords)

                if keywords and not matched:
                    continue

                results.append(
                    {
                        "source": source_name,
                        "title": title,
                        "url": url,
                        "published": published_dt.isoformat(),
                        "summary": summary,
                        "matched_keywords": ", ".join(matched),
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

        except Exception:
            continue

    return results


def fetch_gdelt(keywords, from_dt, to_dt, max_records=50):
    if not keywords:
        return []

    query = " OR ".join([f'"{kw}"' if " " in kw else kw for kw in keywords])

    if not query.strip():
        return []

    mode = "ArtList"
    format_type = "json"

    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(query)}"
        f"&mode={mode}"
        f"&maxrecords={max_records}"
        f"&format={format_type}"
    )

    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        data = response.json()
    except Exception:
        return []

    articles = []

    for item in data.get("articles", []):
        title = clean_text(item.get("title", ""))
        url = item.get("url", "")
        source = item.get("sourceCommonName") or item.get("domain") or "GDELT"
        summary = clean_text(item.get("seendate", ""))

        published_raw = item.get("seendate", "")
        published_dt = None

        try:
            # GDELT format example: 20260308T123000Z
            published_dt = datetime.strptime(published_raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if from_dt and published_dt < from_dt:
            continue
        if to_dt and published_dt > to_dt:
            continue

        searchable_text = f"{title} {summary}"
        matched = keyword_match(searchable_text, keywords)

        if keywords and not matched:
            continue

        articles.append(
            {
                "source": source,
                "title": title,
                "url": url,
                "published": published_dt.isoformat(),
                "summary": summary,
                "matched_keywords": ", ".join(matched),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return articles


def filter_df(df, keywords, selected_sources, from_dt, to_dt):
    if df.empty:
        return df

    if selected_sources:
        df = df[df["source"].isin(selected_sources)]

    if keywords:
        pattern = "|".join([re.escape(kw) for kw in keywords])
        df = df[
            df["title"].str.contains(pattern, case=False, na=False)
            | df["summary"].str.contains(pattern, case=False, na=False)
            | df["matched_keywords"].str.contains(pattern, case=False, na=False)
        ]

    if from_dt:
        df = df[pd.to_datetime(df["published"], utc=True) >= pd.Timestamp(from_dt)]

    if to_dt:
        df = df[pd.to_datetime(df["published"], utc=True) <= pd.Timestamp(to_dt)]

    df = df.sort_values("published", ascending=False)
    return df


def make_clickable(url, title):
    safe_title = html.escape(title or url)
    return f'<a href="{url}" target="_blank">{safe_title}</a>'


st.set_page_config(page_title="Nyhetsscanner", layout="wide")
st.title("Nyhetsscanner")
st.caption("Sök nyheter via RSS och GDELT med egna sökord, källor och tidsperiod.")

init_db()

with st.sidebar:
    st.header("Inställningar")

    keyword_input = st.text_area(
        "Nyckelord/sökord",
        placeholder="Exempel: NATO, Karlskoga, Ukraina, försörjningstrygghet",
        height=120,
    )

    selected_sources = st.multiselect(
        "Källor",
        options=list(RSS_SOURCES.keys()),
        default=["SVT Nyheter", "Sveriges Radio Ekot", "BBC World", "Reuters World", "The Guardian World"],
    )

    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("Från datum", value=None)
    with col2:
        to_date = st.date_input("Till datum", value=None)

    use_gdelt = st.checkbox("Använd även GDELT", value=True)

    fetch_now = st.button("Hämta nyheter nu", type="primary")
    reload_db = st.button("Ladda från lokal databas")

keywords = [k.strip() for k in re.split(r"[,;\n]+", keyword_input) if k.strip()]

from_dt = None
to_dt = None

if from_date:
    from_dt = datetime.combine(from_date, datetime.min.time()).replace(tzinfo=timezone.utc)

if to_date:
    to_dt = datetime.combine(to_date, datetime.max.time()).replace(tzinfo=timezone.utc)

if fetch_now:
    with st.spinner("Hämtar nyheter..."):
        all_articles = []

        rss_articles = fetch_rss(selected_sources, keywords, from_dt, to_dt)
        all_articles.extend(rss_articles)

        if use_gdelt:
            gdelt_articles = fetch_gdelt(keywords, from_dt, to_dt, max_records=100)
            all_articles.extend(gdelt_articles)

        # Deduplicera på URL
        dedup = {}
        for article in all_articles:
            url = article.get("url", "").strip()
            if url:
                dedup[url] = article

        unique_articles = list(dedup.values())
        save_articles(unique_articles)

        st.success(f"Hämtade {len(unique_articles)} artiklar.")

if reload_db or fetch_now:
    df = load_articles()
else:
    df = load_articles()

filtered_df = filter_df(df, keywords, selected_sources, from_dt, to_dt)

st.subheader("Resultat")

if filtered_df.empty:
    st.info("Inga träffar hittades.")
else:
    display_df = filtered_df.copy()
    display_df["published"] = pd.to_datetime(display_df["published"], utc=True).dt.strftime("%Y-%m-%d %H:%M")
    display_df["Länk"] = display_df.apply(lambda row: make_clickable(row["url"], row["title"]), axis=1)

    display_df = display_df[
        ["published", "source", "matched_keywords", "Länk", "summary"]
    ].rename(
        columns={
            "published": "Publicerad",
            "source": "Källa",
            "matched_keywords": "Matchade sökord",
            "summary": "Sammanfattning / metadata",
        }
    )

    st.markdown(
        display_df.to_html(escape=False, index=False),
        unsafe_allow_html=True,
    )

    csv_data = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Ladda ner resultat som CSV",
        data=csv_data,
        file_name="nyhetstraffar.csv",
        mime="text/csv",
    )