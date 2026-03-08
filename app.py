import re
import html
import sqlite3
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

import requests
import feedparser
import pandas as pd
import streamlit as st


DB_PATH = "news_cache.db"
USER_AGENT = "Mozilla/5.0 (compatible; NewsScanner/2.0)"


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
            source_type TEXT,
            title TEXT,
            url TEXT UNIQUE,
            published TEXT,
            summary TEXT,
            matched_keywords TEXT,
            fetched_at TEXT
        )
        """
    )

    # Lägg till kolumn om databasen skapats av äldre version
    try:
        cur.execute("ALTER TABLE articles ADD COLUMN source_type TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def save_articles(articles):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for article in articles:
        try:
            cur.execute(
                """
                INSERT OR REPLACE INTO articles
                (source, source_type, title, url, published, summary, matched_keywords, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article.get("source", ""),
                    article.get("source_type", ""),
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


def parse_keywords(text):
    if not text:
        return []
    parts = re.split(r"[,;\n]+", text)
    return [p.strip() for p in parts if p.strip()]


def keyword_match(text, keywords, match_mode="any"):
    if not keywords:
        return []

    text_lower = text.lower()
    matched = [kw for kw in keywords if kw.lower() in text_lower]

    if match_mode == "all":
        return matched if len(matched) == len(keywords) else []

    return matched


def fetch_rss(selected_sources, keywords, from_dt, to_dt, match_mode="any"):
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

                if not url or not published_dt:
                    continue

                if from_dt and published_dt < from_dt:
                    continue
                if to_dt and published_dt > to_dt:
                    continue

                searchable_text = f"{title} {summary}"
                matched = keyword_match(searchable_text, keywords, match_mode=match_mode)

                if keywords and not matched:
                    continue

                results.append(
                    {
                        "source": source_name,
                        "source_type": "RSS",
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


def fetch_gdelt(keywords, from_dt, to_dt, max_records=100, match_mode="any"):
    if not keywords:
        return []

    query = " OR ".join([f'"{kw}"' if " " in kw else kw for kw in keywords])
    if not query.strip():
        return []

    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(query)}"
        "&mode=ArtList"
        f"&maxrecords={max_records}"
        "&format=json"
    )

    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        data = response.json()
    except Exception:
        return []

    results = []

    for item in data.get("articles", []):
        title = clean_text(item.get("title", ""))
        url = item.get("url", "")
        source = item.get("sourceCommonName") or item.get("domain") or "GDELT"
        summary = clean_text(item.get("socialimage", "") or "")
        published_raw = item.get("seendate", "")

        try:
            published_dt = datetime.strptime(published_raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if not url or not title:
            continue

        if from_dt and published_dt < from_dt:
            continue
        if to_dt and published_dt > to_dt:
            continue

        searchable_text = f"{title} {summary} {source}"
        matched = keyword_match(searchable_text, keywords, match_mode=match_mode)

        if keywords and not matched:
            continue

        results.append(
            {
                "source": source,
                "source_type": "GDELT",
                "title": title,
                "url": url,
                "published": published_dt.isoformat(),
                "summary": "",
                "matched_keywords": ", ".join(matched),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return results


def filter_df(df, keywords, selected_rss_sources, from_dt, to_dt, include_gdelt, match_mode="any"):
    if df.empty:
        return df

    df["published_dt"] = pd.to_datetime(df["published"], utc=True, errors="coerce")
    df = df.dropna(subset=["published_dt"])

    if from_dt:
        df = df[df["published_dt"] >= pd.Timestamp(from_dt)]
    if to_dt:
        df = df[df["published_dt"] <= pd.Timestamp(to_dt)]

    # RSS filtreras på valda RSS-källor, GDELT får vara kvar om användaren valt GDELT
    rss_mask = (df["source_type"] == "RSS") & (df["source"].isin(selected_rss_sources))
    gdelt_mask = (df["source_type"] == "GDELT") if include_gdelt else False

    df = df[rss_mask | gdelt_mask]

    if keywords:
        if match_mode == "all":
            for kw in keywords:
                df = df[
                    df["title"].str.contains(re.escape(kw), case=False, na=False)
                    | df["summary"].str.contains(re.escape(kw), case=False, na=False)
                    | df["matched_keywords"].str.contains(re.escape(kw), case=False, na=False)
                ]
        else:
            pattern = "|".join(re.escape(kw) for kw in keywords)
            df = df[
                df["title"].str.contains(pattern, case=False, na=False)
                | df["summary"].str.contains(pattern, case=False, na=False)
                | df["matched_keywords"].str.contains(pattern, case=False, na=False)
            ]

    return df.sort_values("published_dt", ascending=False)


st.set_page_config(page_title="Nyhetsscanner", layout="wide")
st.title("Nyhetsscanner")
st.caption("Sök nyheter via RSS och GDELT med egna sökord, källor och tidsperiod.")

init_db()

with st.sidebar:
    st.header("Inställningar")

    keyword_input = st.text_area(
        "Nyckelord/sökord",
        value="",
        placeholder="Exempel:\nNATO\nSverige\nUkraina",
        help="Skriv ett sökord per rad, eller separera med kommatecken.",
        height=140,
    )

    match_mode = st.radio(
        "Träfflogik",
        options=["any", "all"],
        format_func=lambda x: "Minst ett sökord" if x == "any" else "Alla sökord måste finnas",
    )

    selected_sources = st.multiselect(
        "RSS-källor",
        options=list(RSS_SOURCES.keys()),
        default=["SVT Nyheter", "Sveriges Radio Ekot", "BBC World", "Reuters World", "The Guardian World"],
    )

    days_back = st.selectbox(
        "Tidsperiod",
        options=[1, 3, 7, 14, 30],
        index=2,
        format_func=lambda x: f"Senaste {x} dagarna",
    )

    use_gdelt = st.checkbox("Använd även GDELT", value=True)

    if st.button("Hämta nyheter nu", type="primary"):
        st.session_state["fetch_now"] = True

keywords = parse_keywords(keyword_input)

to_dt = datetime.now(timezone.utc)
from_dt = to_dt - timedelta(days=days_back)

if st.session_state.get("fetch_now"):
    with st.spinner("Hämtar nyheter..."):
        all_articles = []

        rss_articles = fetch_rss(
            selected_sources,
            keywords,
            from_dt,
            to_dt,
            match_mode=match_mode,
        )
        all_articles.extend(rss_articles)

        if use_gdelt:
            gdelt_articles = fetch_gdelt(
                keywords,
                from_dt,
                to_dt,
                max_records=100,
                match_mode=match_mode,
            )
            all_articles.extend(gdelt_articles)

        dedup = {}
        for article in all_articles:
            url = article.get("url", "").strip()
            if url:
                dedup[url] = article

        unique_articles = list(dedup.values())
        save_articles(unique_articles)

        st.success(f"Hämtade {len(unique_articles)} artiklar.")
        st.session_state["fetch_now"] = False

df = load_articles()
filtered_df = filter_df(
    df,
    keywords,
    selected_sources,
    from_dt,
    to_dt,
    use_gdelt,
    match_mode=match_mode,
)

st.subheader("Resultat")

if filtered_df.empty:
    st.info("Inga träffar hittades.")
else:
    display_df = filtered_df.copy()
    display_df["publicerad"] = display_df["published_dt"].dt.strftime("%Y-%m-%d %H:%M")
    display_df["öppna"] = display_df["url"]

    display_df = display_df[
        ["publicerad", "source_type", "source", "matched_keywords", "title", "öppna"]
    ].rename(
        columns={
            "publicerad": "Publicerad",
            "source_type": "Typ",
            "source": "Källa",
            "matched_keywords": "Sökordsträff",
            "title": "Rubrik",
            "öppna": "Länk",
        }
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Länk": st.column_config.LinkColumn(
                "Länk",
                display_text="Öppna artikel",
            )
        },
    )

    csv_data = filtered_df.drop(columns=["published_dt"], errors="ignore").to_csv(index=False).encode("utf-8")
    st.download_button(
        "Ladda ner resultat som CSV",
        data=csv_data,
        file_name="nyhetstraffar.csv",
        mime="text/csv",
    )
