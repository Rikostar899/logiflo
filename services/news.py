# -*- coding: utf-8 -*-
"""
Logiflo - services/news.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Veille sectorielle (Google RSS + NewsAPI) + widget carousel
Version 7.0 (juillet 2026) — carousel via components.html (JS execute)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import requests
import re
import os
import xml.etree.ElementTree as _ET
import datetime

NEWS_QUERIES = {
    "transport_routier":     "transport routier PME carburant CNR France actualite",
    "transport_maritime":    "fret maritime conteneur Marseille logistique actualite",
    "transport_aerien_intl": "fret aerien cargo IATA logistique international",
    "transport_routier_eu":  "transport routier international Europe logistique",
    "stock_pharma":          "rupture medicaments supply chain pharmaceutique France",
    "stock_agroalim":        "logistique agroalimentaire froid chaine approvisionnement",
    "stock_distribution":    "distribution logistique entrepot stock rupture",
    "stock_industrie":       "logistique industrielle supply chain production",
    "stock_retail":          "logistique retail e-commerce stock gestion",
    "stock_btp":             "logistique chantier BTP materiau construction",
    "supply_chain_maghreb":  "logistique Maroc transport Casablanca supply chain",
    "supply_chain_afrique":  "logistique Afrique Abidjan Dakar transport",
    "transport_maritime_intl": "maritime shipping container freight port international",
    "generique":             "supply chain logistique France actualite innovation",
}


def fetch_news_google_rss(query, lang="fr", country="FR", max_results=5):
    """Recupere les actualites via Google News RSS."""
    try:
        import urllib.parse
        url = (
            f"https://news.google.com/rss/search"
            f"?q={urllib.parse.quote(query)}"
            f"&hl={lang}-{country}&gl={country}&ceid={country}:{lang}"
        )
        r = requests.get(
            url, timeout=8,
            headers={"User-Agent": "Mozilla/5.0 Logiflo/2.0"}
        )
        root = _ET.fromstring(r.content)
        items = root.findall(".//item")[:max_results]
        articles = []
        for item in items:
            title = item.findtext("title", "").split(" - ")[0].strip()
            link = item.findtext("link", "")
            date = item.findtext("pubDate", "")[:16]
            desc_raw = item.findtext("description", "")
            desc_clean = re.sub(r'<[^>]+>', ' ', desc_raw).strip()
            desc_clean = re.sub(r'\s+', ' ', desc_clean)[:400]
            if title and link:
                articles.append({
                    "title": title, "link": link,
                    "date": date, "desc": desc_clean,
                })
        return articles
    except Exception:
        return []


def fetch_news_newsapi(query, lang="fr", max_results=5):
    """Recupere les actualites via NewsAPI (fallback)."""
    try:
        key = os.environ.get("NEWSAPI_KEY", "")
        if not key:
            try:
                key = st.secrets.get("NEWSAPI_KEY", "")
            except Exception:
                key = ""
        if not key:
            return []
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": query, "language": lang, "sortBy": "publishedAt",
                "pageSize": max_results, "apiKey": key,
            },
            timeout=8,
        )
        articles = []
        for a in r.json().get("articles", [])[:max_results]:
            articles.append({
                "title": a.get("title", "").split(" - ")[0][:80],
                "link": a.get("url", ""),
                "date": (a.get("publishedAt", "") or "")[:10],
                "desc": (a.get("description", "") or "")[:400],
            })
        return articles
    except Exception:
        return []


def get_sector_news(sector_key, lang="fr"):
    """Recupere les actus sectorielles (cache Supabase 4h + RSS + NewsAPI)."""
    from services.supabase_client import get_supabase
    sb = get_supabase()

    # Cache Supabase (TTL 4h)
    if sb:
        try:
            resp = (
                sb.table("news_cache")
                .select("articles,fetched_at")
                .eq("sector_key", sector_key)
                .order("fetched_at", desc=True)
                .limit(1)
                .execute()
            )
            if resp.data:
                fetched = datetime.datetime.fromisoformat(
                    resp.data[0]["fetched_at"].replace("Z", "")
                )
                age_h = (datetime.datetime.utcnow() - fetched).total_seconds() / 3600
                if age_h < 4:
                    return resp.data[0]["articles"] or []
        except Exception:
            pass

    # Fetch frais
    query = NEWS_QUERIES.get(sector_key, NEWS_QUERIES["generique"])
    country = (
        "MA" if "maghreb" in sector_key
        else ("CI" if "afrique" in sector_key else "FR")
    )
    articles = fetch_news_google_rss(query, lang=lang, country=country)
    if not articles:
        articles = fetch_news_newsapi(query, lang=lang)

    # Sauvegarder en cache
    if sb and articles:
        try:
            sb.table("news_cache").insert({
                "sector_key": sector_key,
                "articles": articles,
                "fetched_at": datetime.datetime.now().isoformat(),
            }).execute()
        except Exception:
            pass

    return articles


def render_news_widget(sector_key, lang="fr"):
    """Affiche le carousel d'actualites sectorielles (defilement auto).

    CHANGEMENT V7 : utilise st.components.v1.html au lieu de st.markdown.
    st.markdown NE PEUT PAS executer de JavaScript (Streamlit le neutralise),
    c'est pourquoi le carousel ne defilait pas. components.v1.html execute
    le JS dans une iframe isolee -> le defilement automatique fonctionne.
    """
    import streamlit.components.v1 as _components

    news = get_sector_news(sector_key, lang)
    _arts = [
        a for a in (news or [])
        if str(a.get("title", "")).strip() and a.get("link", "")
    ]
    if not _arts:
        return

    _lbl = "Actualites sectorielles" if lang == "fr" else "Sector News"
    _read = "Lire l article" if lang == "fr" else "Read article"
    _uid = abs(hash(sector_key + lang)) % 999999
    _n = min(len(_arts), 6)

    # ── Construction des cartes ──
    _cards = ""
    for _a in _arts[:_n]:
        _t = str(_a.get("title", ""))[:150].replace("<", "&lt;").replace(">", "&gt;")
        _l = str(_a.get("link", ""))
        _d = str(_a.get("date", ""))[:16]
        _desc = str(_a.get("desc", ""))[:300].replace("<", "&lt;").replace(">", "&gt;")
        _src = _l.split("/")[2].replace("www.", "") if "://" in _l else ""
        _cards += (
            f'<a href="{_l}" target="_blank" rel="noopener" '
            f'class="ns{_uid}" style="width:100%;flex-shrink:0;box-sizing:border-box;'
            f'padding:20px 24px;background:white;border-top:4px solid #00C896;'
            f'cursor:pointer;text-decoration:none;display:block;">'
            f'<div style="font-size:10px;font-weight:700;color:#00C896;'
            f'text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px;">{_src}</div>'
            f'<div style="font-size:15px;font-weight:800;color:#0B2545;line-height:1.4;'
            f'margin-bottom:10px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;'
            f'-webkit-box-orient:vertical;">{_t}</div>'
            f'<div style="font-size:12px;color:#4A6080;line-height:1.65;'
            f'margin-bottom:14px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:4;'
            f'-webkit-box-orient:vertical;">{_desc if _desc else _t}</div>'
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'border-top:1px solid #F0F4F8;padding-top:10px;">'
            f'<span style="font-size:11px;color:#8FA3BC;">{_d}</span>'
            f'<span style="font-size:11px;font-weight:700;color:white;background:#00C896;'
            f'padding:4px 14px;border-radius:20px;">{_read} &rarr;</span>'
            f'</div></a>'
        )

    # ── Dots navigation ──
    _dots = ""
    for i in range(_n):
        _act = " act" if i == 0 else ""
        _dots += f'<button class="ndot{_uid}{_act}" onclick="nwG{_uid}({i})"></button>'

    _title_html = (
        f'<div style="font-size:11px;font-weight:700;color:#4A6080;letter-spacing:2px;'
        f'text-transform:uppercase;margin-bottom:10px;font-family:sans-serif;">'
        f'&#128240; {_lbl}</div>'
    )

    # ── HTML COMPLET autonome (styles + cards + dots + script) ──
    _full = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  * {{ font-family:'Syne',-apple-system,BlinkMacSystemFont,sans-serif; box-sizing:border-box; }}
  body {{ margin:0; padding:0; background:transparent; }}
  #nw{_uid} {{ overflow:hidden; border-radius:12px; box-shadow:0 2px 16px rgba(11,37,69,0.08); }}
  #ntr{_uid} {{ display:flex; transition:transform 0.5s ease; will-change:transform; }}
  .ns{_uid}:hover {{ background:#F0FDF9 !important; }}
  #nd{_uid} {{ display:flex; gap:6px; justify-content:center; padding:12px 0 4px 0; }}
  .ndot{_uid} {{ width:7px; height:7px; border-radius:50%; background:#E2E8F0;
                 transition:all 0.3s; cursor:pointer; border:none; padding:0; }}
  .ndot{_uid}.act {{ background:#00C896; width:20px; border-radius:4px; }}
</style></head>
<body>
  {_title_html}
  <div id="nw{_uid}"><div id="ntr{_uid}">{_cards}</div></div>
  <div id="nd{_uid}">{_dots}</div>
<script>
(function(){{
  var tr=document.getElementById("ntr{_uid}");
  var c=0,n={_n};
  var ds=document.querySelectorAll("#nd{_uid} .ndot{_uid}");
  function go(i){{
    c=i;
    tr.style.transform='translateX(-'+(i*100)+'%)';
    for(var j=0;j<ds.length;j++){{ds[j].classList.toggle('act',j===i);}}
  }}
  window["nwG{_uid}"]=go;
  setInterval(function(){{go((c+1)%n);}},5000);
}})();
</script>
</body></html>"""

    _components.html(_full, height=290, scrolling=False)
