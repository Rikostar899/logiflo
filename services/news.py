import streamlit as st
import requests
import xml.etree.ElementTree as _ET
import datetime

NEWS_QUERIES = {
    "transport_routier":    "transport routier PME carburant CNR France actualite",
    "transport_maritime":   "fret maritime conteneur Marseille logistique actualite",
    "transport_aerien_intl":"fret aerien cargo IATA logistique international",
    "transport_routier_eu": "transport routier international Europe logistique",
    "stock_pharma":         "rupture medicaments supply chain pharmaceutique France",
    "stock_agroalim":       "logistique agroalimentaire froid chaine approvisionnement",
    "stock_distribution":   "distribution logistique entrepot stock rupture",
    "stock_industrie":      "logistique industrielle supply chain production",
    "stock_retail":         "logistique retail e-commerce stock gestion",
    "stock_btp":            "logistique chantier BTP materiau construction",
    "supply_chain_maghreb": "logistique Maroc transport Casablanca supply chain",
    "supply_chain_afrique": "logistique Afrique Abidjan Dakar transport",
    "transport_maritime_intl":"maritime shipping container freight port international",
    "generique":            "supply chain logistique France actualite innovation",
}


def fetch_news_google_rss(query, lang="fr", country="FR", max_results=5):
    try:
        import urllib.parse
        url = (f"https://news.google.com/rss/search"
               f"?q={urllib.parse.quote(query)}"
               f"&hl={lang}-{country}&gl={country}&ceid={country}:{lang}")
        r = requests.get(url, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0 Logiflo/1.0"})
        root = _ET.fromstring(r.content)
        items = root.findall(".//item")[:max_results]
        articles = []
        for item in items:
            title = item.findtext("title", "").split(" - ")[0].strip()
            link  = item.findtext("link", "")
            date  = item.findtext("pubDate", "")[:16]
            if title and link:
                articles.append({"title": title, "link": link, "date": date})
        return articles
    except Exception:
        return []


def fetch_news_newsapi(query, lang="fr", max_results=5):
    try:
        key = st.secrets.get("NEWSAPI_KEY", "")
        if not key:
            return []
        r = requests.get("https://newsapi.org/v2/everything", params={
            "q": query, "language": lang, "sortBy": "publishedAt",
            "pageSize": max_results, "apiKey": key,
        }, timeout=8)
        articles = []
        for a in r.json().get("articles", [])[:max_results]:
            articles.append({
                "title": a.get("title", "").split(" - ")[0][:80],
                "link":  a.get("url", ""),
                "date":  (a.get("publishedAt", "") or "")[:10],
            })
        return articles
    except Exception:
        return []


def get_sector_news(sector_key, lang="fr"):
    from services.supabase_client import get_supabase
    sb = get_supabase()
    if sb:
        try:
            resp = (sb.table("news_cache")
                      .select("articles,fetched_at")
                      .eq("sector_key", sector_key)
                      .order("fetched_at", desc=True)
                      .limit(1)
                      .execute())
            if resp.data:
                fetched = datetime.datetime.fromisoformat(
                    resp.data[0]["fetched_at"].replace("Z", ""))
                age_h = (datetime.datetime.utcnow() - fetched).total_seconds() / 3600
                if age_h < 4:
                    return resp.data[0]["articles"] or []
        except Exception:
            pass

    query = NEWS_QUERIES.get(sector_key, NEWS_QUERIES["generique"])
    country = "MA" if "maghreb" in sector_key else ("CI" if "afrique" in sector_key else "FR")
    articles = fetch_news_google_rss(query, lang=lang, country=country)
    if not articles:
        articles = fetch_news_newsapi(query, lang=lang)

    if sb and articles:
        try:
            sb.table("news_cache").insert({
                "sector_key": sector_key,
                "articles":   articles,
                "fetched_at": datetime.datetime.now().isoformat(),
            }).execute()
        except Exception:
            pass
    return articles


def render_news_widget(sector_key, lang="fr"):
    news = get_sector_news(sector_key, lang)
    _arts = [a for a in (news or []) if str(a.get("title", "")).strip() and a.get("link", "")]
    if not _arts:
        return

    _lbl  = "Actualites sectorielles" if lang == "fr" else "Sector News"
    _read = "Lire l article complet" if lang == "fr" else "Read full article"
    _uid  = abs(hash(sector_key + lang)) % 999999

    _cards = ""
    for _a in _arts[:6]:
        _t   = str(_a.get("title", ""))[:150].replace("<", "&lt;").replace(">", "&gt;").replace("'", "&#39;")
        _l   = str(_a.get("link", ""))
        _d   = str(_a.get("date", ""))[:10]
        _src = _l.split("/")[2].replace("www.", "") if "://" in _l else ""
        _ap  = (_t[:180] + "...") if len(_t) > 180 else _t
        _cards += (
            f'<div class="ns{_uid}" style="min-width:100%;box-sizing:border-box;'
            f'padding:24px 28px;background:white;border-top:4px solid #00C896;cursor:pointer;"'
            f' onclick="window.open(\'{_l}\',\'_blank\')">'
            f'<div style="font-size:10px;font-weight:700;color:#00C896;text-transform:uppercase;'
            f'letter-spacing:1.5px;margin-bottom:10px;">{_src}</div>'
            f'<div style="font-size:17px;font-weight:800;color:#0B2545;line-height:1.4;'
            f'margin-bottom:12px;">{_t}</div>'
            f'<div style="font-size:13px;color:#4A6080;line-height:1.65;margin-bottom:18px;'
            f'overflow:hidden;display:-webkit-box;-webkit-line-clamp:3;'
            f'-webkit-box-orient:vertical;">{_ap}</div>'
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'border-top:1px solid #F0F4F8;padding-top:14px;">'
            f'<span style="font-size:11px;color:#8FA3BC;">{_d}</span>'
            f'<span style="font-size:12px;font-weight:700;color:white;background:#00C896;'
            f'padding:6px 16px;border-radius:20px;">{_read} &rarr;</span>'
            f'</div></div>'
        )

    _n = len(_arts[:6])
    st.markdown(f"""
<div style="font-size:11px;font-weight:700;color:#4A6080;letter-spacing:2px;
            text-transform:uppercase;margin-bottom:12px;margin-top:24px;">
    📰 {_lbl}
</div>
<style>
#nw{_uid}{{overflow:hidden;border-radius:14px;box-shadow:0 2px 20px rgba(11,37,69,0.10);}}
#ntr{_uid}{{display:flex;transition:transform 0.55s cubic-bezier(.4,0,.2,1);will-change:transform;}}
.ns{_uid}:hover{{background:#F0FDF9 !important;}}
#nd{_uid}{{display:flex;gap:6px;justify-content:center;padding:10px 0 2px;}}
.ndot{_uid}{{width:7px;height:7px;border-radius:50%;background:#E2E8F0;transition:all 0.3s;cursor:pointer;border:none;padding:0;}}
.ndot{_uid}.act{{background:#00C896;width:22px;border-radius:4px;}}
</style>
<div id="nw{_uid}"><div id="ntr{_uid}">{_cards}</div></div>
<div id="nd{_uid}">
{"".join(f'<button class="ndot{_uid}{" act" if i==0 else ""}" onclick="nwG{_uid}({i})"></button>' for i in range(_n))}
</div>
<script>
(function(){{
  var c=0,n={_n};
  var tr=document.getElementById("ntr{_uid}");
  var ds=document.querySelectorAll("#nd{_uid} .ndot{_uid}");
  function go(i){{c=i;tr.style.transform="translateX(-"+i*100+"%)";
    ds.forEach(function(d,j){{d.classList.toggle("act",j===i);}});}}
  window["nwG{_uid}"]=go;
  setInterval(function(){{go((c+1)%n);}},5000);
}})();
</script>
""", unsafe_allow_html=True)
