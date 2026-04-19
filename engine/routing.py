import streamlit as st
import requests
import math
import time
import concurrent.futures

from engine.ingester import nettoyer

try:
    ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")
except Exception:
    ORS_API_KEY = ""


def calculate_haversine(lon1, lat1, lon2, lat2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def fetch_geo(city, _t=None):
    if not city or str(city).strip() in ("", "nan", "None"):
        return city, None
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": str(city).strip(), "format": "json", "limit": 1},
            headers={"User-Agent": "Logiflo.io/2.0"},
            timeout=5
        )
        if r.status_code == 200:
            d = r.json()
            if d:
                return city, [float(d[0]["lon"]), float(d[0]["lat"])]
    except Exception:
        pass
    return city, None


def geocode_cities_mapbox(cities):
    villes = [c for c in set(str(v) for v in cities)
              if c not in st.session_state.geo_cache and c not in ("", "nan", "None")]
    if villes:
        calc_txt = "Computing..." if st.session_state.get("language", "fr") == "en" else "Calcul en cours..."
        bar = st.progress(0, text=calc_txt)
        for i, city in enumerate(villes):
            _, coord = fetch_geo(city)
            if coord:
                st.session_state.geo_cache[city] = coord
            time.sleep(1.1)
            bar.progress((i + 1) / len(villes), text=calc_txt)
        bar.empty()
    return {c: st.session_state.geo_cache[c]
            for c in set(str(v) for v in cities)
            if c in st.session_state.geo_cache}


@st.cache_data(show_spinner=False)
def _ors_distance(lon1, lat1, lon2, lat2):
    for profile in ["driving-hgv", "driving-car"]:
        try:
            r = requests.post(
                f"https://api.openrouteservice.org/v2/directions/{profile}",
                json={"coordinates": [[lon1, lat1], [lon2, lat2]], "instructions": False},
                headers={"Accept": "application/json", "Content-Type": "application/json", "Authorization": ORS_API_KEY},
                timeout=6
            )
            if r.status_code == 200:
                return r.json()["routes"][0]["summary"]["distance"] / 1000.0
        except Exception:
            continue
    return None


def fetch_route(dep, arr, mode, coords, _t=None):
    c1, c2 = coords.get(str(dep)), coords.get(str(arr))
    if not c1 or not c2:
        return (dep, arr, mode), 0.0
    lon1, lat1 = c1
    lon2, lat2 = c2
    dv = calculate_haversine(lon1, lat1, lon2, lat2)
    m = str(mode).lower()
    if any(k in m for k in ["mer","sea","maritime","bateau","port","ferry","conteneur"]):
        return (dep, arr, mode), dv * 1.25
    elif any(k in m for k in ["air","avion","aerien","flight"]):
        return (dep, arr, mode), dv * 1.05
    elif any(k in m for k in ["fer","rail","train","sncf"]):
        return (dep, arr, mode), dv * 1.15
    else:
        d = _ors_distance(lon1, lat1, lon2, lat2)
        return (dep, arr, mode), (d if d and d > 0 else dv * 1.30)


def smart_multimodal_router(df, dep_col, arr_col, mode_col=None):
    import pandas as pd
    coords = geocode_cities_mapbox(pd.concat([df[dep_col], df[arr_col]]).dropna().unique())
    uniq = []
    for _, row in df.iterrows():
        dep  = row[dep_col]
        arr  = row[arr_col]
        mode = str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"
        k = (dep, arr, mode)
        if k not in st.session_state.route_cache and k not in uniq:
            uniq.append(k)
    if uniq:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(fetch_route, r[0], r[1], r[2], coords) for r in uniq]
            for f in concurrent.futures.as_completed(futures):
                key, dist = f.result()
                st.session_state.route_cache[key] = dist
    df["_DIST_CALCULEE"] = [
        st.session_state.route_cache.get(
            (row[dep_col], row[arr_col],
             str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"),
            0.0)
        for _, row in df.iterrows()
    ]
    return df
