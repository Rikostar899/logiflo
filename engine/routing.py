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


def smart_multimodal_router(df, dep_col, arr_col, mode_col=None, mode_force=None):
    """Calcule la distance pour chaque trajet selon le mode.
    
    - Routier : ORS API en priorité, fallback Haversine si échec
    - Maritime : Haversine x 1.25
    - Aérien : Haversine x 1.05
    - Ferroviaire : Haversine x 1.15
    
    mode_force : si fourni, force ce mode pour TOUS les trajets (override mode_col).
    """
    if dep_col not in df.columns or arr_col not in df.columns:
        return df
    
    df = df.copy()
    df["_DIST_CALCULEE"] = 0.0
    df["_GEO_OK"] = False
    
    # Geocoder toutes les villes uniques en une fois (gain de temps)
    cities_unique = set()
    for col in [dep_col, arr_col]:
        cities_unique.update(df[col].dropna().astype(str).str.strip().unique())
    cities_unique.discard("")
    
    geo_cache = st.session_state.get("geo_cache", {})
    for city in cities_unique:
        if city not in geo_cache:
            coords = fetch_geo(city)
            if coords:
                geo_cache[city] = coords
    st.session_state["geo_cache"] = geo_cache
    
    route_cache = st.session_state.get("route_cache", {})
    
    for idx, row in df.iterrows():
        dep = str(row.get(dep_col, "")).strip()
        arr = str(row.get(arr_col, "")).strip()
        if not dep or not arr or dep == "nan" or arr == "nan":
            continue
        
        # Mode pour ce trajet
        if mode_force:
            mode = mode_force
        elif mode_col and mode_col in df.columns:
            mode_val = str(row.get(mode_col, "")).lower()
            if any(k in mode_val for k in ["mer","ocean","maritime","sea","navire","bateau","conteneur","container"]):
                mode = "maritime"
            elif any(k in mode_val for k in ["air","aerien","avion","fret aerien"]):
                mode = "aerien"
            elif any(k in mode_val for k in ["rail","train","ferroviaire","sncf"]):
                mode = "ferroviaire"
            else:
                mode = "routier"
        else:
            mode = "routier"
        
        # Coordonnées
        coord_dep = geo_cache.get(dep)
        coord_arr = geo_cache.get(arr)
        if not coord_dep or not coord_arr:
            continue
        
        df.at[idx, "_GEO_OK"] = True
        cache_key = f"{mode}__{dep}__{arr}"
        if cache_key in route_cache:
            df.at[idx, "_DIST_CALCULEE"] = route_cache[cache_key]
            continue
        
        # Calcul selon le mode
        dist = 0.0
        if mode == "routier":
            # ORS en priorité
            try:
                ors_dist = _ors_distance(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1])
                if ors_dist and ors_dist > 0:
                    dist = ors_dist
            except Exception:
                pass
            # Fallback Haversine si ORS échoue
            if dist == 0:
                dist = calculate_haversine(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1]) * 1.3
        
        elif mode == "maritime":
            dist = calculate_haversine(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1]) * 1.25
        
        elif mode == "aerien":
            dist = calculate_haversine(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1]) * 1.05
        
        elif mode == "ferroviaire":
            dist = calculate_haversine(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1]) * 1.15
        
        else:
            dist = calculate_haversine(coord_dep[0], coord_dep[1], coord_arr[0], coord_arr[1]) * 1.3
        
        df.at[idx, "_DIST_CALCULEE"] = round(dist, 1)
        route_cache[cache_key] = round(dist, 1)
    
    st.session_state["route_cache"] = route_cache
    return df
