# -*- coding: utf-8 -*-
"""
Logiflo - engine/routing.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Geocoding + calcul distances (Nominatim + ORS)
Version 6.1 (mai 2026) — fix coords None/list + mode_force support

V1 : Transport routier uniquement (coherent avec ingester V6.1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import requests
import math
import time
import os
import concurrent.futures

from engine.ingester import nettoyer

# ── Lecture cle ORS depuis env vars (Render) ou st.secrets ──
ORS_API_KEY = os.environ.get("ORS_API_KEY", "")
if not ORS_API_KEY:
    try:
        ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")
    except Exception:
        ORS_API_KEY = ""


def calculate_haversine(lon1, lat1, lon2, lat2):
    """
    Distance a vol d'oiseau en km entre 2 points (lon/lat).
    Protege contre None, listes, et valeurs non-numeriques.
    """
    # ── Guard : convertir en float, gerer None/list ──
    try:
        lon1 = float(lon1) if lon1 is not None else None
        lat1 = float(lat1) if lat1 is not None else None
        lon2 = float(lon2) if lon2 is not None else None
        lat2 = float(lat2) if lat2 is not None else None
    except (TypeError, ValueError):
        return 0.0

    if None in (lon1, lat1, lon2, lat2):
        return 0.0

    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _extract_coords(coord):
    """
    Extrait (lon, lat) depuis un resultat de geocoding.
    Gere les formats : [lon, lat], (lon, lat), [[lon, lat]], None.
    Retourne (lon, lat) ou (None, None).
    """
    if coord is None:
        return None, None
    if isinstance(coord, (list, tuple)):
        if len(coord) == 0:
            return None, None
        # Si le 1er element est aussi une liste → [[lon, lat]]
        if isinstance(coord[0], (list, tuple)):
            if len(coord[0]) >= 2:
                try:
                    return float(coord[0][0]), float(coord[0][1])
                except (TypeError, ValueError):
                    return None, None
            return None, None
        # Format normal [lon, lat]
        if len(coord) >= 2:
            try:
                return float(coord[0]), float(coord[1])
            except (TypeError, ValueError):
                return None, None
    return None, None


def fetch_geo(city, _t=None):
    """Geocode une ville via Nominatim. Retourne (city, [lon, lat]) ou (city, None)."""
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
    """Geocode une liste de villes avec cache session + Nominatim."""
    if "geo_cache" not in st.session_state:
        st.session_state.geo_cache = {}

    villes = [
        c for c in set(str(v) for v in cities)
        if c not in st.session_state.geo_cache and c not in ("", "nan", "None")
    ]

    if villes:
        lang = st.session_state.get("language", "fr")
        calc_txt = "Computing..." if lang == "en" else "Calcul en cours..."
        bar = st.progress(0, text=calc_txt)
        for i, city in enumerate(villes):
            _, coord = fetch_geo(city)
            if coord:
                st.session_state.geo_cache[city] = coord
            time.sleep(1.1)  # Nominatim rate limit
            bar.progress((i + 1) / len(villes), text=calc_txt)
        bar.empty()

    return {
        c: st.session_state.geo_cache[c]
        for c in set(str(v) for v in cities)
        if c in st.session_state.geo_cache
    }


@st.cache_data(show_spinner=False)
def _ors_distance(lon1, lat1, lon2, lat2):
    """Calcule la distance routiere reelle via ORS API."""
    # Guard contre None
    if None in (lon1, lat1, lon2, lat2):
        return None
    try:
        lon1, lat1, lon2, lat2 = float(lon1), float(lat1), float(lon2), float(lat2)
    except (TypeError, ValueError):
        return None

    for profile in ["driving-hgv", "driving-car"]:
        try:
            r = requests.post(
                f"https://api.openrouteservice.org/v2/directions/{profile}",
                json={
                    "coordinates": [[lon1, lat1], [lon2, lat2]],
                    "instructions": False,
                },
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": ORS_API_KEY,
                },
                timeout=6,
            )
            if r.status_code == 200:
                return r.json()["routes"][0]["summary"]["distance"] / 1000.0
        except Exception:
            continue
    return None


def fetch_route(dep, arr, mode, coords, _t=None):
    """
    Calcule la distance entre dep et arr.
    En V1 (routier) : utilise ORS puis fallback haversine * 1.30.
    """
    c1_raw = coords.get(str(dep))
    c2_raw = coords.get(str(arr))

    lon1, lat1 = _extract_coords(c1_raw)
    lon2, lat2 = _extract_coords(c2_raw)

    if None in (lon1, lat1, lon2, lat2):
        return (dep, arr, mode), 0.0

    # Distance a vol d'oiseau
    dv = calculate_haversine(lon1, lat1, lon2, lat2)

    # En V1, tout est traite comme routier
    # On tente ORS pour la distance reelle, sinon haversine * 1.30
    d = _ors_distance(lon1, lat1, lon2, lat2)
    return (dep, arr, mode), (d if d and d > 0 else dv * 1.30)


def smart_multimodal_router(df, dep_col, arr_col, mode_col=None, mode_force=None):
    """
    Geocode les villes depart/arrivee et calcule les distances.

    En V1, mode_force est ignore (tout est routier).
    Le parametre est accepte pour compatibilite avec logiflo_app.py.

    Ajoute la colonne '_DIST_CALCULEE' au DataFrame.
    """
    import pandas as pd

    # Init caches si absent
    if "geo_cache" not in st.session_state:
        st.session_state.geo_cache = {}
    if "route_cache" not in st.session_state:
        st.session_state.route_cache = {}

    # Guard : colonnes absentes
    if dep_col not in df.columns or arr_col not in df.columns:
        df["_DIST_CALCULEE"] = 0.0
        return df

    # Geocoder toutes les villes
    all_cities = pd.concat([df[dep_col], df[arr_col]]).dropna().unique()
    coords = geocode_cities_mapbox(all_cities)

    # Identifier les trajets uniques a calculer
    uniq = []
    for _, row in df.iterrows():
        dep = row[dep_col]
        arr = row[arr_col]
        mode = (
            str(row[mode_col]).lower()
            if mode_col and mode_col in df.columns and pd.notna(row.get(mode_col))
            else "route"
        )
        k = (dep, arr, mode)
        if k not in st.session_state.route_cache and k not in uniq:
            uniq.append(k)

    # Calcul en parallele
    if uniq:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futures = [
                ex.submit(fetch_route, r[0], r[1], r[2], coords)
                for r in uniq
            ]
            for f in concurrent.futures.as_completed(futures):
                try:
                    key, dist = f.result()
                    st.session_state.route_cache[key] = dist
                except Exception:
                    pass

    # Appliquer les distances
    df["_DIST_CALCULEE"] = [
        st.session_state.route_cache.get(
            (
                row[dep_col],
                row[arr_col],
                str(row[mode_col]).lower()
                if mode_col and mode_col in df.columns and pd.notna(row.get(mode_col))
                else "route",
            ),
            0.0,
        )
        for _, row in df.iterrows()
    ]

    return df
