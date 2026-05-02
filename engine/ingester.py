import streamlit as st
import pandas as pd
import numpy as np
import unicodedata
import re
import difflib
import math
import json

SYNONYMES = {
    "reference":["reference","ref","article","code","codearticle","codeproduit","cdarticle","cdart","cdproduit","sku","ean","ean13","upc","gtin","produit","designation","libelle","description","descproduit","descarticle","nom","item","itemcode","itemno","itemref","partnumber","partno","partref","refarticle","refproduit","numeroproduit","matricule","identifiant","id","cable","cablage","matiere","materiel","composant","piece","repere","nomenclature","famille","sousfamille","categorie","productcode","productref","dsg","desig","design","designat","articlecode","articleref","artcode","artref","artno","artnum"],
    "quantite":["quantite","qte","qty","qtstk","qte_stk","qtestk","stock","stk","stockactuel","stockdispo","stockdisponible","stockreel","stockphysique","niveaustock","qtestock","qtedispo","qtedisponible","qtereel","qtephysique","volume","pieces","pcs","units","unit","unites","restant","solde","soldedisponible","encours","inventaire","disponible","existant","existants","present","metre","metres","meter","meters","bobine","bobines","longueur","longueurstock","quantitedisponible","quantitestock","quantiterestante","quantitepresente","nbarticle","nbarticles","nbpieces","nbunites","nb","nbre","nombre","qte_disponible","qt_stk","qtstck","qtstock"],
    "prix_unitaire":["prix","prixunitaire","prixachat","prixderevient","prixmoyen","prixmoyenpondere","pmp","pa","pu","pxu","px_u","price","unitprice","avgprice","cout","coutunitaire","coutachat","coutderevient","coutmoyen","cost","unitcost","avgcost","valeur","valeurunitaire","valeurachat","tarif","tarifunitaire","montantunitaire","achat","prixfournisseur","euro","eur","devise","prixbase","baseachat","priceeuro","priceeur"],
    "conso_an1":["conso2022","conso22","consommation2022","sorties2022","ventes2022","c2022","n3","nminus3","annee2022","a2022","quantite2022","qte2022","cso22","cso2022"],
    "conso_an2":["conso2023","conso23","consommation2023","sorties2023","ventes2023","c2023","n2","nminus2","annee2023","a2023","quantite2023","qte2023","cso23","cso2023"],
    "conso_an3":["conso2024","conso24","consommation2024","sorties2024","ventes2024","c2024","n1","nminus1","annee2024","a2024","quantite2024","qte2024","cso24","cso2024"],
    "conso_an4":["conso2025","conso25","consommation2025","sorties2025","ventes2025","c2025","n0","nactuel","annee2025","a2025","quantite2025","qte2025","cso25","cso2025","sortie2025","consoactuelle","consoencoursannee"],
    "ca":["ca","chiffreaffaires","revenue","revenu","facture","facturation","recette","vente","ventes","montantfacture","montantca","totalca","prixvente","prixdevente","tariflbp","turnover","sales","salesamount","invoiceamount","totalrevenue"],
    "co":["cout","couts","cost","costs","charge","charges","depense","depenses","coutrevient","coutderevient","coutachat","coutexploitation","coutprestation","coutservice","couttransport","frais","fraistransport","fraisexploitation","montantachat","totalcout","totalcouts"],
    "fournisseur":["fournisseur","supplier","vendor","fournisseurs","suppliers","prestataire","prestataires","acheteur","source","origine","partnername","vendorname","suppliername"],
    "date_col":["date","dates","dateop","datetransaction","datemouvement","datecommande","datelivraison","datesortie","datentree","dateachat","datestock","period","periode","mois","month","annee","year","semaine","week","exercice","timestamp","datetime","jour","day"],
    "delai":["delai","delailivraison","leadtime","lt","lead","delaifournisseur","delaiapprovisionnement","delaireapprovisionnement","supplierleadtime","leadtimedays","leadtimeweeks"],
    "categorie":["categorie","categories","category","famille","familles","family","sousfamille","type","types","classe","classes","segment","gamme","rayon","departement","division","group","groupe"],
}

GEO_ALIASES = {
    "marseille":["marseille","fos","fos-sur-mer","fos sur mer","gpmm"],
    "le havre":["le havre","havre","lehavre","gpmh"],
    "dunkerque":["dunkerque","dunkirk","gpmd"],
    "cdg":["cdg","roissy","charles de gaulle","paris-cdg","paris cdg","lfpg"],
    "orly":["orly","paris-orly","paris orly","lfpo"],
    "rotterdam":["rotterdam","eurtm","port of rotterdam"],
    "anvers":["anvers","antwerp","antwerpen"],
    "hambourg":["hambourg","hamburg"],
    "tanger":["tanger","tanger med","tangier","tanger-med"],
    "france":["france","fr","fra"],
    "maroc":["maroc","morocco","ma","mar","casablanca","rabat","agadir","tanger"],
    "algerie":["algerie","algeria","dz","alger","oran"],
    "tunisie":["tunisie","tunisia","tn","tunis","sfax"],
    "espagne":["espagne","spain","es","esp","madrid","barcelone","valence"],
    "italie":["italie","italy","it","ita","rome","milan"],
    "allemagne":["allemagne","germany","de","deu","hambourg","francfort","munich"],
    "belgique":["belgique","belgium","be","bel","anvers","bruxelles"],
}


def nettoyer(t):
    t = str(t).lower()
    t = unicodedata.normalize("NFD", t).encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"[^a-z0-9]", "", t)


def _normalize_geo(text):
    if not text:
        return text
    t = str(text).lower().strip()
    for canonical, aliases in GEO_ALIASES.items():
        if t in aliases:
            return canonical
    return t


def detect_periode(df):
    import datetime as _dt_p
    try:
        from dateutil import parser as _dparser
    except ImportError:
        return {"trimestre":"T2","mois_min":4,"mois_max":6,"annee":2025,"label":"Avr-Juin 2025","saison":"standard","contexte_fr":"","contexte_en":""}

    mois_min, mois_max, annee = None, None, 2025
    date_cols = [c for c in df.columns if any(k in str(c).lower() for k in ["date","mois","month","periode","semaine","week","exercice","timestamp","datetime","jour","day"])]
    for col in date_cols:
        try:
            sample = df[col].dropna().head(50).astype(str)
            parsed = []
            for v in sample:
                try:
                    parsed.append(_dparser.parse(v, dayfirst=True))
                except Exception:
                    pass
            if len(parsed) >= 3:
                mois_vals = [d.month for d in parsed]
                annee = max(d.year for d in parsed)
                mois_min = min(mois_vals)
                mois_max = max(mois_vals)
                break
        except Exception:
            continue

    if mois_min is None:
        today = _dt_p.date.today()
        mois_min = mois_max = today.month
        annee = today.year

    if mois_max <= 3:      trim = "T1"
    elif mois_max <= 6:    trim = "T2"
    elif mois_max <= 9:    trim = "T3"
    else:                  trim = "T4"

    labels_trim = {"T1":"Janv-Mars","T2":"Avr-Juin","T3":"Juil-Sept","T4":"Oct-Dec"}
    saison = "standard"
    if mois_min >= 10 or (mois_max >= 10 and mois_min >= 9):  saison = "pre_fetes"
    elif mois_min >= 6 and mois_max <= 9:                      saison = "ete"
    elif mois_max <= 2:                                        saison = "post_fetes"
    elif mois_min >= 3 and mois_max <= 5:                      saison = "printemps"

    ctx_fr = {
        "pre_fetes": "Periode pre-fetes (oct-dec). Un surstock est normal en anticipation de Noel.",
        "ete": "Periode estivale (jun-sept). Attention aux variations de consommation liees aux conges.",
        "post_fetes": "Periode post-fetes (jan-fev). Les surstocks residuels de Noel sont normaux.",
        "printemps": "Periode printemps (mar-mai). Debut de saison pour certains secteurs.",
        "standard": ""
    }
    ctx_en = {
        "pre_fetes": "Pre-holiday period (Oct-Dec). Overstock is normal in anticipation of Christmas.",
        "ete": "Summer period (Jun-Sep). Watch for consumption variations due to holidays.",
        "post_fetes": "Post-holiday period (Jan-Feb). Residual Christmas overstock is normal.",
        "printemps": "Spring period (Mar-May). Start of season for some sectors.",
        "standard": ""
    }
    return {
        "trimestre": trim, "mois_min": mois_min, "mois_max": mois_max, "annee": annee,
        "label": f"{labels_trim[trim]} {annee}", "saison": saison,
        "contexte_fr": ctx_fr[saison], "contexte_en": ctx_en[saison],
    }


def _levenshtein(s1, s2):
    if len(s1) < len(s2): return _levenshtein(s2, s1)
    if len(s2) == 0: return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j+1]+1, curr[j]+1, prev[j]+(c1 != c2)))
        prev = curr
    return prev[-1]


def _score_nom(propre, std):
    syns = SYNONYMES.get(std, [])
    best = 0
    try:
        from rapidfuzz import fuzz as _rfuzz
        for syn in syns:
            if propre == syn: return 100
            if len(syn) >= 4 and propre.startswith(syn): best = max(best, 95)
            if len(syn) >= 3 and syn in propre: best = max(best, 88)
            if len(propre) >= 3 and propre in syn: best = max(best, 82)
            best = max(best, int(_rfuzz.ratio(propre, syn)))
            best = max(best, int(_rfuzz.partial_ratio(propre, syn) * 0.9))
    except ImportError:
        for syn in syns:
            if propre == syn: return 100
            if len(syn) >= 4 and propre.startswith(syn): best = max(best, 95)
            if len(syn) >= 3 and syn in propre: best = max(best, 88)
            if len(propre) >= 3 and propre in syn: best = max(best, 82)
            r = difflib.SequenceMatcher(None, propre, syn).ratio()
            best = max(best, int(r * 85))
    year_bonus = {"conso_an1":["2022","22"],"conso_an2":["2023","23"],"conso_an3":["2024","24"],"conso_an4":["2025","25"]}
    if std in year_bonus and any(y in propre for y in year_bonus[std]):
        best = max(best, 85)
    return best


def _score_contenu(series, std):
    sample = series.dropna().head(50)
    if len(sample) == 0: return 0
    cleaned = (sample.astype(str)
               .str.replace(r'[€$£\s\xa0%]', '', regex=True)
               .str.replace(',', '.', regex=False)
               .str.replace(r'[^\d.\-]', '', regex=True))
    numeric = pd.to_numeric(cleaned, errors='coerce')
    pct_num = numeric.notna().mean()
    vals = numeric.dropna()
    raw_text = sample.astype(str)
    avg_len = raw_text.str.len().mean()
    pct_alpha = raw_text.str.contains(r'[a-zA-Z]', na=False).mean()
    unique_r = sample.nunique() / len(sample)
    has_dec = (vals % 1 != 0).mean() if len(vals) > 0 else 0
    pct_int = (vals % 1 == 0).mean() if len(vals) > 0 else 0
    pct_pos = (vals >= 0).mean() if len(vals) > 0 else 0
    pct_zero = (vals == 0).mean() if len(vals) > 0 else 0

    if std == "reference":
        score = 0
        if pct_alpha > 0.5: score += 40
        if unique_r > 0.7:  score += 25
        if 3 <= avg_len <= 50: score += 20
        if pct_num < 0.5:   score += 15
        if pct_num > 0.9 and pct_alpha < 0.1: score -= 30
        return max(0, min(score, 100))
    elif std == "quantite":
        if pct_num < 0.6: return 10
        score = 40
        if pct_int > 0.85: score += 30
        elif pct_int > 0.65: score += 15
        if pct_zero > 0.05: score += 8
        if pct_pos > 0.85: score += 8
        if has_dec > 0.55: score -= 20
        if pct_alpha > 0.3: score -= 25
        return max(0, min(score, 100))
    elif std == "prix_unitaire":
        if pct_num < 0.6: return 5
        score = 35
        if has_dec > 0.45: score += 30
        elif has_dec > 0.25: score += 15
        if pct_zero < 0.05: score += 12
        if pct_pos > 0.85: score += 8
        if pct_int > 0.95: score -= 15
        if pct_alpha > 0.3: score -= 25
        return max(0, min(score, 100))
    elif std in ("conso_an1","conso_an2","conso_an3","conso_an4"):
        if pct_num < 0.5: return 5
        score = 30
        if pct_int > 0.80: score += 25
        elif pct_int > 0.60: score += 12
        if pct_zero > 0.15: score += 15
        if pct_pos > 0.5: score += 10
        if has_dec > 0.5: score -= 15
        if pct_alpha > 0.3: score -= 25
        return max(0, min(score, 100))
    return 0


def smart_ingester_stock_ultime(df, client_ai=None):
    df = df.dropna(how='all').copy()
    df = df[df.apply(lambda r: r.astype(str).str.strip().ne('').any(), axis=1)]
    CIBLES = list(SYNONYMES.keys())
    propres = {col: nettoyer(col) for col in df.columns}
    scores = {std: {} for std in CIBLES}
    for col in df.columns:
        propre = propres[col]
        for std in CIBLES:
            sn = _score_nom(propre, std)
            sc = _score_contenu(df[col], std)
            if sn >= 70:   sf = int(sn*0.65 + sc*0.35)
            elif sn >= 45: sf = int(sn*0.55 + sc*0.45)
            else:          sf = int(sn*0.25 + sc*0.75)
            scores[std][col] = min(sf, 100)

    for col in df.columns:
        vals = pd.to_numeric(df[col].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce').dropna()
        if len(vals) > 5:
            if (vals % 1 == 0).mean() > 0.9 and vals.median() > 10:
                scores["quantite"][col]     = min(scores["quantite"][col] + 10, 100)
                scores["prix_unitaire"][col] = max(scores["prix_unitaire"][col] - 8, 0)
            if (vals % 1 != 0).mean() > 0.5 and vals.median() < 1000:
                scores["prix_unitaire"][col] = min(scores["prix_unitaire"][col] + 10, 100)
                scores["quantite"][col]      = max(scores["quantite"][col] - 8, 0)

    trouvees = {}
    utilisees = set()
    ORDRE  = ["reference","quantite","prix_unitaire","conso_an4","conso_an3","conso_an2","conso_an1"]
    SEUILS = {"reference":35,"quantite":55,"prix_unitaire":55,"conso_an4":55,"conso_an3":55,"conso_an2":55,"conso_an1":55}

    for std in ORDRE:
        seuil = SEUILS.get(std, 55)
        candidats = [(col, scores[std][col]) for col in scores[std] if col not in trouvees and scores[std][col] >= seuil]
        if not candidats: continue
        nom_forts = [(col, sc) for col, sc in candidats if _score_nom(propres[col], std) >= 70]
        gagnant = (max(nom_forts, key=lambda x: _score_nom(propres[x[0]], std))[0]
                   if nom_forts else max(candidats, key=lambda x: x[1])[0])
        trouvees[gagnant] = std
        utilisees.add(std)

    cols = list(df.columns)
    if "reference" not in utilisees:
        for c in cols:
            if c not in trouvees:
                s = df[c].dropna().head(20)
                if s.astype(str).str.contains(r'[a-zA-Z]', na=False).mean() > 0.3 or s.nunique()/max(len(s),1) > 0.6:
                    trouvees[c] = "reference"; utilisees.add("reference"); break
    if "quantite" not in utilisees:
        for c in cols:
            if c not in trouvees:
                num = pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce')
                if num.notna().mean() > 0.6 and (num.dropna() % 1 == 0).mean() > 0.6:
                    trouvees[c] = "quantite"; utilisees.add("quantite"); break

    critiques = [s for s in ["reference","quantite"] if s not in utilisees]
    if critiques and client_ai:
        titres = list(df.columns)
        sample_data = df.head(5).astype(str).to_dict(orient='list')
        prompt = f"""Logistics file. Columns: {titres}\nData (5 rows): {json.dumps(sample_data, ensure_ascii=False)[:3000]}\nMissing concepts: {critiques}\nReply ONLY JSON: {{"concept": "exact_title"}} or null. Choose from: {titres}"""
        try:
            r = client_ai.chat.completions.create(model="gpt-4o-mini",
                messages=[{"role":"system","content":prompt}], temperature=0.0)
            raw = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
            gpt_map = json.loads(raw)
            for std, col in gpt_map.items():
                if std in critiques and col in df.columns and col not in trouvees:
                    trouvees[col] = std; utilisees.add(std)
        except Exception:
            pass

    df = df.rename(columns=trouvees)
    manq = [c for c in ["reference","quantite"] if c not in df.columns]
    if manq:
        return None, f"Colonnes introuvables : {', '.join(manq)}.\nColonnes dans votre fichier : {list(df.columns[:10])}"

    df["quantite"] = pd.to_numeric(df["quantite"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce')
    df = df.dropna(subset=["quantite"]).copy()
    df = df[df["reference"].astype(str).str.strip().ne('')]
    df = df[~df["reference"].astype(str).str.lower().isin(['nan','none',''])]

    if "prix_unitaire" not in df.columns:
        df["prix_unitaire"] = 0.0; df["_sans_prix"] = True
    else:
        df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce').fillna(0)
        df["_sans_prix"] = (df["prix_unitaire"] == 0).all()

    has_conso = False; conso_cols = []
    for c in ["conso_an1","conso_an2","conso_an3","conso_an4"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'), errors='coerce').fillna(0)
            conso_cols.append(c); has_conso = True

    df["_has_conso"] = has_conso
    df["_conso_moy"] = df[conso_cols].mean(axis=1) if has_conso else 0.0
    return df.copy(), "Succes"


def auto_map_columns_with_ai(df, client_ai=None):
    try:
        from openai import OpenAI
        import os
        if client_ai is None:
            _key = os.environ.get("OPENAI_API_KEY", "") or st.secrets.get("OPENAI_API_KEY", "")
            client_ai = OpenAI(api_key=_key)
    except Exception:
        return {"client": df.columns[0], "ca": df.columns[1] if len(df.columns) > 1 else None, "co": None}

    titres = list(df.columns)
    profil = {col: {"exemples": list(df[col].dropna().astype(str).unique()[:5])} for col in titres}
    prompt = f"""Titres: {titres}\nDonnees: {json.dumps(profil, ensure_ascii=False)}\nAssocie a un titre EXACT. Si absent: null.\nConcepts: "client","ca","co","dep","arr","dist","poids","mode".\nJSON uniquement."""
    try:
        r = client_ai.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt}], temperature=0.0)
        raw = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        return {k: v for k, v in json.loads(raw).items() if v in titres}
    except Exception:
        return {"client": titres[0], "ca": titres[1] if len(titres) > 1 else None, "co": None}


def detect_transport_mode(df, dep_col=None, arr_col=None, mode_col=None):
    PORTS = ["havre","marseille","dunkerque","bordeaux","hamburg","rotterdam","antwerp","anvers","barcelona","barcelone","genova","genes","tanger","tangermed","casablanca","dakar","abidjan","shanghai","ningbo","shenzhen","hongkong","singapore","singapour","dubai","jeddah","mumbai"]
    AIRPORT_CODES = {"cdg","ory","lyo","mrs","nce","tls","bod","jfk","lax","lhr","fra","muc","ams","bru","mad","fco","mxp","dxb","auh","doh","bom","del","hkg","nrt","sin"}
    ROAD_CITIES = {"paris","lyon","toulouse","bordeaux","lille","marseille","nantes","strasbourg","rennes","nice","grenoble","montpellier","tours","dijon","metz","nancy","reims","rouen","amiens","clermont","limoges","bruxelles","amsterdam","berlin","munich","madrid","rome","milan","geneve","zurich","rotterdam","hamburg"}
    KW_AIR  = ["aerien","air freight","airfreight","awb","air waybill","fret aerien","airline cargo","avion"]
    KW_SEA  = ["maritime","seafreight","sea freight","ocean freight","bateau","navire","conteneur","container","teu","fcl","lcl","armateur","roro","reefer","vrac","bulk","mer","ocean"]
    KW_RAIL = ["ferroviaire","rail","train","sncf","wagon","fret ferroviaire","railway"]
    KW_ROAD = ["routier","road","camion","truck","ftl","ltl","vl","tir","messagerie","groupage","express","fret routier","road freight","haulage","trucking"]

    scores = {"aerien":0,"maritime":0,"ferroviaire":0,"routier":0}

    if mode_col and mode_col in df.columns:
        for v in df[mode_col].dropna().astype(str).str.lower():
            for kw in KW_AIR:
                if kw in v: scores["aerien"] += 3
            for kw in KW_SEA:
                if kw in v: scores["maritime"] += 3
            for kw in KW_RAIL:
                if kw in v: scores["ferroviaire"] += 3
            for kw in KW_ROAD:
                if kw in v: scores["routier"] += 3

    for col in [dep_col, arr_col]:
        if not col or col not in df.columns: continue
        for v in df[col].dropna().astype(str):
            raw_tokens = re.split(r'[\s\-/,]+', v.strip())
            tokens_clean = [nettoyer(t) for t in raw_tokens if t.strip()]
            for tok in tokens_clean:
                if tok in AIRPORT_CODES: scores["aerien"] += 2
                if any(p in tok or tok in p for p in PORTS if len(p) >= 5): scores["maritime"] += 1
                if tok in ROAD_CITIES or any(tok in rc for rc in ROAD_CITIES if len(rc) >= 5): scores["routier"] += 1

    hdrs = [nettoyer(c) for c in df.columns]
    if any("awb" in h for h in hdrs):              scores["aerien"] += 6
    if any("airwaybill" in h for h in hdrs):        scores["aerien"] += 6
    if any("billoflading" in h or "bl"==h for h in hdrs): scores["maritime"] += 6
    if any("teu" in h for h in hdrs):              scores["maritime"] += 5
    if any("conteneur" in h or "container" in h for h in hdrs): scores["maritime"] += 5
    if any("distancekm" in h or "km" in h for h in hdrs): scores["routier"] += 4
    if any("wagon" in h or "sncf" in h for h in hdrs): scores["ferroviaire"] += 6

    total = sum(scores.values())
    dominant = max(scores, key=scores.get)
    top_val = scores[dominant]

    # SCORE INSUFFISANT → on retourne "unknown" pour demander à l'user
    SCORE_MIN = 4
    if total < 2 or top_val < SCORE_MIN:
        return "unknown", "?", "?"

    # ÉGALITÉ entre 2 modes ou plus → unknown
    rivals = [k for k, v in scores.items() if v == top_val and k != dominant]
    if rivals:
        return "unknown", "?", "?"

    lang = st.session_state.get("language", "fr")
    labels_fr = {"aerien":("✈️ Mode Aerien detecte","✈️"),"maritime":("⚓ Mode Maritime detecte","⚓"),"ferroviaire":("🚂 Mode Ferroviaire detecte","🚂"),"routier":("🚛 Mode Routier detecte","🚛")}
    labels_en = {"aerien":("✈️ Air mode detected","✈️"),"maritime":("⚓ Maritime mode detected","⚓"),"ferroviaire":("🚂 Rail mode detected","🚂"),"routier":("🚛 Road mode detected","🚛")}
    labels = labels_en if lang == "en" else labels_fr
    label, emoji = labels[dominant]
    return dominant, label, emoji


def super_clean(val):
    if pd.isna(val): return 0.0
    try:
        return float(str(val).replace('€','').replace('$','').replace('EUR','').replace(' ','').replace('\xa0','').replace(',','.'))
    except Exception:
        return 0.0
