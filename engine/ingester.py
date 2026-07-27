# -*- coding: utf-8 -*-
"""
Logiflo - engine/ingester.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Smart Ingester unifie Stock + Transport
Version 8.4 (juillet 2026) — has_conso exige du volume reel + trace du mapping

MARQUEUR DE VERSION (pour verifier le deploiement) :
LOGIFLO_INGESTER_VERSION = "8.3-dtype-guard"
Si ce marqueur n'apparait pas dans le fichier deploye sur le serveur,
c'est que l'ancienne version est encore active.
"""
LOGIFLO_INGESTER_VERSION = "8.3-dtype-guard"

import streamlit as st
import pandas as pd
import numpy as np
import unicodedata
import re
import difflib
import json
import os


# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION VERSION
# ════════════════════════════════════════════════════════════════════════════
# Si True : detection multi-mode + UI (V2)
# Si False : tout est traite comme du routier (V1)
ENABLE_MULTIMODAL_DETECTION = False


# ════════════════════════════════════════════════════════════════════════════
# DICTIONNAIRE DES SYNONYMES (etendu pour PME terrain + ERP/TMS varies)
# ════════════════════════════════════════════════════════════════════════════
SYNONYMES = {
    # ─── STOCK ────────────────────────────────────────────────────────────
    "reference": [
        # Generiques
        "reference", "ref", "article", "code", "sku", "ean", "ean13", "upc",
        "gtin", "produit", "designation", "libelle", "description", "nom",
        "item", "id", "identifiant", "matricule",
        # E-commerce / marketplace
        "title", "productname", "itemname", "itemtitle", "productdescription",
        "listingname", "listingtitle", "name", "product",
        # Variantes courantes
        "codearticle", "codeproduit", "cdarticle", "cdart", "cdproduit",
        "descproduit", "descarticle", "itemcode", "itemno", "itemref",
        "partnumber", "partno", "partref", "refarticle", "refproduit",
        "numeroproduit", "productcode", "productref",
        # Abreviations
        "dsg", "desig", "design", "designat", "articlecode", "articleref",
        "artcode", "artref", "artno", "artnum",
        # Specifiques PME
        "cable", "cablage", "matiere", "materiel", "composant", "piece",
        "repere", "nomenclature",
        # Transport (peut servir de reference)
        "bookingid", "bookingnumber", "tripid", "shipmentid",
        "trackingnumber", "noreference", "nocommande", "noFacture", "nofacture",
    ],
    "quantite": [
        # Generiques
        "quantite", "qte", "qty", "stock", "stk", "volume", "pieces", "pcs",
        "units", "unit", "unites", "nombre", "nb", "nbre",
        # Variantes stock en main
        "qtstk", "qte_stk", "qtestk", "stockactuel", "stockdispo",
        "stockdisponible", "stockreel", "stockphysique", "niveaustock",
        "qtestock", "qtedispo", "qtedisponible", "qtereel", "qtephysique",
        # Etat stock
        "restant", "solde", "soldedisponible", "encours", "inventaire",
        "disponible", "existant", "existants", "present",
        # EN: stock on hand (e-commerce + ERP)
        "available", "availableqty", "qtyavailable", "quantityavailable",
        "onhand", "qtyonhand", "stockonhand", "instock", "currentstock",
        "remainingstock", "remaining", "stocktotal", "totalstock",
        "inventoryqty", "inventoryquantity", "inventory",
        # Unites specifiques
        "metre", "metres", "meter", "meters", "bobine", "bobines", "longueur",
        "longueurstock",
        # Variations longues
        "quantitedisponible", "quantitestock", "quantiterestante",
        "quantitepresente", "nbarticle", "nbarticles", "nbpieces", "nbunites",
        # Abreviations courantes PME
        "qte_disponible", "qt_stk", "qtstck", "qtstock", "stckphys", "stockphys",
    ],
    "prix_unitaire": [
        # Generiques
        "prix", "prixunitaire", "prixachat", "prixderevient", "prixmoyen",
        "price", "unitprice", "avgprice", "tarif", "tarifunitaire",
        "valeur", "valeurunitaire", "valeurachat",
        # Abreviations
        "pmp", "pa", "pu", "pxu", "px_u",
        # Cout (peut etre prix unitaire d'achat)
        "coutunitaire", "coutmoyen", "unitcost", "avgcost",
        # E-commerce / marketplace
        "listprice", "sellingprice", "retailprice", "saleprice",
        "currentprice", "buyitnowprice", "itemprice", "offerprice",
        "prixvente", "prixpublic", "pvttc", "pvht",
        # Variantes
        "montantunitaire", "achat", "prixfournisseur", "prixbase",
        "baseachat", "priceeuro", "priceeur",
        "prixmoyenpondere", "productprice", "mrp",
    ],
    "conso_an1": ["conso2022", "conso22", "consommation2022", "sorties2022",
                   "ventes2022", "c2022", "nminus3", "annee2022",
                   "quantite2022", "qte2022", "cso22", "cso2022"],
    "conso_an2": ["conso2023", "conso23", "consommation2023", "sorties2023",
                   "ventes2023", "c2023", "nminus2", "annee2023",
                   "quantite2023", "qte2023", "cso23", "cso2023"],
    "conso_an3": ["conso2024", "conso24", "consommation2024", "sorties2024",
                   "ventes2024", "c2024", "nminus1", "annee2024",
                   "quantite2024", "qte2024", "cso24", "cso2024"],
    "conso_an4": ["conso2025", "conso25", "consommation2025", "sorties2025",
                   "ventes2025", "c2025", "nactuel", "annee2025",
                   "quantite2025", "qte2025", "cso25", "cso2025",
                   "sortie2025", "consoactuelle", "consoencoursannee",
                   # EN: sold / consumption (e-commerce, ERP, WMS)
                   "sold", "itemssold", "unitssold", "totalsold", "qtysold",
                   "quantitysold", "salesqty", "salescount", "numbersold",
                   "totalitemssold", "totalunitssold",
                   # FR: vendu / consomme
                   "vendu", "vendus", "nbvendu", "nbvendus", "qtevendue",
                   "quantitevendue", "totalvendu",
                   # Generique: consommation / sorties / demande
                   "consumption", "usage", "demand", "output", "outbound",
                   "consommation", "sorties", "sortie",
                   # Hebdo / mensuel (sera annualise par logiflo_app)
                   "venteshebdo", "venteshebdomadaires", "weeklysales",
                   "ventesmensuelles", "monthlysales", "saleshebdo",
                   "ventessemaine", "salesperweek", "salespermonth",
                   "consohebdo", "consomensuelle",
                   ],

    # ─── TRANSPORT ROUTIER (V1) ───────────────────────────────────────────
    "ca": [
        # Generiques FR
        "ca", "chiffreaffaires", "chiffre_affaires", "facture", "facturation",
        "recette", "vente", "ventes", "tarif", "tariff",
        # Generiques EN
        "revenue", "revenu", "turnover", "sales", "salesamount",
        "invoiceamount", "totalrevenue", "billed", "billedamount",
        # Variantes FR
        "montantfacture", "montantca", "totalca", "prixvente", "prixdevente",
        "prixfacture", "totalfacture", "totalvente",
        # Specifiques transport
        "freight", "freightcharge", "freightrevenue", "yield", "tariflbp",
        "prixtransport", "tarifclient", "produittransport",
        # Variantes comptables PME
        "ht", "totalht", "montantht", "facture_ht", "ventes_ht",
    ],
    "co": [
        # Generiques FR
        "cout", "couts", "charge", "charges", "depense", "depenses",
        "frais", "achat", "achats",
        # Generiques EN
        "cost", "costs", "expense", "expenses",
        # Variantes
        "coutrevient", "coutderevient", "coutachat", "coutexploitation",
        "coutprestation", "coutservice", "couttransport",
        "fraistransport", "fraisexploitation", "montantachat",
        "totalcout", "totalcouts", "totalcost", "operatingcost", "transportcost",
        # Specifiques transport
        "fuel", "carburant", "peages", "tolls", "salairechauffeur",
        "fraischauffeur", "fraisroutiers",
    ],
    "client": [
        # Generiques
        "client", "clients", "customer", "customers", "customername",
        "customernamecode", "customer_name_code",
        "compte", "account", "company", "societe", "raison", "raisonsociale",
        # Specifiques transport
        "shipper", "consignee", "destinataire", "expediteur", "donneurordre",
        "donneurdordre", "facturea", "factureapayer",
    ],
    "dep": [
        "depart", "origine", "origin", "originlocation", "origin_location",
        "from", "depuis", "expedition", "shipfrom", "pickup", "ramassage",
        "lieuchargement", "villedepart", "depcity",
        "originport", "originairport", "startlocation", "pickuplocation",
        "lieudepart", "departville", "departcity",
    ],
    "arr": [
        "arrivee", "destination", "destinationlocation", "destination_location",
        "to", "vers", "shipto", "delivery", "deliverylocation",
        "lieulivraison", "villearrivee", "arrcity", "arrivalcity",
        "destport", "destairport", "endlocation", "deliverylocation",
        "lieuarrivee", "arrivalville",
    ],
    "dist": [
        "distance", "distancekm", "km", "kilometre", "kilometres", "miles",
        "mileage", "transportationdistance", "transportationdistanceinkm",
        "transportation_distance_in_km",
        "longueurtrajet", "tripdistance", "distancetotale",
        "kmtotal", "kmparcourus", "totalkm",
    ],
    "poids": [
        "poids", "weight", "kg", "kilo", "kilos", "tonnage", "tonnes", "tons",
        "grossweight", "netweight", "chargeweight", "loadweight",
        "poidsbrut", "poidsnet", "tonnage_t",
    ],
    "mode": [
        "mode", "modetransport", "transportmode", "typetransport",
        "shipmentmode", "service", "serviceniveau", "servicelevel",
        "vehicletype", "vehicle_type", "typevehicule",
    ],

    # ─── COMMUNS ──────────────────────────────────────────────────────────
    "fournisseur": [
        "fournisseur", "supplier", "vendor", "fournisseurs", "suppliers",
        # E-commerce / marketplace
        "brand", "marque", "manufacturer", "fabricant", "maker",
        "brandname", "nommarque", "marqueproduit",
        "prestataire", "prestataires", "acheteur", "source", "origine",
        "partnername", "vendorname", "suppliername",
        "suppliernamecode", "supplier_name_code",
        "transporteur", "carrier", "shippingcompany", "soustraitant",
    ],
    "date_col": [
        "date", "dates", "dateop", "datetransaction", "datemouvement",
        "datecommande", "datelivraison", "datesortie", "datentree", "dateachat",
        "datestock", "period", "periode", "mois", "month", "annee", "year",
        "semaine", "week", "exercice", "timestamp", "datetime", "jour", "day",
        "tripstartdate", "trip_start_date", "shipdate", "shippingdate",
        "datefacture", "datefacturation",
    ],
    "date_peremption": [
        # FR
        "dlc", "ddm", "dluo", "peremption", "datepremption", "dateperemption",
        "datelimite", "datelimiteconso", "datelimiteconsommation",
        "datedureeminimale", "datedureevie", "finvalidite", "dateexpiration",
        "expirationle", "perimele", "aconsommeravant", "aconsommer",
        # EN
        "expiry", "expirydate", "expirationdate", "expdate", "exp",
        "bestbefore", "bestbeforedate", "usebydate", "useby",
        "sellby", "sellbydate", "shelflife", "validuntil", "validthrough",
        # Abreviations courtes courantes
        "exp_date", "exp_dt", "dlc_date", "peremp",
    ],
    "delai": [
        "delai", "delailivraison", "leadtime", "lt", "lead", "delaifournisseur",
        "delaiapprovisionnement", "delaireapprovisionnement",
        "supplierleadtime", "leadtimedays", "leadtimeweeks",
    ],
    "categorie": [
        "categorie", "categories", "category", "famille", "familles", "family",
        "sousfamille", "type", "types", "classe", "classes", "segment", "gamme",
        "rayon", "departement", "division", "group", "groupe",
        "materialshipped", "material_shipped", "marchandise", "goods", "cargo",
    ],
}


# ════════════════════════════════════════════════════════════════════════════
# ALIAS GEOGRAPHIQUES (pour future detection ports/aeroports V2)
# ════════════════════════════════════════════════════════════════════════════
GEO_ALIASES = {
    "marseille": ["marseille", "fos", "fos-sur-mer", "fos sur mer", "gpmm"],
    "le havre": ["le havre", "havre", "lehavre", "gpmh"],
    "dunkerque": ["dunkerque", "dunkirk", "gpmd"],
    "cdg": ["cdg", "roissy", "charles de gaulle", "paris-cdg", "paris cdg", "lfpg"],
    "orly": ["orly", "paris-orly", "paris orly", "lfpo"],
    "rotterdam": ["rotterdam", "eurtm", "port of rotterdam"],
    "anvers": ["anvers", "antwerp", "antwerpen"],
    "hambourg": ["hambourg", "hamburg"],
    "tanger": ["tanger", "tanger med", "tangier", "tanger-med"],
    "france": ["france", "fr", "fra"],
    "maroc": ["maroc", "morocco", "ma", "mar", "casablanca", "rabat", "agadir", "tanger"],
    "algerie": ["algerie", "algeria", "dz", "alger", "oran"],
    "tunisie": ["tunisie", "tunisia", "tn", "tunis", "sfax"],
    "espagne": ["espagne", "spain", "es", "esp", "madrid", "barcelone", "valence"],
    "italie": ["italie", "italy", "it", "ita", "rome", "milan"],
    "allemagne": ["allemagne", "germany", "de", "deu", "hambourg", "francfort", "munich"],
    "belgique": ["belgique", "belgium", "be", "bel", "anvers", "bruxelles"],
}


# ════════════════════════════════════════════════════════════════════════════
# UTILITAIRES BAS NIVEAU
# ════════════════════════════════════════════════════════════════════════════
def nettoyer(t):
    """Normalise un texte : lowercase, sans accents, sans caracteres speciaux."""
    t = str(t).lower()
    t = unicodedata.normalize("NFD", t).encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"[^a-z0-9]", "", t)


def super_clean(val):
    """Convertit une valeur quelconque en float robuste."""
    if pd.isna(val):
        return 0.0
    try:
        return float(
            str(val)
            .replace('€', '').replace('$', '').replace('EUR', '')
            .replace(' ', '').replace('\xa0', '')
            .replace(',', '.')
        )
    except Exception:
        return 0.0


def _normalize_geo(text):
    """Normalise un nom de ville/pays vers sa forme canonique."""
    if not text:
        return text
    t = str(text).lower().strip()
    for canonical, aliases in GEO_ALIASES.items():
        if t in aliases:
            return canonical
    return t


def _safe_numeric(series):
    """Convertit une serie pandas en numerique en preservant les NaN."""
    return pd.to_numeric(
        series.astype(str)
            .str.replace(r'[^\d.,-]', '', regex=True)
            .str.replace(',', '.'),
        errors='coerce'
    )


# ════════════════════════════════════════════════════════════════════════════
# MODE DEBUG (logs pour diagnostiquer le mapping)
# ════════════════════════════════════════════════════════════════════════════
def _debug_log(msg, level="info"):
    """Stocke un log de debug si debug_mode est active."""
    if not st.session_state.get("debug_mode", False):
        return
    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = []
    icon = {"info": "ℹ", "ok": "✓", "warn": "⚠", "err": "✗"}.get(level, "•")
    st.session_state.debug_logs.append(f"{icon} {msg}")


def render_debug_logs():
    """A appeler depuis logiflo_app.py pour afficher les logs."""
    if not st.session_state.get("debug_mode", False):
        return
    logs = st.session_state.get("debug_logs", [])
    if not logs:
        return
    with st.expander("🔍 Debug mapping (mode debug active)", expanded=False):
        for log in logs:
            st.text(log)
    st.session_state.debug_logs = []


# ════════════════════════════════════════════════════════════════════════════
# DETECTION PERIODE / SAISON
# ════════════════════════════════════════════════════════════════════════════
def _periode_default():
    return {
        "trimestre": "T2", "mois_min": 4, "mois_max": 6, "annee": 2025,
        "label": "Avr-Juin 2025", "saison": "standard",
        "contexte_fr": "", "contexte_en": "",
    }


def detect_periode(df):
    """Detecte la periode (trimestre, mois, annee) couverte par le fichier."""
    import datetime as _dt_p
    try:
        from dateutil import parser as _dparser
    except ImportError:
        return _periode_default()

    mois_min, mois_max, annee = None, None, 2025
    date_cols = [
        c for c in df.columns
        if any(k in str(c).lower() for k in [
            "date", "mois", "month", "periode", "semaine", "week",
            "exercice", "timestamp", "datetime", "jour", "day"
        ])
    ]

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

    if mois_max <= 3:    trim = "T1"
    elif mois_max <= 6:  trim = "T2"
    elif mois_max <= 9:  trim = "T3"
    else:                trim = "T4"

    labels_trim = {"T1": "Janv-Mars", "T2": "Avr-Juin",
                   "T3": "Juil-Sept", "T4": "Oct-Dec"}

    saison = "standard"
    if mois_min >= 10 or (mois_max >= 10 and mois_min >= 9):
        saison = "pre_fetes"
    elif mois_min >= 6 and mois_max <= 9:
        saison = "ete"
    elif mois_max <= 2:
        saison = "post_fetes"
    elif mois_min >= 3 and mois_max <= 5:
        saison = "printemps"

    ctx_fr = {
        "pre_fetes": "Periode pre-fetes (oct-dec). Un surstock est normal en anticipation de Noel.",
        "ete": "Periode estivale (jun-sept). Attention aux variations de consommation liees aux conges.",
        "post_fetes": "Periode post-fetes (jan-fev). Les surstocks residuels de Noel sont normaux.",
        "printemps": "Periode printemps (mar-mai). Debut de saison pour certains secteurs.",
        "standard": "",
    }
    ctx_en = {
        "pre_fetes": "Pre-holiday period (Oct-Dec). Overstock is normal in anticipation of Christmas.",
        "ete": "Summer period (Jun-Sep). Watch for consumption variations due to holidays.",
        "post_fetes": "Post-holiday period (Jan-Feb). Residual Christmas overstock is normal.",
        "printemps": "Spring period (Mar-May). Start of season for some sectors.",
        "standard": "",
    }

    return {
        "trimestre": trim,
        "mois_min": mois_min,
        "mois_max": mois_max,
        "annee": annee,
        "label": f"{labels_trim[trim]} {annee}",
        "saison": saison,
        "contexte_fr": ctx_fr[saison],
        "contexte_en": ctx_en[saison],
    }


# ════════════════════════════════════════════════════════════════════════════
# CLIENTS IA (OpenAI + Gemini fallback)
# ════════════════════════════════════════════════════════════════════════════
def _get_openai_client():
    """Initialise un client OpenAI depuis env vars (Render) ou st.secrets."""
    try:
        from openai import OpenAI
    except ImportError:
        _debug_log("OpenAI lib non installee", "err")
        return None

    key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        try:
            key = st.secrets.get("OPENAI_API_KEY", "")
        except Exception:
            key = ""
    if not key:
        _debug_log("OPENAI_API_KEY introuvable", "err")
        return None

    try:
        return OpenAI(api_key=key)
    except Exception as e:
        _debug_log(f"OpenAI init failed: {e}", "err")
        return None


def _get_gemini_model():
    """Initialise le modele Gemini en fallback."""
    try:
        import google.generativeai as genai
    except ImportError:
        _debug_log("google-generativeai non installe", "err")
        return None

    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        try:
            key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            key = ""
    if not key:
        _debug_log("GEMINI_API_KEY introuvable", "err")
        return None

    try:
        genai.configure(api_key=key)
        return genai.GenerativeModel("gemini-1.5-flash")
    except Exception as e:
        _debug_log(f"Gemini init failed: {e}", "err")
        return None


def _ai_call_with_fallback(prompt, client_ai=None, timeout=15):
    """Appelle OpenAI puis Gemini en fallback. Retourne str ou None."""
    # Tentative OpenAI
    if client_ai is None:
        client_ai = _get_openai_client()

    if client_ai is not None:
        try:
            r = client_ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": prompt}],
                temperature=0.0,
                timeout=timeout,
            )
            response = r.choices[0].message.content.strip()
            _debug_log("OpenAI repond OK", "ok")
            return response
        except Exception as e:
            _debug_log(f"OpenAI failed: {str(e)[:100]}", "warn")

    # Fallback Gemini
    gemini_model = _get_gemini_model()
    if gemini_model is not None:
        try:
            r = gemini_model.generate_content(prompt)
            response = r.text.strip() if r and r.text else None
            if response:
                _debug_log("Gemini fallback OK", "ok")
                return response
        except Exception as e:
            _debug_log(f"Gemini failed: {str(e)[:100]}", "err")

    _debug_log("OpenAI ET Gemini ont echoue", "err")
    return None


def _parse_json_response(raw_text):
    """Parse une reponse texte qui peut contenir du JSON entoure de markdown."""
    if not raw_text:
        return {}
    cleaned = raw_text.strip().replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except Exception:
        match = re.search(r'\{[^{}]*\}', cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
        return {}


# ════════════════════════════════════════════════════════════════════════════
# SCORING : NOM + CONTENU
# ════════════════════════════════════════════════════════════════════════════
def _score_nom(propre, std):
    """Score de similarite entre un nom de colonne nettoye et un concept (0-100)."""
    syns = SYNONYMES.get(std, [])
    best = 0

    try:
        from rapidfuzz import fuzz as _rfuzz
        for syn in syns:
            if propre == syn:
                return 100
            if len(syn) >= 4 and propre.startswith(syn):
                best = max(best, 95)
            if len(syn) >= 3 and syn in propre:
                best = max(best, 88)
            if len(propre) >= 3 and propre in syn:
                best = max(best, 82)
            best = max(best, int(_rfuzz.ratio(propre, syn)))
            best = max(best, int(_rfuzz.partial_ratio(propre, syn) * 0.9))
    except ImportError:
        for syn in syns:
            if propre == syn:
                return 100
            if len(syn) >= 4 and propre.startswith(syn):
                best = max(best, 95)
            if len(syn) >= 3 and syn in propre:
                best = max(best, 88)
            if len(propre) >= 3 and propre in syn:
                best = max(best, 82)
            r = difflib.SequenceMatcher(None, propre, syn).ratio()
            best = max(best, int(r * 85))

    year_bonus = {
        "conso_an1": ["2022", "22"],
        "conso_an2": ["2023", "23"],
        "conso_an3": ["2024", "24"],
        "conso_an4": ["2025", "25"],
    }
    if std in year_bonus and any(y in propre for y in year_bonus[std]):
        best = max(best, 85)

    return best


def _score_contenu(series, std):
    """Score base sur le contenu de la colonne (0-100)."""
    sample = series.dropna().head(50)
    if len(sample) == 0:
        return 0

    # ── DATE DE PEREMPTION : score base sur la reconnaissance de format date ──
    if std == "date_peremption":
        raw = sample.astype(str)
        # Une vraie date contient des separateurs (-, /, .) ou des lettres (mois).
        # Une colonne numerique pure (poids, code) se parse en timestamp 1970 → faux positif.
        has_date_sep = raw.str.contains(r'[-/.]|[a-zA-Z]', na=False).mean()
        if has_date_sep < 0.5:
            return 0
        parsed = pd.to_datetime(sample, errors='coerce', format='ISO8601')
        if parsed.isna().mean() > 0.5:
            parsed = pd.to_datetime(sample, errors='coerce', dayfirst=True)
        pct_valid_date = parsed.notna().mean()
        if pct_valid_date < 0.5:
            return 0
        score = 40 + int(pct_valid_date * 40)
        # Bonus si les dates sont plausibles pour une peremption (pas 100% dans le passe lointain)
        try:
            future_ratio = (parsed.dropna() >= pd.Timestamp.now() - pd.Timedelta(days=730)).mean()
            if future_ratio > 0.3:
                score += 10
        except Exception:
            pass
        return max(0, min(score, 100))

    cleaned = (
        sample.astype(str)
        .str.replace(r'[€$£\s\xa0%]', '', regex=True)
        .str.replace(',', '.', regex=False)
        .str.replace(r'[^\d.\-]', '', regex=True)
    )
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
    median_val = vals.median() if len(vals) > 0 else 0

    # ── REFERENCE / CLIENT (texte alpha-numerique unique) ────────────────
    if std in ("reference", "client"):
        score = 0
        if pct_alpha > 0.5: score += 40
        if unique_r > 0.7:  score += 25
        if 3 <= avg_len <= 50: score += 20
        if pct_num < 0.5:   score += 15
        if pct_num > 0.9 and pct_alpha < 0.1: score -= 30
        return max(0, min(score, 100))

    # ── QUANTITE (entiers positifs) ──────────────────────────────────────
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

    # ── PRIX UNITAIRE (decimaux modestes) ────────────────────────────────
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

    # ── CONSOMMATION ANNUELLE ────────────────────────────────────────────
    elif std in ("conso_an1", "conso_an2", "conso_an3", "conso_an4"):
        if pct_num < 0.5: return 5
        score = 30
        if pct_int > 0.80: score += 25
        elif pct_int > 0.60: score += 12
        if pct_zero > 0.15: score += 15
        if pct_pos > 0.5:  score += 10
        if has_dec > 0.5:  score -= 15
        if pct_alpha > 0.3: score -= 25
        return max(0, min(score, 100))

    # ── CA / REVENUS (montants importants positifs) ──────────────────────
    elif std == "ca":
        if pct_num < 0.7: return 5
        score = 35
        if pct_pos > 0.95:    score += 20
        if median_val > 100:  score += 20
        if has_dec > 0.2:     score += 10
        if pct_zero > 0.10:   score -= 15
        if pct_alpha > 0.2:   score -= 30
        return max(0, min(score, 100))

    # ── COUTS (montants importants) ──────────────────────────────────────
    elif std == "co":
        if pct_num < 0.7: return 5
        score = 35
        if median_val > 50:   score += 20
        if has_dec > 0.2:     score += 10
        if pct_zero > 0.20:   score -= 10
        if pct_alpha > 0.2:   score -= 30
        return max(0, min(score, 100))

    # ── DISTANCE (positifs, souvent decimales) ───────────────────────────
    elif std == "dist":
        if pct_num < 0.7: return 5
        score = 30
        if pct_pos > 0.95:    score += 20
        if median_val > 5:    score += 15
        if pct_alpha > 0.2:   score -= 30
        return max(0, min(score, 100))

    # ── POIDS (positifs, souvent decimales) ──────────────────────────────
    elif std == "poids":
        if pct_num < 0.7: return 5
        score = 30
        if pct_pos > 0.95:    score += 20
        if has_dec > 0.3:     score += 15
        if pct_alpha > 0.2:   score -= 30
        return max(0, min(score, 100))

    # ── DEP / ARR (texte, villes) ────────────────────────────────────────
    elif std in ("dep", "arr"):
        score = 0
        if pct_alpha > 0.6: score += 50
        if 3 <= avg_len <= 60: score += 25
        if pct_num < 0.3:   score += 15
        if unique_r < 0.95: score += 10
        return max(0, min(score, 100))

    # ── MODE (texte, peu de valeurs uniques) ─────────────────────────────
    elif std == "mode":
        score = 0
        if pct_alpha > 0.6: score += 40
        if unique_r < 0.30: score += 30
        if pct_num < 0.2:   score += 15
        return max(0, min(score, 100))

    return 0


def _score_combine(propre, series, std):
    """Combine score nom et score contenu de maniere ponderee."""
    sn = _score_nom(propre, std)
    sc = _score_contenu(series, std)
    if sn >= 70:
        return int(sn * 0.65 + sc * 0.35)
    elif sn >= 45:
        return int(sn * 0.55 + sc * 0.45)
    else:
        return int(sn * 0.25 + sc * 0.75)


# ════════════════════════════════════════════════════════════════════════════
# MAPPER UNIFIE (cœur du systeme)
# ════════════════════════════════════════════════════════════════════════════
def _build_score_matrix(df, concepts):
    """Construit une matrice score[concept][colonne] avec boosters."""
    propres = {col: nettoyer(col) for col in df.columns}
    scores = {std: {} for std in concepts}

    for col in df.columns:
        propre = propres[col]
        for std in concepts:
            scores[std][col] = min(_score_combine(propre, df[col], std), 100)

    # Booster : desambiguation quantite vs prix
    if "quantite" in concepts and "prix_unitaire" in concepts:
        for col in df.columns:
            vals = _safe_numeric(df[col]).dropna()
            if len(vals) > 5:
                if (vals % 1 == 0).mean() > 0.9 and vals.median() > 10:
                    scores["quantite"][col] = min(scores["quantite"][col] + 10, 100)
                    scores["prix_unitaire"][col] = max(scores["prix_unitaire"][col] - 8, 0)
                if (vals % 1 != 0).mean() > 0.5 and vals.median() < 1000:
                    scores["prix_unitaire"][col] = min(scores["prix_unitaire"][col] + 10, 100)
                    scores["quantite"][col] = max(scores["quantite"][col] - 8, 0)

    # Booster : desambiguation CA vs Cout (par moyenne)
    if "ca" in concepts and "co" in concepts:
        ca_candidates = [
            col for col in df.columns
            if scores["ca"][col] >= 50 and scores["co"][col] >= 50
        ]
        if len(ca_candidates) >= 2:
            avgs = {}
            for col in ca_candidates:
                vals = _safe_numeric(df[col]).dropna()
                if len(vals) > 0:
                    avgs[col] = vals.mean()

            if len(avgs) >= 2:
                sorted_cols = sorted(avgs.items(), key=lambda x: -x[1])
                ca_winner = sorted_cols[0][0]
                co_winner = sorted_cols[-1][0]

                if ca_winner != co_winner:
                    scores["ca"][ca_winner] = min(scores["ca"][ca_winner] + 8, 100)
                    scores["co"][ca_winner] = max(scores["co"][ca_winner] - 8, 0)
                    scores["co"][co_winner] = min(scores["co"][co_winner] + 8, 100)
                    scores["ca"][co_winner] = max(scores["ca"][co_winner] - 8, 0)

    # Booster V7 : desambiguation sold vs available (e-commerce / ERP)
    # "sold" = consommation, "available" = stock. Jamais l'inverse.
    _sold_kw = ("sold", "itemssold", "unitssold", "vendu", "vendus", "nbvendu")
    _avail_kw = ("available", "onhand", "instock", "dispo", "disponible", "stockdispo", "remaining")
    _conso_stds = [s for s in concepts if s.startswith("conso_")]
    if "quantite" in concepts and _conso_stds:
        for col in df.columns:
            cn = nettoyer(col)
            # Si le nom de colonne contient un mot "sold" → c'est de la conso, PAS du stock
            if any(kw in cn for kw in _sold_kw):
                scores["quantite"][col] = max(scores["quantite"][col] - 40, 0)
                for cs in _conso_stds:
                    scores[cs][col] = min(scores[cs][col] + 30, 100)
            # Si le nom contient "available" → c'est du stock, PAS de la conso
            if any(kw in cn for kw in _avail_kw):
                scores["quantite"][col] = min(scores["quantite"][col] + 25, 100)
                for cs in _conso_stds:
                    scores[cs][col] = max(scores[cs][col] - 25, 0)

    # Booster V7 : anti-reference pour location/date/texte non-produit
    _anti_ref_kw = ("location", "lieu", "adresse", "address", "city", "ville",
                     "country", "pays", "region", "updated", "date", "time",
                     "created", "modified", "timestamp", "currency", "devise")
    if "reference" in concepts:
        for col in df.columns:
            cn = nettoyer(col)
            if any(kw in cn for kw in _anti_ref_kw):
                scores["reference"][col] = max(scores["reference"][col] - 30, 0)

    # ══ GARDE-FOU V8.1 : CONSO = NOM OBLIGATOIRE ══════════════════════════
    # Une consommation est INDISTINGUABLE d'un stock ou de tout autre nombre
    # par ses valeurs seules. Si le NOM de la colonne n'evoque pas une conso
    # (score nom < 45), le concept est interdit pour cette colonne — quel que
    # soit son contenu. Empeche d'inventer des consommations a partir de
    # colonnes numeriques parasites (poids, codes, prix barres...).
    # En plus : blocklist des termes de VALORISATION. "Valeur vente", "PV",
    # "Marge" sont des montants, jamais des quantites consommees.
    _conso_all = [s for s in concepts if s.startswith("conso_")]
    _conso_block = ("valeur", "montant", "marge", "tarif", "prix", "cout", "cost", "amount", "revenue")
    for cs in _conso_all:
        for col in df.columns:
            cn = propres[col]
            if _score_nom(cn, cs) < 45:
                scores[cs][col] = 0
            elif any(b in cn for b in _conso_block) or cn.startswith("pv") or cn.startswith("pa"):
                scores[cs][col] = 0

    # ══ GARDE-FOU V8.1 : PRIX UNITAIRE vs VALEUR TOTALE vs PRIX DE VENTE ══
    # Si une colonne au nom explicitement UNITAIRE existe, elle prime.
    # Les colonnes "valeur/total/montant" sont des agregats ligne, pas des PU.
    # Le prix d'ACHAT (PA) prime sur le prix de VENTE (PV) pour valoriser le stock.
    if "prix_unitaire" in concepts:
        _unit_kw = ("unitaire", "unit", "pmp")
        _total_kw = ("valeur", "total", "montant", "somme", "amount")
        _achat_kw = ("achat", "pa", "purchase", "cost", "revient")
        _has_unit_col = any(any(k in propres[c] for k in _unit_kw) for c in df.columns)
        _has_achat_col = any(
            any(k in propres[c] for k in _achat_kw) and any(k in propres[c] for k in _unit_kw)
            for c in df.columns
        )
        for col in df.columns:
            cn = propres[col]
            if _has_unit_col:
                if any(k in cn for k in _unit_kw):
                    scores["prix_unitaire"][col] = min(scores["prix_unitaire"][col] + 20, 100)
                elif any(k in cn for k in _total_kw):
                    scores["prix_unitaire"][col] = max(scores["prix_unitaire"][col] - 45, 0)
            # PA prime sur PV uniquement si une colonne achat unitaire existe
            if _has_achat_col and (cn.startswith("pv") or "prixvente" in cn or "sellingprice" in cn or "pvunitaire" in cn):
                scores["prix_unitaire"][col] = max(scores["prix_unitaire"][col] - 25, 0)

    # ══ GARDE-FOU V8.1 : CARDINALITE REFERENCE vs CATEGORIE ══════════════
    # Une reference est quasi-unique par ligne. Une categorie/famille se
    # repete massivement. C'est le discriminant SCM universel.
    if "reference" in concepts and len(df) >= 15:
        for col in df.columns:
            try:
                s = df[col].dropna()
                if len(s) < 10:
                    continue
                unique_ratio = s.nunique() / len(s)
                # Peu de valeurs uniques = categorie/famille, PAS une reference
                if unique_ratio < 0.30:
                    scores["reference"][col] = max(scores["reference"][col] - 50, 0)
                    if "categorie" in concepts:
                        scores["categorie"][col] = min(scores["categorie"][col] + 20, 100)
                # Quasi-unique par ligne = reference, PAS une categorie
                elif unique_ratio > 0.85:
                    scores["reference"][col] = min(scores["reference"][col] + 15, 100)
                    if "categorie" in concepts:
                        scores["categorie"][col] = max(scores["categorie"][col] - 30, 0)
            except Exception:
                continue

    # ══ GARDE-FOU V8.2 : PURETE NUMERIQUE pour QUANTITE et PRIX ══════════
    # Un champ numerique (quantite, prix) doit privilegier une colonne
    # numeriquement PROPRE. Une colonne texte-libre du type "14 packs de 4
    # + 15 unites" produit des nombres absurdes une fois nettoyee (14415).
    # Signal le plus fiable : le dtype pandas. Si pandas a lu la colonne comme
    # float/int, elle est propre. Si object/str, au moins une valeur est du
    # texte -> risque. On scanne TOUTE la colonne (pas un echantillon) pour
    # ne pas rater une valeur sale en position lointaine.
    import pandas.api.types as _ptypes
    _num_concepts = [c for c in ("quantite", "prix_unitaire") if c in concepts]
    if _num_concepts:
        for std in _num_concepts:
            # Colonnes candidates bien scorees pour ce concept
            candidates = [c for c in df.columns if scores[std][c] >= 40]
            # Parmi elles, y a-t-il au moins une colonne numerique NATIVE (float/int) ?
            native_num = [c for c in candidates if _ptypes.is_numeric_dtype(df[c])]
            if native_num:
                # Une colonne propre existe : toute colonne candidate NON-native
                # (object/str, donc contenant au moins une valeur texte) est
                # fortement penalisee. Un seul "14 packs de 4" suffit a la
                # rendre dangereuse (produit des nombres absurdes au nettoyage).
                for c in candidates:
                    if not _ptypes.is_numeric_dtype(df[c]):
                        scores[std][c] = max(int(scores[std][c] * 0.20), 0)
                # Et on donne un petit bonus aux natives pour trancher les ex-aequo
                for c in native_num:
                    scores[std][c] = min(scores[std][c] + 6, 100)

    return scores, propres


def _select_best_columns(scores, propres, ordre, seuils):
    """
    Selectionne pour chaque concept la meilleure colonne candidate.

    Strategie :
    1. Pour chaque colonne, trouver le concept qui a le meilleur score NOM
       (priorite haute si nom >= 70 = match exact ou tres proche)
    2. Puis traiter les concepts dans l'ordre de priorite, en utilisant
       les colonnes qui n'ont pas deja ete prises par un score nom fort
    """
    trouvees = {}
    utilises = set()

    # ── ETAPE 1 : pre-attribution par score NOM fort (>= 70) ──
    # Pour chaque colonne, on regarde quel concept a le meilleur score nom.
    # Si ce score est >= 70, on attribue immediatement.
    # V8.1 : MAIS seulement si le score combine reste competitif (>= max - 10).
    # Les garde-fous penalisent volontairement certains scores combines
    # (ex: "Valeur achat" pour prix_unitaire) — la selection doit les respecter.
    for col in list(scores[ordre[0]].keys()):  # iterate sur les colonnes
        best_concept = None
        best_nom_score = 0
        for std in ordre:
            if std in utilises:
                continue
            nom_score = _score_nom(propres[col], std)
            if nom_score > best_nom_score and nom_score >= 70:
                # Verifier que le score combine depasse le seuil ET reste competitif
                _max_comb = max(scores[std].values()) if scores[std] else 0
                if scores[std][col] >= seuils.get(std, 55) and scores[std][col] >= _max_comb - 10:
                    best_nom_score = nom_score
                    best_concept = std
        if best_concept and col not in trouvees:
            trouvees[col] = best_concept
            utilises.add(best_concept)
            _debug_log(
                f"Match {best_concept:14s} → '{col}' "
                f"(score={scores[best_concept][col]}, nom={best_nom_score}) [pre-attribution]",
                "ok"
            )

    # ── ETAPE 2 : selection classique pour les concepts restants ──
    for std in ordre:
        if std in utilises:
            continue
        seuil = seuils.get(std, 55)
        candidats = [
            (col, scores[std][col]) for col in scores[std]
            if col not in trouvees and scores[std][col] >= seuil
        ]
        if not candidats:
            continue

        _best_comb = max(sc for _, sc in candidats)
        nom_forts = [
            (col, sc) for col, sc in candidats
            if _score_nom(propres[col], std) >= 70 and sc >= _best_comb - 10
        ]
        if nom_forts:
            gagnant = max(nom_forts, key=lambda x: _score_nom(propres[x[0]], std))[0]
        else:
            gagnant = max(candidats, key=lambda x: x[1])[0]

        trouvees[gagnant] = std
        utilises.add(std)
        _debug_log(
            f"Match {std:14s} → '{gagnant}' "
            f"(score={scores[std][gagnant]}, "
            f"nom={_score_nom(propres[gagnant], std)})",
            "ok"
        )

    return trouvees, utilises


def _fallback_heuristique(df, concepts, trouvees, utilises):
    """Fallback si concepts critiques manquent (heuristiques sans IA)."""
    cols = list(df.columns)

    # Reference manquante : colonne alphanumerique la PLUS unique (pas la 1ere venue)
    if "reference" in concepts and "reference" not in utilises:
        best_ref, best_uratio = None, 0.0
        for c in cols:
            if c not in trouvees:
                s = df[c].dropna().head(50)
                if len(s) < 5:
                    continue
                has_alpha = s.astype(str).str.contains(r'[a-zA-Z]', na=False).mean() > 0.3
                uratio = s.nunique() / max(len(s), 1)
                # Une reference doit etre majoritairement unique : seuil 0.5 minimum
                if has_alpha and uratio >= 0.5 and uratio > best_uratio:
                    best_ref, best_uratio = c, uratio
        if best_ref:
            trouvees[best_ref] = "reference"
            utilises.add("reference")
            _debug_log(f"Fallback reference → '{best_ref}' (unicite={best_uratio:.0%})", "warn")

    # Quantite manquante : colonne d'entiers
    if "quantite" in concepts and "quantite" not in utilises:
        for c in cols:
            if c not in trouvees:
                num = _safe_numeric(df[c])
                if num.notna().mean() > 0.6 and (num.dropna() % 1 == 0).mean() > 0.6:
                    trouvees[c] = "quantite"
                    utilises.add("quantite")
                    _debug_log(f"Fallback quantite → '{c}'", "warn")
                    break

    # Client manquant : 1ere colonne textuelle
    if "client" in concepts and "client" not in utilises:
        for c in cols:
            if c not in trouvees:
                s = df[c].dropna().head(20)
                if s.astype(str).str.contains(r'[a-zA-Z]', na=False).mean() > 0.4:
                    trouvees[c] = "client"
                    utilises.add("client")
                    _debug_log(f"Fallback client → '{c}'", "warn")
                    break

    # CA manquant : colonne numerique avec plus grande moyenne
    if "ca" in concepts and "ca" not in utilises:
        best_col, best_avg = None, 0
        for c in cols:
            if c not in trouvees:
                num = _safe_numeric(df[c]).dropna()
                if len(num) > 5 and (num >= 0).mean() > 0.9:
                    avg = num.mean()
                    if avg > best_avg:
                        best_avg = avg
                        best_col = c
        if best_col:
            trouvees[best_col] = "ca"
            utilises.add("ca")
            _debug_log(f"Fallback ca → '{best_col}' (moy={best_avg:.0f})", "warn")

    # Cout manquant : colonne numerique avec 2eme plus grande moyenne
    if "co" in concepts and "co" not in utilises:
        best_col, best_avg = None, 0
        for c in cols:
            if c not in trouvees:
                num = _safe_numeric(df[c]).dropna()
                if len(num) > 5 and (num >= 0).mean() > 0.9:
                    avg = num.mean()
                    if avg > best_avg:
                        best_avg = avg
                        best_col = c
        if best_col:
            trouvees[best_col] = "co"
            utilises.add("co")
            _debug_log(f"Fallback co → '{best_col}' (moy={best_avg:.0f})", "warn")

    return trouvees, utilises


def _ai_rescue(df, manquants, trouvees, client_ai=None):
    """Dernier recours : appel IA (OpenAI puis Gemini)."""
    if not manquants:
        return trouvees

    titres = list(df.columns)
    sample_data = df.head(5).astype(str).to_dict(orient='list')
    prompt = (
        f"Logistics file column mapping task.\n"
        f"Available columns: {titres}\n"
        f"Data sample (5 rows): {json.dumps(sample_data, ensure_ascii=False)[:3000]}\n"
        f"Missing concepts to map: {manquants}\n\n"
        f"For each missing concept, choose the most appropriate column from "
        f"the available columns (or null if none fits).\n"
        f"Reply ONLY with a JSON object like:\n"
        f'{{"concept_name": "exact_column_title"}}\n'
        f"or null values if no match.\n"
        f"Available columns to choose from: {titres}\n"
        f"Reply with JSON only, no markdown."
    )

    response = _ai_call_with_fallback(prompt, client_ai=client_ai)
    if not response:
        _debug_log("AI rescue impossible (OpenAI + Gemini KO)", "err")
        return trouvees

    gpt_map = _parse_json_response(response)
    if not gpt_map:
        _debug_log("AI rescue : parsing JSON echoue", "err")
        return trouvees

    for std, col in gpt_map.items():
        if std in manquants and col in df.columns and col not in trouvees:
            trouvees[col] = std
            _debug_log(f"AI rescue {std} → '{col}'", "warn")

    return trouvees


# ════════════════════════════════════════════════════════════════════════════
# HOOKS PERSISTENCE SUPABASE (V2 - placeholders)
# ════════════════════════════════════════════════════════════════════════════
def _file_signature(df):
    """Signature unique pour cache persistant V2."""
    import hashlib
    cols_str = "|".join(sorted(str(c) for c in df.columns))
    sample_str = df.head(3).astype(str).to_string()
    combined = (cols_str + sample_str).encode("utf-8")
    return hashlib.md5(combined).hexdigest()[:16]


def _try_get_cached_mapping_supabase(user_id, file_sig):
    """[V2 PLACEHOLDER] Recupere un mapping cache. Retourne None en V1."""
    return None


def _save_mapping_to_supabase(user_id, file_sig, mapping, module="transport"):
    """[V2 PLACEHOLDER] Sauvegarde un mapping. Ne fait rien en V1."""
    return False


# ════════════════════════════════════════════════════════════════════════════
# API PUBLIQUE 1 : SMART INGESTER STOCK
# ════════════════════════════════════════════════════════════════════════════
def smart_ingester_stock_ultime(df, client_ai=None):
    """
    Mappe les colonnes d'un fichier de stock.

    Returns:
        (df_renomme, "Succes") ou (None, message_erreur)
    """
    _debug_log("════ Smart Ingester STOCK ════", "info")

    df = df.dropna(how='all').copy()
    df = df[df.apply(lambda r: r.astype(str).str.strip().ne('').any(), axis=1)]

    if df.empty:
        return None, "Le fichier est vide ou ne contient que des lignes vides."

    concepts = ["reference", "quantite", "prix_unitaire",
                "conso_an1", "conso_an2", "conso_an3", "conso_an4",
                "date_peremption"]

    scores, propres = _build_score_matrix(df, concepts)

    ordre = ["reference", "quantite", "prix_unitaire",
             "conso_an4", "conso_an3", "conso_an2", "conso_an1",
             "date_peremption"]
    seuils = {
        "reference": 35, "quantite": 55, "prix_unitaire": 55,
        "conso_an4": 55, "conso_an3": 55, "conso_an2": 55, "conso_an1": 55,
        "date_peremption": 45,
    }

    trouvees, utilises = _select_best_columns(scores, propres, ordre, seuils)
    trouvees, utilises = _fallback_heuristique(df, concepts, trouvees, utilises)

    critiques = [s for s in ["reference", "quantite"] if s not in utilises]
    if critiques:
        trouvees = _ai_rescue(df, critiques, trouvees, client_ai)

    df = df.rename(columns=trouvees)

    # ── TRACE DU MAPPING (indispensable pour diagnostiquer une conso fantome) ──
    try:
        _map_txt = " | ".join(f"'{src}' -> {dst}" for src, dst in trouvees.items())
        _debug_log(f"Mapping retenu : {_map_txt}", "info")
        _map_conso = [f"'{src}' -> {dst}" for src, dst in trouvees.items()
                      if str(dst).startswith("conso_")]
        if _map_conso:
            _debug_log("Colonnes interpretees comme CONSOMMATION : "
                       + " | ".join(_map_conso), "warn")
    except Exception:
        pass

    manq = [c for c in ["reference", "quantite"] if c not in df.columns]
    if manq:
        return None, (
            f"Colonnes introuvables : {', '.join(manq)}.\n"
            f"Colonnes dans votre fichier : {list(df.columns[:10])}"
        )

    df["quantite"] = _safe_numeric(df["quantite"])
    df = df.dropna(subset=["quantite"]).copy()
    df = df[df["reference"].astype(str).str.strip().ne('')]
    df = df[~df["reference"].astype(str).str.lower().isin(['nan', 'none', ''])]

    if "prix_unitaire" not in df.columns:
        df["prix_unitaire"] = 0.0
        df["_sans_prix"] = True
    else:
        df["prix_unitaire"] = _safe_numeric(df["prix_unitaire"]).fillna(0)
        df["_sans_prix"] = (df["prix_unitaire"] == 0).all()

    # ══ GARDE-FOU V8.4 : has_conso EXIGE DU VOLUME REEL ═══════════════════
    # BUG HISTORIQUE : has_conso passait a True des que la COLONNE EXISTAIT,
    # sans verifier qu'elle contienne la moindre valeur > 0. Une colonne
    # parasite mappee par erreur (code article, numero de ligne...) suffisait
    # a activer tout le moteur de predictions. Tous les garde-fous en aval
    # lisent ce drapeau : s'il est faux ici, ils sont tous inoperants.
    # Desormais : au moins _CONSO_MIN_RATIO des lignes doivent porter une
    # valeur strictement positive, avec un plancher absolu de 3 lignes.
    _CONSO_MIN_RATIO = 0.10
    _CONSO_MIN_ROWS = 3

    has_conso = False
    conso_cols = []
    _conso_diag = []
    _n_lignes = max(len(df), 1)
    _seuil_lignes = max(_CONSO_MIN_ROWS, int(_n_lignes * _CONSO_MIN_RATIO))

    for c in ["conso_an1", "conso_an2", "conso_an3", "conso_an4"]:
        if c in df.columns:
            df[c] = _safe_numeric(df[c]).fillna(0)
            _n_pos = int((df[c] > 0).sum())
            _conso_diag.append(f"{c}: {_n_pos}/{_n_lignes} lignes > 0")
            if _n_pos >= _seuil_lignes:
                conso_cols.append(c)
                has_conso = True
            else:
                # Colonne mappee mais vide/quasi-vide -> neutralisee.
                df[c] = 0.0

    if _conso_diag:
        _debug_log("Conso detectee -> " + " | ".join(_conso_diag)
                   + f" (seuil={_seuil_lignes} lignes)",
                   "info" if has_conso else "warn")
    if not has_conso:
        _debug_log("has_conso = FALSE : aucune colonne de consommation exploitable. "
                   "Predictions de rupture, surstock et stock mort desactives.", "warn")

    df["_has_conso"] = has_conso
    df["_conso_moy"] = df[conso_cols].mean(axis=1) if has_conso else 0.0

    # ── V8 : DATE DE PEREMPTION ──────────────────────────────────────────
    has_peremption = False
    if "date_peremption" in df.columns:
        _raw_dates = df["date_peremption"].dropna().astype(str)
        _looks_like_dates = _raw_dates.str.contains(r'[-/.]|[a-zA-Z]', na=False).mean() >= 0.5 if len(_raw_dates) > 0 else False
        if _looks_like_dates:
            _parsed = pd.to_datetime(df["date_peremption"], errors='coerce', format='ISO8601')
            if _parsed.isna().mean() > 0.5:
                _parsed = pd.to_datetime(df["date_peremption"], errors='coerce', dayfirst=True)
            # Rattrapage format MM/YYYY ou MM-YYYY (frequent sur conserves) → 1er du mois
            _still_na = _parsed.isna() & df["date_peremption"].notna()
            if _still_na.any():
                _mm = df.loc[_still_na, "date_peremption"].astype(str).str.strip()
                _r1 = pd.to_datetime(_mm, errors='coerce', format='%m/%Y')
                _r2 = pd.to_datetime(_mm, errors='coerce', format='%m-%Y')
                _parsed.loc[_still_na] = _r1.fillna(_r2)
            df["date_peremption"] = _parsed
            # Colonne exploitable si au moins 10% des lignes ont une date valide
            if df["date_peremption"].notna().mean() >= 0.10:
                has_peremption = True
            else:
                df = df.drop(columns=["date_peremption"])
        else:
            # Colonne numerique pure mal mappee (poids, code...) → on retire
            df = df.rename(columns={"date_peremption": "_col_rejetee_peremption"})
    df["_has_peremption"] = has_peremption

    _debug_log(f"Stock OK. Colonnes finales : {list(df.columns)[:15]}", "ok")
    return df.copy(), "Succes"


# ════════════════════════════════════════════════════════════════════════════
# API PUBLIQUE 2 : MAPPER TRANSPORT ROUTIER (V1)
# ════════════════════════════════════════════════════════════════════════════
def auto_map_columns_with_ai(df, client_ai=None):
    """
    Mappe les colonnes d'un fichier transport routier.

    En V1, traite uniquement le transport ROUTIER (les autres modes sont
    ignores volontairement pour stabilite).

    Returns:
        dict {concept: nom_colonne} avec cles toujours presentes :
        client, ca, co, dep, arr, dist, mode, poids
    """
    _debug_log("════ Mapper TRANSPORT ROUTIER ════", "info")

    if df is None or df.empty:
        _debug_log("DataFrame vide", "err")
        return {"client": None, "ca": None, "co": None, "dep": None,
                "arr": None, "dist": None, "mode": None, "poids": None}

    # Cache local session (par hash de structure)
    cache_key = f"transport_mapping_{hash(tuple(df.columns))}_{len(df)}"
    if cache_key in st.session_state:
        cached = st.session_state[cache_key]
        _debug_log(f"Mapping cache (session) recupere", "info")
        return cached

    # [V2 PLACEHOLDER] Cache persistant Supabase
    user_id = st.session_state.get("current_user", None)
    file_sig = _file_signature(df)
    cached_remote = _try_get_cached_mapping_supabase(user_id, file_sig)
    if cached_remote:
        _debug_log("Mapping cache (Supabase) recupere", "info")
        st.session_state[cache_key] = cached_remote
        return cached_remote

    # Mapping from scratch
    concepts = ["client", "ca", "co", "dep", "arr", "dist", "poids", "mode"]
    scores, propres = _build_score_matrix(df, concepts)

    ordre = ["ca", "co", "dist", "client", "dep", "arr", "poids", "mode"]
    seuils = {
        "client": 40, "ca": 50, "co": 50, "dep": 45, "arr": 45,
        "dist": 50, "poids": 55, "mode": 55,
    }

    trouvees, utilises = _select_best_columns(scores, propres, ordre, seuils)
    trouvees, utilises = _fallback_heuristique(df, concepts, trouvees, utilises)

    # Si CA, CO ou client critique manquant : IA en dernier recours
    critiques = [s for s in ["ca", "co", "client"] if s not in utilises]
    if critiques:
        trouvees = _ai_rescue(df, critiques, trouvees, client_ai)

    mapping_final = {}
    for col, std in trouvees.items():
        mapping_final[std] = col

    # Garantir TOUTES les cles essentielles (None si non trouvees)
    for k in ["client", "ca", "co", "dep", "arr", "dist", "mode", "poids"]:
        if k not in mapping_final:
            mapping_final[k] = None

    # Ultime fallback defensif
    if not mapping_final.get("client"):
        mapping_final["client"] = df.columns[0] if len(df.columns) > 0 else None

    if not mapping_final.get("ca") and len(df.columns) >= 2:
        used_cols = set(v for v in mapping_final.values() if v)
        for c in df.columns:
            if c not in used_cols:
                num = _safe_numeric(df[c]).dropna()
                if len(num) > 5 and (num >= 0).mean() > 0.8:
                    mapping_final["ca"] = c
                    _debug_log(f"Ultime fallback ca → '{c}'", "warn")
                    break

    _debug_log(f"Transport mapping final : {mapping_final}", "ok")

    st.session_state[cache_key] = mapping_final
    _save_mapping_to_supabase(user_id, file_sig, mapping_final, module="transport")

    return mapping_final


# ════════════════════════════════════════════════════════════════════════════
# API PUBLIQUE 3 : DETECTION MODE TRANSPORT (V1 = ROUTIER FORCE)
# ════════════════════════════════════════════════════════════════════════════
def detect_transport_mode(df, dep_col=None, arr_col=None, mode_col=None):
    """
    Detection du mode de transport.

    EN V1 : retourne TOUJOURS "routier" (les autres modes sont caches).
    Le code de detection complet est conserve mais desactive via
    ENABLE_MULTIMODAL_DETECTION = False en haut du fichier.

    Reactivation en V2/V3 : passer ENABLE_MULTIMODAL_DETECTION a True.

    Returns:
        (mode, label, emoji)
    """
    # ── V1 : MODE ROUTIER FORCE (silencieux) ──
    if not ENABLE_MULTIMODAL_DETECTION:
        lang = st.session_state.get("language", "fr")
        label = "🚛 Transport routier" if lang == "fr" else "🚛 Road transport"
        _debug_log("Mode forcé à 'routier' (V1)", "info")
        return "routier", label, "🚛"

    # ── V2/V3 : DETECTION MULTI-MODE COMPLETE ──
    PORTS = [
        "havre", "marseille", "dunkerque", "bordeaux", "hamburg", "rotterdam",
        "antwerp", "anvers", "barcelona", "barcelone", "genova", "genes",
        "tanger", "tangermed", "casablanca", "dakar", "abidjan",
        "shanghai", "ningbo", "shenzhen", "hongkong", "singapore", "singapour",
        "dubai", "jeddah", "mumbai",
    ]
    AIRPORT_CODES = {
        "cdg", "ory", "lyo", "mrs", "nce", "tls", "bod",
        "jfk", "lax", "lhr", "fra", "muc", "ams", "bru", "mad", "fco", "mxp",
        "dxb", "auh", "doh", "bom", "del", "hkg", "nrt", "sin",
    }
    ROAD_CITIES = {
        "paris", "lyon", "toulouse", "bordeaux", "lille", "marseille",
        "nantes", "strasbourg", "rennes", "nice", "grenoble", "montpellier",
        "tours", "dijon", "metz", "nancy", "reims", "rouen", "amiens",
        "clermont", "limoges", "bruxelles", "amsterdam", "berlin", "munich",
        "madrid", "rome", "milan", "geneve", "zurich", "rotterdam", "hamburg",
    }
    KW_AIR = ["aerien", "air freight", "airfreight", "awb", "air waybill",
              "fret aerien", "airline cargo", "avion"]
    KW_SEA = ["maritime", "seafreight", "sea freight", "ocean freight",
              "bateau", "navire", "conteneur", "container", "teu", "fcl", "lcl",
              "armateur", "roro", "reefer", "vrac", "bulk", "mer", "ocean"]
    KW_RAIL = ["ferroviaire", "rail freight", "fret ferroviaire",
               "sncf", "wagon", "railway", "train cargo"]
    KW_ROAD = ["routier", "road", "camion", "truck", "ftl", "ltl", "vl", "tir",
               "messagerie", "groupage", "express", "fret routier",
               "road freight", "haulage", "trucking", "trailer"]

    scores = {"aerien": 0, "maritime": 0, "ferroviaire": 0, "routier": 0}

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
        if not col or col not in df.columns:
            continue
        for v in df[col].dropna().astype(str):
            raw_tokens = re.split(r'[\s\-/,]+', v.strip())
            tokens_clean = [nettoyer(t) for t in raw_tokens if t.strip()]
            for tok in tokens_clean:
                if tok in AIRPORT_CODES:
                    scores["aerien"] += 2
                if any(p in tok or tok in p for p in PORTS if len(p) >= 5):
                    scores["maritime"] += 1
                if (tok in ROAD_CITIES
                    or any(tok in rc for rc in ROAD_CITIES if len(rc) >= 5)):
                    scores["routier"] += 1

    hdrs = [nettoyer(c) for c in df.columns]
    if any("awb" in h for h in hdrs):
        scores["aerien"] += 6
    if any("airwaybill" in h for h in hdrs):
        scores["aerien"] += 6
    if any("billoflading" in h or "bl" == h for h in hdrs):
        scores["maritime"] += 6
    if any("teu" in h for h in hdrs):
        scores["maritime"] += 5
    if any("conteneur" in h or "container" in h for h in hdrs):
        scores["maritime"] += 5
    if any("distancekm" in h or "km" in h for h in hdrs):
        scores["routier"] += 4
    if any("wagon" in h or "sncf" in h for h in hdrs):
        scores["ferroviaire"] += 6

    total = sum(scores.values())
    dominant = max(scores, key=scores.get)
    top_val = scores[dominant]

    _debug_log(f"Mode detection scores : {scores}", "info")

    SCORE_MIN = 4
    if total < 2 or top_val < SCORE_MIN:
        _debug_log("Mode = unknown (score insuffisant)", "warn")
        return "unknown", "?", "?"

    rivals = [k for k, v in scores.items() if v == top_val and k != dominant]
    if rivals:
        _debug_log(f"Mode = unknown (egalite {dominant} vs {rivals})", "warn")
        return "unknown", "?", "?"

    lang = st.session_state.get("language", "fr")
    labels_fr = {
        "aerien": ("✈️ Mode Aerien detecte", "✈️"),
        "maritime": ("⚓ Mode Maritime detecte", "⚓"),
        "ferroviaire": ("🚂 Mode Ferroviaire detecte", "🚂"),
        "routier": ("🚛 Mode Routier detecte", "🚛"),
    }
    labels_en = {
        "aerien": ("✈️ Air mode detected", "✈️"),
        "maritime": ("⚓ Maritime mode detected", "⚓"),
        "ferroviaire": ("🚂 Rail mode detected", "🚂"),
        "routier": ("🚛 Road mode detected", "🚛"),
    }
    labels = labels_en if lang == "en" else labels_fr
    label, emoji = labels[dominant]

    _debug_log(f"Mode detecte = {dominant} (score={top_val})", "ok")
    return dominant, label, emoji
