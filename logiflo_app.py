# Public app - no Streamlit auth required

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import datetime
import re
import tempfile
import os
import difflib
import unicodedata
import math
import time
import requests
import concurrent.futures
import base64
import json
import io
from fpdf import FPDF
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="LOGIFLO.IO | Control Tower", layout="wide", page_icon="🏢")

# =========================================
# 0. INIT IA
# =========================================
client = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY", ""))

# =========================================
# 0.1 AUTH
# =========================================
USERS_DB = {
    "eric":         "logiflo2026",
    "admin":        "admin123",
    "demo_client1": "audit2026",
    "demo_client2": "test2026",
    "jury":         "pitch2026",
    "partenaire":   "partner2026",
    "test":         "test123",
}

# =========================================
# 0.2 ORS
# =========================================
ORS_API_KEY = st.secrets.get("ORS_API_KEY", "")

# =========================================
# 0.3 GOOGLE SHEETS
# =========================================
SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID", "")

@st.cache_resource
def get_gsheet_client():
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception:
        return None

def get_user_sheet(username):
    gc = get_gsheet_client()
    if not gc or not SHEET_ID:
        return None
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            return sh.worksheet(username)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=username, rows=1000, cols=12)
            ws.append_row(["date","heure","module","nb_lignes",
                           "kpi_1","kpi_2","kpi_3",
                           "kpi_label_1","kpi_label_2","kpi_label_3",
                           "resume_ia","pdf_base64"])
            return ws
    except Exception:
        return None

def save_audit_to_sheets(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
    ws = get_user_sheet(username)
    if not ws:
        return False
    try:
        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else ""
        now = datetime.datetime.now()
        ws.append_row([
            now.strftime("%d/%m/%Y"), now.strftime("%H:%M"),
            module, nb_lignes,
            round(kpis[0], 2) if len(kpis) > 0 else "",
            round(kpis[1], 2) if len(kpis) > 1 else "",
            round(kpis[2], 2) if len(kpis) > 2 else "",
            labels[0] if len(labels) > 0 else "",
            labels[1] if len(labels) > 1 else "",
            labels[2] if len(labels) > 2 else "",
            resume_ia[:800] if resume_ia else "",
            pdf_b64,
        ])
        return True
    except Exception:
        return False

def load_archives_from_sheets(username):
    ws = get_user_sheet(username)
    if not ws:
        return None
    try:
        records = ws.get_all_records()
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception:
        return None

# =========================================
# 0.4 PROMPTS IA
# =========================================
PROMPT_STOCK = """
Tu es l'Auditeur Financier et Directeur Supply Chain Senior pour Logiflo.io.
Langue: FRANÇAIS UNIQUEMENT.

RÈGLE CRITIQUE sur les données :
- Si prix disponibles → analyse financière complète (capital immobilisé, valeur dormants)
- Si PAS de prix → analyse opérationnelle pure : rotation, vélocité, ruptures, dormants en quantités
- Si consommations historiques disponibles → calcule la couverture en mois et la tendance
- Si PAS de consommations → signale-le comme ANGLE MORT et propose une médiane sectorielle indicative
- Adapte toujours ton analyse aux données réellement disponibles. Ne jamais inventer de chiffres.

Structure obligatoire :

### DIAGNOSTIC OPERATIONNEL
Bilan du taux de service et de la rotation. Nomme les 3 références les plus critiques avec leurs chiffres exacts.

### DIAGNOSTIC FINANCIER & STOCKS DORMANTS
Si prix disponibles : capital immobilisé, dormants, cash trap.
Si pas de prix : vélocité par référence, articles à rotation nulle ou lente, risques de rupture cachés.
Si pas de consommations : signale ANGLE MORT - couverture estimée non calculable.

### PLAN D'ACTION IMMÉDIAT (TOP 3)
3 recommandations concrètes et actionnables.
Impact potentiel : Fort/Moyen/Faible | Difficulté d'exécution : 1 à 5

### SCORING LOGIFLO
- Performance & Rotation stock : /100
- Risque de rupture : /100
- Résilience supply chain : /100

RÈGLES : N'invente aucun montant. Saute une ligne entre chaque idée.
"""

PROMPT_TERRAIN = """
Tu es un chef magasinier expérimenté qui aide son équipe au quotidien.
Langue: FRANÇAIS, ton direct et simple, phrases courtes maximum.
Pas de jargon financier. Parle comme à un collègue sur le terrain.

RÈGLE CRITIQUE sur les données :
- Si pas de prix → parle uniquement en quantités (mètres, pièces, unités selon le contexte)
- Si pas de consommations → dis-le clairement : "Sans historique de conso, impossible de calculer ta couverture exacte. 
  Voici ce qu'on peut observer quand même :"
- Si consommations disponibles → calcule la couverture en semaines ou mois et dis si c'est trop ou insuffisant
- Cite toujours les vraies références du fichier

Structure :

### Ce qui est urgent
Les articles à commander ou surveiller maintenant. Références précises, quantités exactes.

### Ce qui dort
Articles sans mouvement ou en surstock. Action concrète pour chacun.

### Tes 3 actions pour cette semaine
Actions simples et réalisables.
Difficulté : Facile / Moyen / Compliqué

### En résumé
2-3 phrases max pour briefer ton responsable en 30 secondes.

RÈGLES : Concret uniquement. Pas de chiffres inventés.
"""

PROMPT_TRANSPORT = """
Tu es un Auditeur Senior en Stratégie Transport & Supply Chain.
Langue: FRANÇAIS UNIQUEMENT.
NE SOIS PAS UN PERROQUET : déduis les problèmes cachés.
Si le poids est absent : signale ANGLE MORT STRATÉGIQUE.

Structure obligatoire :

### AUDIT DE RENTABILITE
Analyse de la marge globale. Nomme les 3 trajets/clients qui détruisent la rentabilité.

### DIAGNOSTIC RÉSEAU
Cohérence spatiale et efficacité. Si poids disponible : analyse coût/kg et remplissage estimé.

### PLAN DE RATIONALISATION (TOP 3)
3 recommandations agressives.
Impact Cash : Fort/Moyen/Faible | Difficulté d'exécution : 1 à 5

### SCORING LOGIFLO
- Rentabilité & Yield Transport : /100
- Efficacité Opérationnelle : /100
- Maîtrise des OPEX : /100

RÈGLES : N'invente aucun montant. Saute une ligne entre chaque idée.
"""

# =========================================
# 1. SESSION STATE
# =========================================
defaults = {
    "page": "accueil", "module": "", "auth": False,
    "current_user": None,
    "df_stock": None, "df_trans": None,
    "history_stock": [], "stock_view": "MANAGER",
    "seuil_bas": 15, "seuil_rupture": 0, "seuil_km": 0,
    "geo_cache": {}, "route_cache": {},
    "trans_mapping": None, "trans_filename": None,
    "analysis_stock": None, "analysis_trans": None,
    "last_pdf": None, "last_kpis": [], "last_labels": [],
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# =========================================
# 2. CSS
# =========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');
:root {
    --navy:#0B2545;--navy2:#162D52;--green:#00C896;--green2:#00A87A;
    --slate:#4A6080;--light:#F0F4F8;--red:#E8304A;--white:#FFFFFF;
}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--navy);}
.block-container{padding-top:2rem!important;padding-bottom:2rem!important;max-width:95%!important;}
.kpi-card{background:var(--white);padding:24px;border-radius:12px;border:1px solid #e2e8f0;border-top:3px solid var(--green);box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);transition:transform 0.2s;}
.kpi-card:hover{transform:translateY(-2px);box-shadow:0 10px 15px -3px rgba(0,0,0,0.1);}
.kpi-card h4{color:var(--slate)!important;font-family:'DM Sans',sans-serif!important;font-size:0.75rem!important;text-transform:uppercase;font-weight:600;letter-spacing:1.5px;margin-bottom:10px;}
.kpi-card h2{font-family:'Syne',sans-serif!important;font-size:2.2rem!important;font-weight:800!important;margin-top:0;line-height:1;letter-spacing:-1px;}
.kpi-card p{font-size:12px;color:var(--slate);margin-top:6px;}
div.stButton>button{border-radius:8px;font-family:'Syne',sans-serif;font-weight:700;background-color:var(--navy);color:#f8fafc;border:none;transition:0.3s;}
div.stButton>button:hover{background-color:var(--navy2);transform:translateY(-2px);}
[data-testid="stSidebar"]{background-color:var(--navy)!important;}
[data-testid="stSidebar"] *{color:#ffffff!important;font-size:1rem!important;}
[data-testid="stSidebar"] hr{border-color:#1e3a5f!important;}
div[role="radiogroup"]>label{padding-bottom:12px;cursor:pointer;}
.sidebar-logo{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:white;letter-spacing:-0.5px;}
.sidebar-logo span{color:#00C896;}
.import-card{background:var(--white);padding:25px;border-radius:12px;border-left:6px solid var(--green);margin-bottom:20px;box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);}
.import-card h3{margin-top:0;color:var(--navy);font-family:'Syne',sans-serif;font-size:1rem;}
.import-card p{color:var(--slate);font-size:14px;margin-bottom:0;line-height:1.5;}
.report-text{background:var(--light);padding:32px;border-radius:12px;border-left:6px solid var(--navy);box-shadow:0 4px 6px rgba(0,0,0,0.06);line-height:1.8;}
.report-text h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:800;color:var(--navy);text-transform:uppercase;letter-spacing:1.5px;margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.report-text h3:first-child{margin-top:0;}
.report-text p{color:#2d3748;font-size:14px;margin-bottom:8px;}
.report-text strong{color:var(--navy);}
.report-terrain{background:#f8fff8;padding:28px;border-radius:12px;border-left:6px solid var(--green);box-shadow:0 4px 6px rgba(0,0,0,0.06);line-height:1.9;}
.report-terrain h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--green2);margin-top:24px;margin-bottom:8px;}
.report-terrain h3:first-child{margin-top:0;}
.report-terrain p{color:#1a2e1a;font-size:15px;margin-bottom:6px;}
.archive-card{background:var(--white);border:1px solid #E2EAF4;border-radius:12px;padding:20px;margin-bottom:16px;border-left:4px solid var(--green);box-shadow:0 2px 8px rgba(0,0,0,0.04);}
.archive-card h4{font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:var(--navy);margin-bottom:8px;}
.archive-card .meta{font-size:12px;color:var(--slate);margin-bottom:12px;}
.archive-kpi{display:inline-block;background:var(--light);border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;color:var(--navy);margin-right:8px;margin-bottom:8px;}
.sans-prix-badge{background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:var(--green2);font-size:12px;font-weight:600;padding:4px 12px;border-radius:20px;display:inline-block;margin-bottom:12px;}
.big-emoji{font-size:70px;margin-bottom:10px;display:block;text-align:center;}
.legal-text{background:var(--white);padding:32px;border-radius:12px;border:1px solid #E2EAF4;line-height:1.9;}
.legal-text h2{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--navy);margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.legal-text h2:first-child{margin-top:0;}
.legal-text p{color:#2d3748;font-size:14px;margin-bottom:8px;}
.legal-text ul{padding-left:20px;margin-bottom:8px;}
.legal-text li{color:#2d3748;font-size:14px;margin-bottom:4px;}
.legal-box{background:var(--light);border-left:4px solid var(--green);padding:16px 20px;border-radius:8px;margin:12px 0;}
.legal-box p{color:var(--navy)!important;font-weight:500;}
</style>
""", unsafe_allow_html=True)

# =========================================
# 3. HELPER — RENDU RAPPORT IA
# =========================================
def render_report(texte, mode="manager"):
    css_class = "report-terrain" if mode == "terrain" else "report-text"
    html_lines = []
    for line in texte.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.startswith('### '):
            html_lines.append(f"<h3>{line[4:].strip()}</h3>")
        else:
            line = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', line)
            if line.startswith('- ') or line.startswith('* '):
                html_lines.append(f"<p>• {line[2:]}</p>")
            else:
                html_lines.append(f"<p>{line}</p>")
    return f'<div class="{css_class}">{"".join(html_lines)}</div>'

# =========================================
# 4. MOTEUR PDF
# =========================================
class PDFReport(FPDF):
    def footer(self):
        self.set_y(-15)
        self.set_font("Arial","I",8)
        self.set_text_color(150,150,150)
        self.multi_cell(0,4,"Document genere par Logiflo.io. Recommandations a titre indicatif.",align="C")

def generate_expert_pdf(title, content, figs=None):
    pdf = PDFReport()
    pdf.add_page()
    pdf.set_fill_color(11,37,69); pdf.rect(0,0,210,297,'F')
    pdf.set_y(100); pdf.set_text_color(255,255,255)
    pdf.set_font("Arial","B",32); pdf.cell(0,15,"LOGIFLO.IO",ln=True,align='C')
    pdf.set_font("Arial","",14); pdf.set_text_color(200,200,200)
    pdf.cell(0,10,"AUDIT STRATEGIQUE",ln=True,align='C')
    pdf.ln(30); pdf.set_text_color(255,255,255); pdf.set_font("Arial","B",20)
    clean=unicodedata.normalize('NFKD',title).encode('ASCII','ignore').decode('utf-8')
    pdf.cell(0,10,clean,ln=True,align='C'); pdf.ln(10)
    pdf.set_font("Arial","",12)
    pdf.cell(0,10,f"Date : {datetime.date.today().strftime('%d/%m/%Y')}",ln=True,align='C')
    pdf.cell(0,10,"Statut : CONFIDENTIEL",ln=True,align='C')
    pdf.add_page()
    pdf.set_fill_color(240,244,248); pdf.rect(0,0,210,30,'F')
    pdf.set_y(10); pdf.set_text_color(11,37,69); pdf.set_font("Arial","B",18)
    pdf.cell(0,10,"RAPPORT D'ANALYSE",ln=True,align='L')
    pdf.line(10,25,200,25); pdf.ln(15)
    if figs:
        for fig in figs:
            try:
                img_bytes=fig.to_image(format="png",width=800,height=350)
                with tempfile.NamedTemporaryFile(delete=False,suffix=".png") as tmp:
                    tmp.write(img_bytes); tmp_path=tmp.name
                pdf.image(tmp_path,x=15,y=pdf.get_y()+2,w=180); pdf.ln(95)
            except: pass
    if pdf.get_y()>220: pdf.add_page()
    content=(content.replace("\u2019","'").replace("\u2018","'")
             .replace("\u201c",'"').replace("\u201d",'"')
             .replace("\u20ac","EUR").replace("\u2022","-"))
    for line in content.split('\n'):
        line=line.strip()
        if not line: pdf.ln(4); continue
        if line.startswith('### '):
            t=unicodedata.normalize('NFKD',line[4:]).encode('ASCII','ignore').decode('utf-8')
            pdf.ln(6); pdf.set_font("Arial","BU",12); pdf.set_text_color(11,37,69)
            pdf.cell(0,8,t.upper(),ln=True)
            pdf.set_font("Arial","",11); pdf.set_text_color(40,40,40)
        else:
            b=unicodedata.normalize('NFKD',line.replace("**","")).encode('ASCII','ignore').decode('utf-8')
            pdf.multi_cell(0,6,b)
    return pdf.output(dest='S').encode('latin-1')

# =========================================
# 5. SMART INGESTER STOCK (prix et conso optionnels)
# =========================================
def nettoyer(t):
    t=str(t).lower()
    t=unicodedata.normalize('NFD',t).encode('ascii','ignore').decode("utf-8")
    return re.sub(r'[^a-z0-9]','',t)

def smart_ingester_stock_ultime(df):
    # Nettoie les lignes vides et les en-têtes parasites
    df = df.dropna(how='all').copy()
    df = df[df.apply(lambda r: r.astype(str).str.strip().ne('').any(), axis=1)]

    propres = {col: nettoyer(col) for col in df.columns}
    cibles = {
        "reference":     ["reference","ref","article","code","sku","ean","produit",
                          "designation","nom","item","cable","cablage","libelle","description"],
        "quantite":      ["quantite","qte","qty","stock","stk","volume","pieces",
                          "units","restant","metre","meter","bobine","disponible"],
        "prix_unitaire": ["prix","price","cout","cost","valeur","pmp","tarif","montant","pu","achat"],
        "conso_an1":     ["conso2023","conso2024","conso1","consommation2023","consommation2024"],
        "conso_an2":     ["conso2024","conso2025","conso2","consommation2024","consommation2025"],
        "conso_an3":     ["conso2025","conso2026","conso3","consommation2025","consommation2026"],
    }

    # Détection intelligente des colonnes de consommation par année dans le nom
    for col in df.columns:
        propre = nettoyer(col)
        if "2023" in propre and "conso_an1" not in {v: k for k,v in propres.items()}:
            propres[col] = "conso_an1_detect"
        if "2024" in propre and "conso_an2" not in {v: k for k,v in propres.items()}:
            propres[col] = "conso_an2_detect"
        if "2025" in propre and "conso_an3" not in {v: k for k,v in propres.items()}:
            propres[col] = "conso_an3_detect"

    trouvees = {}
    for std, syns in cibles.items():
        for orig, propre in propres.items():
            if orig in trouvees: continue
            if any(s == propre or (len(s) >= 3 and s in propre) for s in syns):
                trouvees[orig] = std; break
            if difflib.get_close_matches(propre, syns, n=1, cutoff=0.82):
                trouvees[orig] = std

    # Détection par position si les noms ne matchent pas
    cols = list(df.columns)
    if "reference" not in trouvees.values() and len(cols) >= 1:
        trouvees[cols[0]] = "reference"
    if "quantite" not in trouvees.values() and len(cols) >= 2:
        for c in cols[1:]:
            sample = pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.]','',regex=True), errors='coerce')
            if sample.notna().sum() > len(df) * 0.5:
                trouvees[c] = "quantite"; break

    df = df.rename(columns=trouvees)

    # Référence obligatoire, quantité obligatoire, prix OPTIONNEL
    manq = [c for c in ["reference","quantite"] if c not in df.columns]
    if manq:
        return None, f"Colonnes introuvables : {', '.join(manq)}. Vérifiez que votre fichier contient bien une colonne article/référence et une colonne quantité."

    # Nettoyage quantité
    df["quantite"] = df["quantite"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.')
    df["quantite"] = pd.to_numeric(df["quantite"], errors='coerce')
    df = df.dropna(subset=["quantite"]).copy()

    # Filtre les lignes sans référence valide
    df = df[df["reference"].astype(str).str.strip().ne('')]
    df = df[~df["reference"].astype(str).str.lower().isin(['nan','none',''])]

    # Prix optionnel
    if "prix_unitaire" not in df.columns:
        df["prix_unitaire"] = 0.0
        df["_sans_prix"] = True
    else:
        df["prix_unitaire"] = pd.to_numeric(
            df["prix_unitaire"].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
            errors='coerce').fillna(0)
        df["_sans_prix"] = (df["prix_unitaire"] == 0).all()

    # Consommations historiques optionnelles
    has_conso = False
    conso_cols = []
    for c in ["conso_an1","conso_an2","conso_an3"]:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(r'[^\d.,-]','',regex=True).str.replace(',','.'),
                errors='coerce').fillna(0)
            conso_cols.append(c)
            has_conso = True

    df["_has_conso"] = has_conso

    # Calcul conso moyenne si disponible
    if has_conso:
        df["_conso_moy"] = df[conso_cols].mean(axis=1)
        df["_conso_moy"] = df["_conso_moy"].fillna(0)
    else:
        df["_conso_moy"] = 0.0

    return df.copy(), "Succès"

# =========================================
# 6. AUTO MAP TRANSPORT
# =========================================
def auto_map_columns_with_ai(df):
    titres = list(df.columns)
    profil = {col: {"exemples": list(df[col].dropna().astype(str).unique()[:5])} for col in titres}
    prompt = f"""Titres: {titres}\nDonnées: {json.dumps(profil, ensure_ascii=False)}
Associe à un titre EXACT. Si absent: null.
Concepts: "client","ca","co","dep","arr","dist","poids".
JSON uniquement."""
    try:
        r = client.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":prompt}],temperature=0.0)
        raw = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        return {k: v for k, v in json.loads(raw).items() if v in titres}
    except:
        return {"client": titres[0], "ca": titres[1] if len(titres)>1 else None, "co": None}

# =========================================
# 7. GÉNÉRATION IA (routeur selon profil)
# =========================================
def generate_ai_analysis(data_summary):
    if st.session_state.module == "transport":
        prompt = PROMPT_TRANSPORT
    elif st.session_state.get("stock_view") == "TERRAIN":
        prompt = PROMPT_TERRAIN
    else:
        prompt = PROMPT_STOCK
    try:
        r = client.chat.completions.create(model="gpt-4o-mini",
            messages=[
                {"role":"system","content":prompt},
                {"role":"user","content":f"Métriques : {data_summary}. Rédige l'audit."}
            ], temperature=0.3)
        texte = r.choices[0].message.content
        try: return texte.encode('latin-1').decode('utf-8')
        except: return texte
    except Exception as e:
        return f"Erreur IA : {str(e)}"

# =========================================
# 8. ROUTING ORS
# =========================================
def calculate_haversine(lon1,lat1,lon2,lat2):
    R=6371.0
    dlat,dlon=math.radians(lat2-lat1),math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

def fetch_geo(city,_token=None):
    if not city or str(city).strip() in ("","nan","None"): return city,None
    try:
        r=requests.get("https://nominatim.openstreetmap.org/search",
            params={"q":str(city).strip(),"format":"json","limit":1},
            headers={"User-Agent":"Logiflo.io/2.0"},timeout=5)
        if r.status_code==200:
            d=r.json()
            if d: return city,[float(d[0]["lon"]),float(d[0]["lat"])]
    except: pass
    return city,None

def geocode_cities_mapbox(cities):
    villes=[c for c in set(str(v) for v in cities)
            if c not in st.session_state.geo_cache and c not in ("","nan","None")]
    if villes:
        bar=st.progress(0,text="📍 Géocodage des villes...")
        for i,city in enumerate(villes):
            _,coord=fetch_geo(city)
            if coord: st.session_state.geo_cache[city]=coord
            time.sleep(1.1)
            bar.progress((i+1)/len(villes),text=f"📍 Géocodage... ({i+1}/{len(villes)})")
        bar.empty()
    return {c:st.session_state.geo_cache[c] for c in set(str(v) for v in cities) if c in st.session_state.geo_cache}

@st.cache_data(show_spinner=False)
def _ors_distance(lon1,lat1,lon2,lat2):
    for profile in ["driving-hgv","driving-car"]:
        try:
            r=requests.post(f"https://api.openrouteservice.org/v2/directions/{profile}",
                json={"coordinates":[[lon1,lat1],[lon2,lat2]],"instructions":False},
                headers={"Accept":"application/json","Content-Type":"application/json","Authorization":ORS_API_KEY},
                timeout=6)
            if r.status_code==200:
                return r.json()["routes"][0]["summary"]["distance"]/1000.0
        except: continue
    return None

def fetch_route(dep,arr,mode,coords,_token=None):
    c1,c2=coords.get(str(dep)),coords.get(str(arr))
    if not c1 or not c2: return (dep,arr,mode),0.0
    lon1,lat1=c1; lon2,lat2=c2
    dist_vol=calculate_haversine(lon1,lat1,lon2,lat2)
    m=str(mode).lower()
    if any(k in m for k in ["mer","sea","maritime","bateau","port","ferry"]): return (dep,arr,mode),dist_vol*1.25
    elif any(k in m for k in ["air","avion","aérien","aerien","flight"]): return (dep,arr,mode),dist_vol*1.05
    elif any(k in m for k in ["fer","rail","train","sncf","ferroviaire"]): return (dep,arr,mode),dist_vol*1.15
    else:
        d=_ors_distance(lon1,lat1,lon2,lat2)
        return (dep,arr,mode),(d if d and d>0 else dist_vol*1.30)

def smart_multimodal_router(df,dep_col,arr_col,mode_col=None):
    coords=geocode_cities_mapbox(pd.concat([df[dep_col],df[arr_col]]).dropna().unique())
    uniq=[]
    for _,row in df.iterrows():
        dep=row[dep_col]; arr=row[arr_col]
        mode=str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"
        k=(dep,arr,mode)
        if k not in st.session_state.route_cache and k not in uniq: uniq.append(k)
    if uniq:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            for key,dist in [f.result() for f in concurrent.futures.as_completed(
                [ex.submit(fetch_route,r[0],r[1],r[2],coords) for r in uniq])]:
                st.session_state.route_cache[key]=dist
    df["_DIST_CALCULEE"]=[
        st.session_state.route_cache.get(
            (row[dep_col],row[arr_col],
             str(row[mode_col]).lower() if mode_col and pd.notna(row.get(mode_col)) else "route"),0.0)
        for _,row in df.iterrows()]
    return df

def super_clean(val):
    if pd.isna(val): return 0.0
    try: return float(str(val).replace('€','').replace('$','').replace('EUR','').replace(' ','').replace('\xa0','').replace(',','.'))
    except: return 0.0

# =========================================
# 9. PAGES
# =========================================

if st.session_state.page=="accueil":
    st.markdown("<h1 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;font-weight:800;letter-spacing:-1px;'>LOGIFLO.IO</h1>",unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;font-size:1.1em;color:#4A6080;'>Plateforme d'Intelligence Logistique et d'Optimisation Financière</p><br>",unsafe_allow_html=True)
    c1,c2=st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📦</span>",unsafe_allow_html=True)
        if st.button("AUDIT STOCKS",use_container_width=True):
            st.session_state.module="stock"; st.session_state.page="choix_profil_stock"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>🌍</span>",unsafe_allow_html=True)
        if st.button("AUDIT TRANSPORT",use_container_width=True):
            st.session_state.module="transport"; st.session_state.page="login"; st.rerun()
    st.markdown("<br><br>",unsafe_allow_html=True)
    _,cm,_=st.columns([1,1,1])
    if cm.button("DEMANDER UN ACCÈS PRIVÉ",use_container_width=True):
        st.session_state.page="contact"; st.rerun()

elif st.session_state.page=="contact":
    st.markdown("<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>Demande d'Accès Réservé</h2>",unsafe_allow_html=True)
    _,cc,_=st.columns([1,1.5,1])
    with cc:
        with st.form("vip"):
            st.text_input("Nom & Prénom"); st.text_input("Email Professionnel"); st.text_input("Entreprise")
            st.selectbox("Volume géré :",["Moins de 10M EUR","De 10M à 50M EUR","Plus de 50M EUR"])
            st.selectbox("Enjeu prioritaire :",["Optimisation BFR (Stocks)","Réduction coûts Transport","Global Supply Chain"])
            if st.form_submit_button("Transmettre",use_container_width=True):
                st.success("Demande transmise. Notre équipe vous contactera sous 24h.")
        if st.button("← Retour",use_container_width=True): st.session_state.page="accueil"; st.rerun()

elif st.session_state.page=="choix_profil_stock":
    st.markdown("<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>Sélectionnez votre Espace de Travail</h2>",unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:#4A6080;'>L'interface s'adaptera à vos habilitations.</p><br><br>",unsafe_allow_html=True)
    c1,c2=st.columns(2)
    with c1:
        st.markdown("<span class='big-emoji'>📊</span>",unsafe_allow_html=True)
        if st.button("PROFIL MANAGER (Stratégie & Finance)",use_container_width=True):
            st.session_state.stock_view="MANAGER"; st.session_state.page="login"; st.rerun()
    with c2:
        st.markdown("<span class='big-emoji'>👷</span>",unsafe_allow_html=True)
        if st.button("PROFIL TERRAIN (Action Opérationnelle)",use_container_width=True):
            st.session_state.stock_view="TERRAIN"; st.session_state.page="login"; st.rerun()

elif st.session_state.page=="login":
    st.markdown(f"<h2 style='text-align:center;color:#0B2545;font-family:Syne,sans-serif;'>Accès Sécurisé — Module {st.session_state.module.upper()}</h2><br>",unsafe_allow_html=True)
    _,cl,_=st.columns([1,1.2,1])
    with cl:
        with st.form("login_form"):
            u=st.text_input("Identifiant")
            p=st.text_input("Mot de passe",type="password")
            st.markdown("<br>",unsafe_allow_html=True)
            if st.form_submit_button("Connexion",use_container_width=True):
                if u in USERS_DB and USERS_DB[u]==p:
                    st.session_state.auth=True
                    st.session_state.current_user=u
                    st.session_state.page="app"; st.rerun()
                else: st.error("Identifiants incorrects.")
        if st.button("← Retour",use_container_width=True): st.session_state.page="accueil"; st.rerun()

elif st.session_state.auth and st.session_state.page=="app":

    with st.sidebar:
        st.markdown(f"""
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
                <div class="sidebar-logo">LOGI<span>FLO</span>.IO</div>
                <div style="font-size:20px;line-height:1.2;">📦<br>📦📦</div>
            </div>
            <div style="font-size:12px;color:#4A6080;margin-bottom:20px;">
                👤 Connecté : <b style="color:white;">{st.session_state.current_user}</b>
            </div>
        """,unsafe_allow_html=True)
        st.markdown("---")
        nav=st.radio("NAVIGATION",["Espace de Travail","Archives","Paramètres","Informations Légales"])
        st.markdown("---")
        if st.button("Déconnexion",use_container_width=True): st.session_state.clear(); st.rerun()
        st.markdown("<div style='margin-top:40px;border-top:1px solid #1e3a5f;padding-top:14px;font-size:11px;color:#4A6080;'>© 2026 Logiflo B2B Enterprise</div>",unsafe_allow_html=True)

    # ── INFORMATIONS LÉGALES ──
    if nav=="Informations Légales":
        st.title("⚖️ Informations Légales")

        tab1, tab2, tab3 = st.tabs(["📋 Mentions Légales", "🔒 Politique de Confidentialité", "📄 CGUV"])

        with tab1:
            st.markdown("""
            <div class="legal-text">
            <h2>1. ÉDITEUR DE LA PLATEFORME</h2>
            <div class="legal-box">
            <p><strong>Logiflo B2B Enterprise</strong><br>
            Forme juridique : SASU (en cours d'immatriculation)<br>
            Siège social : Marseille, France<br>
            Responsable de la publication : Eric [NOM]<br>
            Email : contact@logiflo.io<br>
            Application : https://logiflo-io.streamlit.app<br>
            Site vitrine : https://rikostar899.github.io/logiflo</p>
            </div>

            <h2>2. HÉBERGEMENT</h2>
            <p><strong>Application LOGIFLO.IO</strong><br>
            Streamlit Cloud — Snowflake Inc.<br>
            106 E Babcock St, Bozeman, MT 59715, USA</p>
            <p><strong>Site vitrine</strong><br>
            GitHub Pages — GitHub Inc. (Microsoft Corporation)<br>
            88 Colin P Kelly Jr St, San Francisco, CA 94107, USA</p>

            <h2>3. PROPRIÉTÉ INTELLECTUELLE</h2>
            <p>L'ensemble des éléments de la plateforme LOGIFLO.IO (code source, algorithmes, interface graphique, 
            Smart Ingester™, cerveaux IA, structure des données) sont la propriété exclusive de Logiflo B2B Enterprise 
            et protégés par le Code de la propriété intellectuelle.</p>
            <p>Toute reproduction sans autorisation écrite préalable est interdite sous peine de poursuites 
            (articles L. 335-2 et suivants du CPI).</p>

            <h2>4. LIMITATION DE RESPONSABILITÉ</h2>
            <p>Les analyses générées par la plateforme sont fournies à titre indicatif et constituent un support 
            à la décision uniquement. Logiflo ne saurait être tenu responsable des décisions prises sur cette base.</p>

            <h2>5. DROIT APPLICABLE</h2>
            <p>Les présentes mentions sont régies par le droit français. Tout litige relève de la compétence 
            des juridictions françaises.</p>

            <h2>6. CRÉDITS TECHNIQUES</h2>
            <ul>
            <li>Intelligence Artificielle : OpenAI GPT-4o-mini</li>
            <li>Calcul d'itinéraires : OpenRouteService (HeiGIT, Université de Heidelberg)</li>
            <li>Géocodage : Nominatim / OpenStreetMap contributors</li>
            <li>Framework : Streamlit (Snowflake Inc.)</li>
            </ul>
            <p style="color:#4A6080;font-size:13px;margin-top:20px;"><em>Dernière mise à jour : Avril 2026</em></p>
            </div>
            """, unsafe_allow_html=True)

        with tab2:
            st.markdown("""
            <div class="legal-text">
            <div class="legal-box">
            <p>Cette politique s'applique à la plateforme LOGIFLO.IO et au site vitrine associé.<br>
            Conforme au Règlement (UE) 2016/679 (RGPD) et à la loi Informatique et Libertés.</p>
            </div>

            <h2>1. RESPONSABLE DU TRAITEMENT</h2>
            <p>Logiflo B2B Enterprise — contact@logiflo.io — Marseille, France</p>

            <h2>2. DONNÉES COLLECTÉES</h2>
            <p><strong>Données de connexion :</strong> identifiant, date/heure de connexion, module utilisé.</p>
            <p><strong>Données métier (Zero Data Retention) :</strong> les fichiers CSV/Excel téléversés sont traités 
            exclusivement en mémoire vive et purgés automatiquement à la déconnexion.</p>
            <p><strong>Données d'archive (sur action volontaire) :</strong> si vous sauvegardez un audit, 
            les métriques agrégées, le résumé IA et le PDF sont stockés dans votre espace personnel uniquement.</p>

            <h2>3. POLITIQUE ZERO DATA RETENTION</h2>
            <div class="legal-box">
            <p>✅ Vos fichiers bruts ne sont JAMAIS stockés de manière permanente<br>
            ✅ Vos données ne sont JAMAIS revendues ou transmises à des tiers<br>
            ✅ Vos données ne sont JAMAIS utilisées pour entraîner des modèles IA publics<br>
            ✅ Purge automatique à chaque déconnexion</p>
            </div>

            <h2>4. SOUS-TRAITANTS</h2>
            <ul>
            <li><strong>Streamlit Cloud (Snowflake)</strong> — hébergement — USA (clauses contractuelles types UE)</li>
            <li><strong>OpenAI</strong> — génération des analyses IA — USA (DPA conforme RGPD)</li>
            <li><strong>Google Sheets</strong> — archivage sur demande — UE/USA</li>
            <li><strong>OpenRouteService</strong> — calcul distances — Allemagne (UE)</li>
            </ul>

            <h2>5. VOS DROITS (RGPD)</h2>
            <p>Conformément aux articles 15 à 22 du RGPD, vous disposez des droits d'accès, rectification, 
            effacement, limitation, portabilité et opposition.</p>
            <p>Pour exercer vos droits : <strong>contact@logiflo.io</strong> — Réponse sous 30 jours.<br>
            Réclamation auprès de la CNIL : <strong>www.cnil.fr</strong></p>

            <h2>6. COOKIES</h2>
            <p>La plateforme n'utilise pas de cookies publicitaires. Seuls des cookies techniques 
            strictement nécessaires au fonctionnement de l'authentification sont utilisés.</p>

            <h2>7. SÉCURITÉ</h2>
            <ul>
            <li>Chiffrement HTTPS des communications (TLS 1.3)</li>
            <li>Authentification par identifiants personnels</li>
            <li>Isolation des sessions utilisateurs</li>
            <li>Clés API stockées dans un coffre-fort sécurisé</li>
            <li>Aucun stockage des fichiers bruts après traitement</li>
            </ul>
            <p style="color:#4A6080;font-size:13px;margin-top:20px;"><em>Dernière mise à jour : Avril 2026</em></p>
            </div>
            """, unsafe_allow_html=True)

        with tab3:
            st.markdown("""
            <div class="legal-text">
            <p>Les Conditions Générales d'Utilisation et de Vente complètes (15 articles) sont disponibles 
            sur demande à <strong>contact@logiflo.io</strong></p>

            <h2>RÉSUMÉ DES POINTS CLÉS</h2>

            <h2>Accès et sécurité</h2>
            <p>L'accès est réservé aux professionnels B2B accrédités. Les identifiants sont personnels et confidentiels. 
            Chaque session est techniquement isolée.</p>

            <h2>Nature des recommandations IA</h2>
            <div class="legal-box">
            <p>⚠️ Les audits et recommandations générés par Logiflo sont fournis à titre de <strong>support à la décision uniquement</strong>. 
            Ils ne constituent pas un conseil financier, juridique ou comptable. 
            Le Client demeure le seul et unique décisionnaire.</p>
            </div>

            <h2>Propriété des données</h2>
            <p>Le Client reste propriétaire de l'intégralité de ses données. Logiflo n'acquiert aucun droit 
            de propriété sur les données téléversées. Les rapports générés appartiennent au Client.</p>

            <h2>Responsabilité</h2>
            <p>La responsabilité de Logiflo est limitée aux sommes versées au cours des 12 derniers mois. 
            Logiflo ne saurait être tenu responsable des pertes indirectes ou des décisions prises sur la base 
            des analyses générées.</p>

            <h2>Loi applicable</h2>
            <p>Droit français. Tribunaux de Commerce de Marseille compétents.</p>

            <p style="color:#4A6080;font-size:13px;margin-top:20px;"><em>Version 1.0 — Avril 2026 — 
            Document complet disponible sur demande à contact@logiflo.io</em></p>
            </div>
            """, unsafe_allow_html=True)

    # ── ARCHIVES ──
    elif nav=="Archives":
        st.title("🗄️ Archives & Historique")
        st.markdown(f"Historique des audits du compte **{st.session_state.current_user}**")
        st.markdown("---")
        with st.spinner("Chargement de vos archives..."):
            df_arch=load_archives_from_sheets(st.session_state.current_user)
        if df_arch is None:
            st.warning("⚠️ Connexion Google Sheets non disponible. Vérifiez la configuration dans les Secrets Streamlit.")
        elif df_arch.empty:
            st.info("Aucun audit archivé pour le moment. Générez votre premier audit depuis l'Espace de Travail.")
        else:
            cf1,cf2=st.columns(2)
            module_filter=cf1.selectbox("Filtrer par module",["Tous","stock","transport"])
            nb_max=cf2.slider("Nombre d'audits affichés",5,50,10)
            df_show=df_arch.copy()
            if module_filter!="Tous": df_show=df_show[df_show["module"]==module_filter]
            df_show=df_show.iloc[::-1].head(nb_max)
            st.markdown(f"**{len(df_show)} audit(s) affiché(s)**")
            st.markdown("<br>",unsafe_allow_html=True)
            for _,row in df_show.iterrows():
                module_icon="📦" if row.get("module")=="stock" else "🚚"
                st.markdown(f"""
                <div class="archive-card">
                    <h4>{module_icon} Audit {str(row.get('module','')).upper()} — {row.get('date','')} à {row.get('heure','')}</h4>
                    <div class="meta">{row.get('nb_lignes','')} lignes analysées</div>
                    <span class="archive-kpi">{row.get('kpi_label_1','')}: {row.get('kpi_1','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_2','')}: {row.get('kpi_2','')}</span>
                    <span class="archive-kpi">{row.get('kpi_label_3','')}: {row.get('kpi_3','')}</span>
                </div>
                """,unsafe_allow_html=True)
                with st.expander("📋 Voir le résumé IA"):
                    resume=row.get("resume_ia","")
                    if resume:
                        mode = "terrain" if "terrain" in str(row.get("module","")).lower() else "manager"
                        st.markdown(render_report(str(resume), mode), unsafe_allow_html=True)
                    else:
                        st.info("Résumé non disponible.")
                pdf_b64=row.get("pdf_base64","")
                if pdf_b64:
                    try:
                        pdf_bytes=base64.b64decode(str(pdf_b64))
                        st.download_button(
                            label="📥 Télécharger le PDF",
                            data=pdf_bytes,
                            file_name=f"Logiflo_{row.get('date','').replace('/','_')}_{row.get('module','')}.pdf",
                            key=f"dl_{row.get('date','')}_{row.get('heure','')}_{row.get('module','')}",
                            use_container_width=True)
                    except: pass

    # ── PARAMÈTRES ──
    elif nav=="Paramètres":
        st.title("⚙️ Configuration des Seuils")
        if st.session_state.module=="stock":
            st.session_state.seuil_bas=st.slider("Seuil d'Alerte",0,100,st.session_state.seuil_bas)
            st.session_state.seuil_rupture=st.slider("Seuil de Rupture Critique",0,10,st.session_state.seuil_rupture)
        else:
            st.session_state.seuil_km=st.slider("Seuil Rentabilité (EUR/KM)",0,1000,st.session_state.seuil_km)

    # ── ESPACE DE TRAVAIL ──
    elif nav=="Espace de Travail":

        # ==========================================
        # MODULE STOCK
        # ==========================================
        if st.session_state.module=="stock":
            st.title("📦 Audit Financier des Stocks")
            ci,cb=st.columns([4,1])
            ci.markdown(f"**Profil Actif : {st.session_state.stock_view}**")
            if cb.button("Changer de profil"): st.session_state.page="choix_profil_stock"; st.rerun()

            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown("""
                <div class='import-card'>
                    <h3>📥 Importation Sécurisée</h3>
                    <p>Déposez votre fichier d'inventaire (CSV ou Excel).<br>
                    Le <b>Smart Ingester™</b> détecte automatiquement vos colonnes.<br>
                    <span style='color:#00A87A;font-weight:600;'>✓ Prix optionnel</span> &nbsp;
                    <span style='color:#00A87A;font-weight:600;'>✓ Consommations optionnelles</span> &nbsp;
                    <span style='color:#00A87A;font-weight:600;'>✓ Tous formats acceptés</span></p>
                </div>
            """,unsafe_allow_html=True)

            up=st.file_uploader("",type=["csv","xlsx"],key="stock_upload")
            st.markdown("---")

            if up:
                try:
                    df_brut = pd.read_excel(up) if up.name.endswith("xlsx") else pd.read_csv(up, encoding='utf-8')
                except UnicodeDecodeError:
                    up.seek(0)
                    df_brut = pd.read_csv(up, encoding='latin-1')
                except Exception:
                    up.seek(0)
                    df_brut = pd.read_csv(up, sep=';', encoding='latin-1')

                with st.spinner("⏳ Analyse en cours..."):
                    df_propre,statut=smart_ingester_stock_ultime(df_brut)
                if df_propre is None:
                    st.error(statut)
                else:
                    st.session_state.df_stock=df_propre

            if st.session_state.df_stock is not None:
                df=st.session_state.df_stock.copy()
                sans_prix = bool(df.get("_sans_prix", pd.Series([True])).iloc[0]) if "_sans_prix" in df.columns else True
                has_conso = bool(df.get("_has_conso", pd.Series([False])).iloc[0]) if "_has_conso" in df.columns else False

                # Badge informatif
                badges = []
                if sans_prix:
                    badges.append("📊 Mode opérationnel — analyse sans prix")
                if has_conso:
                    badges.append("📈 Historique de consommation détecté")
                else:
                    badges.append("⚠️ Pas d'historique conso — couverture non calculable")
                for b in badges:
                    st.markdown(f"<span class='sans-prix-badge'>{b}</span>",unsafe_allow_html=True)

                # Calcul statuts
                if has_conso:
                    df["_conso_moy"] = df["_conso_moy"].fillna(0)
                    df["Couverture_mois"] = np.where(
                        df["_conso_moy"] > 0,
                        df["quantite"] / df["_conso_moy"],
                        9999
                    )
                    conditions = [
                        (df["quantite"] <= st.session_state.seuil_rupture),
                        (df["quantite"] > 0) & (df["_conso_moy"] == 0),
                        (df["quantite"] > 0) & (df["Couverture_mois"] > 6)
                    ]
                    choix = ["🚨 RUPTURE","🔴 Dormant","🟠 Surstock (>6 mois)"]
                    df["Statut"] = np.select(conditions, choix, default="✅ Sain")
                else:
                    df["Statut"] = np.where(
                        df["quantite"] <= st.session_state.seuil_rupture,
                        "🚨 RUPTURE", "✅ EN STOCK"
                    )

                df["valeur_totale"] = df["quantite"] * df["prix_unitaire"]
                val_totale = df["valeur_totale"].sum()
                ruptures = df[df["Statut"] == "🚨 RUPTURE"]
                tx_serv = (1 - len(ruptures)/len(df)) * 100 if len(df) > 0 else 100

                if not st.session_state.history_stock or st.session_state.history_stock[-1].get("valeur") != val_totale:
                    st.session_state.history_stock.append({
                        "date": datetime.datetime.now().strftime("%H:%M:%S"),
                        "valeur": val_totale
                    })

                # ── VUE MANAGER ──
                if st.session_state.stock_view=="MANAGER":
                    c1,c2,c3=st.columns(3)
                    if not sans_prix:
                        c1.markdown(f"<div class='kpi-card'><h4>Capital Immobilisé</h4><h2 style='color:#0B2545;'>{val_totale:,.0f} €</h2></div>",unsafe_allow_html=True)
                    else:
                        c1.markdown(f"<div class='kpi-card'><h4>Articles en Stock</h4><h2 style='color:#0B2545;'>{len(df)}</h2></div>",unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>Taux de Service</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>",unsafe_allow_html=True)
                    c3.markdown(f"<div class='kpi-card'><h4>Articles en Rupture</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>",unsafe_allow_html=True)

                    st.markdown("<br>",unsafe_allow_html=True)
                    cp,cl2=st.columns(2)
                    cmap={"🚨 RUPTURE":"#E8304A","✅ EN STOCK":"#00C896","✅ Sain":"#00C896",
                          "🔴 Dormant":"#c0392b","🟠 Surstock (>6 mois)":"#f39c12"}
                    with cp:
                        fig_pie=px.pie(df,names="Statut",hole=0.4,color="Statut",color_discrete_map=cmap)
                        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                        st.plotly_chart(fig_pie,use_container_width=True)
                    with cl2:
                        if has_conso:
                            # Graphique consommation par article (top 15)
                            top15 = df.nlargest(15,"_conso_moy")[["reference","_conso_moy","quantite"]].copy()
                            fig_conso = px.bar(top15, x="reference", y=["quantite","_conso_moy"],
                                barmode="group",
                                color_discrete_map={"quantite":"#0B2545","_conso_moy":"#00C896"},
                                title="Stock vs Conso moyenne (Top 15)")
                            fig_conso.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_conso,use_container_width=True)
                        else:
                            fig_line=px.line(pd.DataFrame(st.session_state.history_stock),x="date",y="valeur")
                            fig_line.update_traces(line_color="#00C896")
                            fig_line.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_line,use_container_width=True)

                    if st.button("GÉNÉRER L'AUDIT FINANCIER (IA)",use_container_width=True):
                        with st.spinner("Analyse approfondie en cours..."):
                            df_tox=df[df["Statut"].isin(["🔴 Dormant","🟠 Surstock (>6 mois)"])]
                            pires=df_tox.nlargest(3,"quantite") if not df_tox.empty else df.nlargest(3,"quantite")
                            top_str=", ".join([f"{r['reference']} (qté: {r['quantite']:.0f})" for _,r in pires.iterrows()])
                            rupt_l=ruptures.nlargest(3,"quantite")["reference"].astype(str).tolist() if not ruptures.empty else "Aucune"

                            # Médiane sectorielle si pas de conso
                            mediane_info = ""
                            if not has_conso:
                                mediane_info = " ANGLE MORT : pas d'historique de consommation fourni. Couverture non calculable. Médiane sectorielle indicative : pour ce type de stock, une couverture saine est généralement de 2 à 4 mois."
                            else:
                                conso_moy_globale = df["_conso_moy"].mean()
                                couverture_moy = df["Couverture_mois"].replace(9999,np.nan).mean()
                                mediane_info = f" Consommation moyenne : {conso_moy_globale:.1f} unités/période. Couverture moyenne : {couverture_moy:.1f} mois."

                            prix_info = "" if sans_prix else f" Capital immobilisé : {val_totale:.0f} EUR."

                            st.session_state.analysis_stock=generate_ai_analysis(
                                f"Articles analysés: {len(df)}. Taux service: {tx_serv:.1f}%. "
                                f"Ruptures: {len(ruptures)}. Top dormants/surstocks: {top_str}. "
                                f"Top ruptures: {rupt_l}.{prix_info}{mediane_info}"
                                f" Prix disponibles: {'Non' if sans_prix else 'Oui'}. "
                                f"Consommations historiques: {'Oui' if has_conso else 'Non'}.")

                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"manager"),unsafe_allow_html=True)
                        st.markdown("<br>",unsafe_allow_html=True)
                        figs_pdf = [fig_pie]
                        if has_conso: figs_pdf.append(fig_conso)
                        pdf_data=generate_expert_pdf("AUDIT STRATEGIQUE DES STOCKS",st.session_state.analysis_stock,figs_pdf)
                        st.session_state.last_pdf=pdf_data
                        kpi1 = val_totale if not sans_prix else float(len(df))
                        label1 = "Capital EUR" if not sans_prix else "Nb articles"
                        st.session_state.last_kpis=[kpi1,tx_serv,len(ruptures)]
                        st.session_state.last_labels=[label1,"Taux service %","Ruptures"]
                        col_dl,col_save=st.columns(2)
                        with col_dl:
                            st.download_button("📥 Télécharger le Rapport (PDF)",pdf_data,"Audit_Stock_Logiflo.pdf",use_container_width=True)
                        with col_save:
                            if st.button("💾 Sauvegarder dans mes Archives",use_container_width=True):
                                ok=save_audit_to_sheets(st.session_state.current_user,"stock",len(df),
                                    st.session_state.last_kpis,st.session_state.last_labels,
                                    st.session_state.analysis_stock,pdf_data)
                                if ok: st.success("✅ Audit sauvegardé !")
                                else: st.warning("⚠️ Sauvegarde impossible — vérifiez la config Google Sheets.")

                # ── VUE TERRAIN ──
                elif st.session_state.stock_view=="TERRAIN":
                    c1,c2=st.columns(2)
                    c1.markdown(f"<div class='kpi-card'><h4>À Réapprovisionner</h4><h2 style='color:#E8304A;'>{len(ruptures)}</h2></div>",unsafe_allow_html=True)
                    c2.markdown(f"<div class='kpi-card'><h4>Disponibilité</h4><h2 style='color:#00C896;'>{tx_serv:.1f} %</h2></div>",unsafe_allow_html=True)

                    st.markdown("### 🚨 Priorités immédiates")
                    if len(ruptures)>0:
                        cols_show = ["reference","quantite","Statut"]
                        if has_conso: cols_show.append("_conso_moy")
                        st.dataframe(ruptures[cols_show],use_container_width=True)
                    else:
                        st.success("✅ Aucun article en rupture.")

                    if st.button("GÉNÉRER L'ANALYSE TERRAIN (IA)",use_container_width=True):
                        with st.spinner("Analyse en cours..."):
                            top_critiques = df.nsmallest(5,"quantite")
                            top_str = ", ".join([f"{r['reference']} ({r['quantite']:.0f})" for _,r in top_critiques.iterrows()])
                            dormants_str = "Non calculable (pas d'historique)"
                            if has_conso:
                                dormants = df[df["_conso_moy"]==0]
                                dormants_str = f"{len(dormants)} articles sans mouvement"
                            mediane = " Pas d'historique de consommation disponible." if not has_conso else f" Conso moyenne globale : {df['_conso_moy'].mean():.1f} unités/période."
                            st.session_state.analysis_stock=generate_ai_analysis(
                                f"Stock terrain : {len(df)} références. Ruptures : {len(ruptures)}. "
                                f"Plus faibles stocks : {top_str}. Dormants : {dormants_str}.{mediane}"
                                f" Prix disponibles : {'Non' if sans_prix else 'Oui'}.")

                    if st.session_state.analysis_stock:
                        st.markdown(render_report(st.session_state.analysis_stock,"terrain"),unsafe_allow_html=True)
                        st.markdown("<br>",unsafe_allow_html=True)
                        st.markdown("### 📋 Stock complet")
                        cols_show = ["reference","quantite","Statut"]
                        if has_conso: cols_show.append("_conso_moy")
                        st.dataframe(df[cols_show],use_container_width=True,height=400)

        # ==========================================
        # MODULE TRANSPORT
        # ==========================================
        elif st.session_state.module=="transport":
            st.title("🚚 Audit de Rentabilité Transport")
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown("""
                <div class='import-card'>
                    <h3>🌍 Importation des Flux de Transport</h3>
                    <p>Déposez votre fichier TMS ou Excel. Le moteur <b>ORS</b> calcule les distances routières réelles.<br>
                    <b>Conseil :</b> incluez une colonne <b>poids (kg)</b> pour activer l'analyse de remplissage.<br>
                    <span style='color:#00A87A;font-weight:600;'>✓ Zero Data Retention</span> | Formats : .CSV, .XLSX</p>
                </div>
            """,unsafe_allow_html=True)

            up_t=st.file_uploader("",type=["csv","xlsx"],key="trans_upload")
            st.markdown("---")

            if up_t:
                if st.session_state.trans_filename!=up_t.name:
                    try: df_t=pd.read_excel(up_t) if up_t.name.endswith("xlsx") else pd.read_csv(up_t,encoding='utf-8')
                    except UnicodeDecodeError:
                        up_t.seek(0); df_t=pd.read_csv(up_t,encoding='latin-1')
                    with st.spinner("⏳ Détection des colonnes..."):
                        mapping=auto_map_columns_with_ai(df_t)
                    st.session_state.trans_mapping=mapping
                    st.session_state.df_trans=df_t
                    st.session_state.trans_filename=up_t.name

            if st.session_state.df_trans is not None:
                df_t=st.session_state.df_trans
                mapping=st.session_state.trans_mapping
                def col(k): return mapping.get(k) if mapping.get(k) in df_t.columns else None
                tour_c=col("client") or df_t.columns[0]
                dep_c=col("dep"); arr_c=col("arr"); dist_c=col("dist")
                mode_c=col("mode"); ca_c=col("ca"); co_c=col("co"); poids_c=col("poids")

                if not co_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["coût","cout","cost","achat"]): co_c=c; break
                if not ca_c:
                    for c in df_t.columns:
                        if any(k in str(c).lower() for k in ["ca","revenue","revenu","facture"]): ca_c=c; break
                if not co_c: st.error("🚨 Colonne 'Coût' introuvable."); st.stop()

                df_t["_CO"]=df_t[co_c].apply(super_clean)
                if ca_c: df_t["_CA"]=df_t[ca_c].apply(super_clean)
                else: df_t["_CA"]=df_t["_CO"]/0.85; st.warning("💡 CA manquant — estimé à marge 15%.")
                df_t["Marge_Nette"]=df_t["_CA"]-df_t["_CO"]

                if dep_c and arr_c and "_DIST_CALCULEE" not in df_t.columns:
                    with st.spinner("⏳ Calcul des distances ORS..."):
                        df_t=smart_multimodal_router(df_t,dep_c,arr_c,mode_c)
                        st.session_state.df_trans=df_t

                df_t["_DIST_FINALE"]=(df_t["_DIST_CALCULEE"] if "_DIST_CALCULEE" in df_t.columns and df_t["_DIST_CALCULEE"].sum()>0
                                      else (df_t[dist_c].apply(super_clean) if dist_c else 0))
                df_t["Rentabilité_%"]=np.where(df_t["_CA"]>0,df_t["Marge_Nette"]/df_t["_CA"]*100,0)
                df_t["_DS"]=df_t["_DIST_FINALE"].replace(0,1)
                df_t["Cout_KM"]=np.where(df_t["_DIST_FINALE"]>0,df_t["_CO"]/df_t["_DS"],0)

                poids_info=""
                if poids_c:
                    df_t["_POIDS"]=df_t[poids_c].apply(super_clean)
                    df_t["Cout_kg"]=np.where(df_t["_POIDS"]>0,df_t["_CO"]/df_t["_POIDS"].replace(0,1),0)
                    poids_info=f" Poids total: {df_t['_POIDS'].sum():,.0f} kg. Coût moyen/kg: {df_t['Cout_kg'].mean():.3f} EUR."

                marge_tot=df_t["Marge_Nette"].sum()
                ca_tot=df_t["_CA"].sum()
                taux=(marge_tot/ca_tot*100) if ca_tot>0 else 0
                traj_def=len(df_t[df_t["Marge_Nette"]<0])
                cout_km=df_t["Cout_KM"].mean()
                toxiques=df_t[df_t["Marge_Nette"]<(df_t["_CA"]*0.05)]
                fuite=toxiques["_CO"].sum()-toxiques["_CA"].sum()
                nb_tox=len(toxiques)

                c1,c2,c3=st.columns(3)
                c1.markdown(f"<div class='kpi-card'><h4>Marge Nette Globale</h4><h2 style='color:#0B2545;'>{marge_tot:,.0f} €</h2></div>",unsafe_allow_html=True)
                c2.markdown(f"<div class='kpi-card'><h4>Taux de Rentabilité</h4><h2 style='color:#00C896;'>{taux:.1f} %</h2></div>",unsafe_allow_html=True)
                if fuite>0:
                    c3.markdown(f"<div class='kpi-card'><h4>🚨 Fuite de Marge</h4><h2 style='color:#E8304A;'>-{fuite:,.0f} €</h2><p>{nb_tox} trajets toxiques</p></div>",unsafe_allow_html=True)
                else:
                    c3.markdown(f"<div class='kpi-card'><h4>✅ Réseau</h4><h2 style='color:#00C896;'>Sain</h2></div>",unsafe_allow_html=True)

                if poids_c: st.info(f"⚖️ Poids détecté — Coût moyen : **{df_t['Cout_kg'].mean():.3f} €/kg** | Total : **{df_t['_POIDS'].sum():,.0f} kg**")

                st.markdown("<br>",unsafe_allow_html=True)
                df_plot=df_t.sort_values("Marge_Nette")
                df_plot["Statut"]=np.where(df_plot["Rentabilité_%"]<10.0,"Alerte (< 10%)","Sain (> 10%)")
                fig_trans=px.bar(df_plot,x=tour_c,y="Marge_Nette",color="Statut",
                    color_discrete_map={"Alerte (< 10%)":"#E8304A","Sain (> 10%)":"#00C896"},
                    title="Analyse de Rentabilité par Trajet")
                fig_trans.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_trans,use_container_width=True)

                if st.button("GÉNÉRER L'AUDIT DE RENTABILITÉ (IA)",use_container_width=True):
                    with st.spinner("Analyse approfondie en cours..."):
                        top3=df_t.nsmallest(3,"Marge_Nette")
                        pires_s=", ".join([f"{r[tour_c]} ({r['Marge_Nette']:.0f} EUR)" for _,r in top3.iterrows()]) if not top3.empty else "Aucun"
                        st.session_state.analysis_trans=generate_ai_analysis(
                            f"Trajets: {len(df_t)}. Marge: {marge_tot:.0f} EUR. Taux: {taux:.1f}%. "
                            f"Déficitaires: {traj_def}. Top 3 pires: {pires_s}. Coût/km: {cout_km:.2f} EUR.{poids_info}")

                if st.session_state.analysis_trans:
                    st.markdown(render_report(st.session_state.analysis_trans,"manager"),unsafe_allow_html=True)
                    st.markdown("<br>",unsafe_allow_html=True)
                    pdf_t=generate_expert_pdf("AUDIT FINANCIER TRANSPORT",st.session_state.analysis_trans,[fig_trans])
                    st.session_state.last_kpis=[marge_tot,taux,nb_tox]
                    st.session_state.last_labels=["Marge EUR","Taux %","Trajets toxiques"]
                    col_dl,col_save=st.columns(2)
                    with col_dl:
                        st.download_button("📥 Télécharger le Rapport (PDF)",pdf_t,"Transport_Logiflo.pdf",use_container_width=True)
                    with col_save:
                        if st.button("💾 Sauvegarder dans mes Archives",use_container_width=True):
                            ok=save_audit_to_sheets(st.session_state.current_user,"transport",len(df_t),
                                st.session_state.last_kpis,st.session_state.last_labels,
                                st.session_state.analysis_trans,pdf_t)
                            if ok: st.success("✅ Audit sauvegardé !")
                            else: st.warning("⚠️ Sauvegarde impossible.")
