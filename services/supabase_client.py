# -*- coding: utf-8 -*-
"""
Logiflo - services/supabase_client.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Client Supabase + Google Sheets fallback
Version 6.1 (mai 2026) — fix SHEET_ID env var
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import pandas as pd
import datetime
import base64
import traceback
import os
import gspread
from google.oauth2.service_account import Credentials

try:
    from supabase import create_client as _supa_create
except Exception:
    _supa_create = None

# Lecture SHEET_ID depuis env vars (Render) puis st.secrets
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
if not SHEET_ID:
    try:
        SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID", "")
    except Exception:
        SHEET_ID = ""


def _debug_supabase(msg, err=None):
    """Log interne visible dans les logs Render/Streamlit."""
    try:
        if err:
            print(f"[SUPABASE] {msg} -- {type(err).__name__}: {err}", flush=True)
        else:
            print(f"[SUPABASE] {msg}", flush=True)
    except Exception:
        pass


def get_supabase():
    """Retourne un client Supabase ou None."""
    if _supa_create is None:
        _debug_supabase("Package 'supabase' non importe - verifier requirements.txt")
        return None
    try:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            try:
                url = url or st.secrets["SUPABASE_URL"]
                key = key or st.secrets["SUPABASE_KEY"]
            except Exception:
                try:
                    url = url or st.secrets["supabase"]["url"]
                    key = key or st.secrets["supabase"]["key"]
                except Exception:
                    pass
        if not url:
            _debug_supabase("SUPABASE_URL manquant (env + secrets)")
            return None
        if not key:
            _debug_supabase("SUPABASE_KEY manquant (env + secrets)")
            return None
        _debug_supabase(f"Connexion Supabase -> {url[:40]}...")
        client = _supa_create(url, key)
        _debug_supabase("Client Supabase cree OK")
        return client
    except Exception as e:
        _debug_supabase("Erreur creation client Supabase", e)
        _debug_supabase(traceback.format_exc())
        return None


def save_audit_to_sheets(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
    """Sauvegarde un audit dans Supabase (fallback Google Sheets)."""
    sb = get_supabase()
    if not sb:
        return _save_audit_sheets_fallback(
            username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes
        )
    try:
        now = datetime.datetime.now()
        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else ""
        _profil = st.session_state.get("stock_view", "MANAGER").lower()
        payload = {
            "user_id": str(username),
            "created_at": now.isoformat(),
            "module": str(module),
            "nb_lignes": int(nb_lignes) if nb_lignes else 0,
            "kpi_1": round(float(kpis[0]), 2) if len(kpis) > 0 else 0,
            "kpi_2": round(float(kpis[1]), 2) if len(kpis) > 1 else 0,
            "kpi_3": round(float(kpis[2]), 2) if len(kpis) > 2 else 0,
            "kpi_label_1": str(labels[0]) if len(labels) > 0 else "",
            "kpi_label_2": str(labels[1]) if len(labels) > 1 else "",
            "kpi_label_3": str(labels[2]) if len(labels) > 2 else "",
            "resume_ia": str(resume_ia or "")[:2000],
            "pdf_base64": pdf_b64,
            "profil": _profil,
            "sector_key": str(st.session_state.get("_last_sector_key", "generique")),
        }
        response = sb.table("audits").insert(payload).execute()
        if response and hasattr(response, 'data') and response.data:
            _debug_supabase(f"save_audit OK -> user={username} module={module}")
            return True
        else:
            _debug_supabase(f"save_audit response vide -> {response}")
            return False
    except Exception as e:
        _debug_supabase("save_audit EXCEPTION", e)
        _debug_supabase(traceback.format_exc())
        return False


def load_archives_from_sheets(username):
    """Charge les archives d'un utilisateur depuis Supabase (fallback Sheets)."""
    sb = get_supabase()
    if not sb:
        return _load_archives_sheets_fallback(username)
    try:
        resp = (
            sb.table("audits")
            .select("*")
            .eq("user_id", str(username))
            .order("created_at", desc=False)
            .execute()
        )
        if not resp or not hasattr(resp, 'data') or not resp.data:
            _debug_supabase(f"load_archives vide pour user={username}")
            return pd.DataFrame()
        df = pd.DataFrame(resp.data)
        if "created_at" in df.columns:
            df["date"] = pd.to_datetime(df["created_at"]).dt.strftime("%d/%m/%Y")
            df["heure"] = pd.to_datetime(df["created_at"]).dt.strftime("%H:%M")
        _debug_supabase(f"load_archives OK -> {len(df)} lignes pour user={username}")
        return df
    except Exception as e:
        _debug_supabase("load_archives EXCEPTION", e)
        return _load_archives_sheets_fallback(username)


def load_user_prefs(username):
    """Charge les preferences utilisateur depuis Supabase."""
    sb = get_supabase()
    if not sb:
        return {}
    try:
        resp = (
            sb.table("user_prefs")
            .select("*")
            .eq("user_id", str(username))
            .execute()
        )
        if resp and hasattr(resp, 'data') and resp.data:
            return resp.data[0]
        return {}
    except Exception as e:
        _debug_supabase("load_user_prefs EXCEPTION", e)
        return {}


def save_user_prefs(username, prefs_dict):
    """Sauvegarde les preferences utilisateur dans Supabase."""
    sb = get_supabase()
    if not sb:
        return False
    try:
        prefs_dict["user_id"] = str(username)
        prefs_dict["updated_at"] = datetime.datetime.now().isoformat()
        sb.table("user_prefs").upsert(prefs_dict).execute()
        return True
    except Exception as e:
        _debug_supabase("save_user_prefs EXCEPTION", e)
        return False


def get_gsheet_client():
    """Initialise un client Google Sheets (fallback)."""
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        return gspread.authorize(creds)
    except Exception:
        return None


def _save_audit_sheets_fallback(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
    """Fallback Google Sheets pour sauvegarder un audit."""
    try:
        gc = get_gsheet_client()
        if not gc or not SHEET_ID:
            return False
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet(username)
        except Exception:
            ws = sh.add_worksheet(title=username, rows=1000, cols=12)
        now = datetime.datetime.now()
        ws.append_row([
            now.strftime("%d/%m/%Y"),
            now.strftime("%H:%M"),
            module,
            nb_lignes,
            round(kpis[0], 2) if len(kpis) > 0 else "",
            round(kpis[1], 2) if len(kpis) > 1 else "",
            round(kpis[2], 2) if len(kpis) > 2 else "",
            labels[0] if len(labels) > 0 else "",
            labels[1] if len(labels) > 1 else "",
            labels[2] if len(labels) > 2 else "",
            (resume_ia or "")[:800],
            base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else "",
        ])
        return True
    except Exception:
        return False


def _load_archives_sheets_fallback(username):
    """Fallback Google Sheets pour charger les archives."""
    try:
        gc = get_gsheet_client()
        if not gc or not SHEET_ID:
            return None
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet(username)
        except Exception:
            return pd.DataFrame()
        records = ws.get_all_records()
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        cols = list(df.columns)
        if len(cols) >= 2 and "date" not in df.columns:
            df.rename(columns={cols[0]: "date", cols[1]: "heure"}, inplace=True)
        if len(cols) >= 3 and "module" not in df.columns:
            df.rename(columns={cols[2]: "module"}, inplace=True)
        if len(cols) >= 4 and "nb_lignes" not in df.columns:
            df.rename(columns={cols[3]: "nb_lignes"}, inplace=True)
        col_map = {
            4: "kpi_1", 5: "kpi_2", 6: "kpi_3",
            7: "kpi_label_1", 8: "kpi_label_2", 9: "kpi_label_3",
            10: "resume_ia", 11: "pdf_base64",
        }
        for idx, name in col_map.items():
            if len(cols) > idx and name not in df.columns:
                df.rename(columns={cols[idx]: name}, inplace=True)
        return df
    except Exception:
        return None


def get_historique_audits(username, module, n=6, current_kpis=None, current_labels=None):
    """Recupere l'historique des audits pour comparaison tendancielle."""
    try:
        df = load_archives_from_sheets(username)
        if df is None or df.empty:
            return None
        df = df[df["module"] == module].copy()

        # Filtrer par secteur pour ne pas melanger les contextes
        if "sector_key" in df.columns:
            last_sector = (
                df["sector_key"].dropna().iloc[-1]
                if len(df["sector_key"].dropna()) > 0 else None
            )
            if last_sector:
                df = df[df["sector_key"] == last_sector].copy()

        if len(df) < 2:
            return None

        for col in ["kpi_1", "kpi_2", "kpi_3"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        try:
            df["_dt"] = pd.to_datetime(
                df["date"] + " " + df["heure"],
                format="%d/%m/%Y %H:%M",
                errors="coerce",
            )
            df = df.sort_values("_dt", ascending=False)
        except Exception:
            pass

        recent = df.head(n).iloc[::-1]
        history = []
        for _, row in recent.iterrows():
            history.append({
                "date": row.get("date", "?"),
                "kpi_1": row.get("kpi_1", 0),
                "kpi_2": row.get("kpi_2", 0),
                "kpi_3": row.get("kpi_3", 0),
                "label_1": row.get("kpi_label_1", "KPI1"),
                "label_2": row.get("kpi_label_2", "KPI2"),
                "label_3": row.get("kpi_label_3", "KPI3"),
                "resume": str(row.get("resume_ia", ""))[:400],
            })

        if current_kpis and len(current_kpis) >= 2:
            cl = current_labels or ["KPI1", "KPI2", "KPI3"]
            history.append({
                "date": datetime.date.today().strftime("%d/%m/%Y"),
                "kpi_1": float(current_kpis[0]) if len(current_kpis) > 0 else 0,
                "kpi_2": float(current_kpis[1]) if len(current_kpis) > 1 else 0,
                "kpi_3": float(current_kpis[2]) if len(current_kpis) > 2 else 0,
                "label_1": cl[0] if len(cl) > 0 else "KPI1",
                "label_2": cl[1] if len(cl) > 1 else "KPI2",
                "label_3": cl[2] if len(cl) > 2 else "KPI3",
                "resume": "",
            })

        if len(history) < 2:
            return None

        first = history[0]
        last = history[-1]

        def delta_pct(new, old):
            try:
                new, old = float(new), float(old)
                if old == 0:
                    return None
                return round((new - old) / abs(old) * 100, 1)
            except Exception:
                return None

        return {
            "history": history,
            "n_audits": len(history),
            "first_date": first["date"],
            "last_date": last["date"],
            "delta_1": delta_pct(last["kpi_1"], first["kpi_1"]),
            "delta_2": delta_pct(last["kpi_2"], first["kpi_2"]),
            "delta_3": delta_pct(last["kpi_3"], first["kpi_3"]),
        }
    except Exception:
        return None
# -*- coding: utf-8 -*-
"""
Logiflo - views/onboarding.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Onboarding multi-pages (une question par ecran).
Collecte : secteur, tranche de CA, effectif, localisation.
Ecrit une seule fois dans la table 'organizations' au recap final.
Version 2.0 (juillet 2026) — refonte multi-etapes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st

# ── LISTES DE REPONSES ───────────────────────────────────────────────────
# (icone, libelle FR, libelle EN, valeur stockee)
SECTORS = [
    ("📦", "Distribution / Negoce", "Distribution / Wholesale", "stock_distribution"),
    ("🛒", "Retail / Commerce de detail", "Retail", "stock_retail"),
    ("💊", "Pharma / Sante", "Pharma / Health", "stock_pharma"),
    ("🍎", "Agroalimentaire", "Food & Beverage", "stock_agroalim"),
    ("🏭", "Industrie / Production", "Industry / Manufacturing", "stock_industrie"),
    ("💻", "E-commerce", "E-commerce", "stock_retail"),
    ("🔧", "Autre", "Other", "generique"),
]

REVENUE_BRACKETS = [
    ("Moins de 500K€", "Under 500K€", "<500K"),
    ("500K€ – 2M€", "500K€ – 2M€", "500K-2M"),
    ("2M€ – 10M€", "2M€ – 10M€", "2M-10M"),
    ("Plus de 10M€", "Over 10M€", ">10M"),
    ("Je prefere ne pas dire", "Prefer not to say", "undisclosed"),
]

# (libelle, valeur stockee = borne haute indicative pour tri)
EMPLOYEE_RANGES = [
    ("1 – 9 (TPE)", "1-9", 9),
    ("10 – 49 (PME)", "10-49", 49),
    ("50 – 249 (PME)", "50-249", 249),
    ("250 et plus (ETI+)", "250+", 250),
]

METROPOLES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Bordeaux",
    "Lille", "Nantes", "Nice", "Strasbourg", "Montpellier",
    "Rennes", "Grenoble",
]

TOTAL_STEPS = 4


# ── HELPERS UI ───────────────────────────────────────────────────────────
def _header():
    st.markdown(
        '<div style="text-align:center;margin-bottom:8px;">'
        '<span style="font-family:Syne,sans-serif;font-weight:800;font-size:2rem;color:#0B2545;">'
        'LOGI<span style="color:#00C896;">FLO</span>.IO</span></div>',
        unsafe_allow_html=True,
    )


def _progress(step, lang):
    """Barre de progression 'Etape X sur 4'."""
    pct = int(step / TOTAL_STEPS * 100)
    label = f"{'Step' if lang == 'en' else 'Etape'} {step} / {TOTAL_STEPS}"
    st.markdown(
        f'<div style="max-width:520px;margin:0 auto 4px auto;font-family:Syne,sans-serif;'
        f'font-size:0.8rem;color:#4A6080;font-weight:600;">{label}</div>'
        f'<div style="max-width:520px;margin:0 auto 24px auto;background:#E2E8F0;'
        f'border-radius:99px;height:8px;overflow:hidden;">'
        f'<div style="width:{pct}%;height:100%;background:#00C896;'
        f'border-radius:99px;transition:width .3s;"></div></div>',
        unsafe_allow_html=True,
    )


def _question_title(text):
    st.markdown(
        f'<h2 style="text-align:center;color:#0B2545;font-family:Syne,sans-serif;'
        f'font-size:1.6rem;margin-bottom:28px;">{text}</h2>',
        unsafe_allow_html=True,
    )


def _goto(step):
    st.session_state.onb_step = step
    st.rerun()


# ── ECRANS ───────────────────────────────────────────────────────────────
def _screen_sector(lang):
    _progress(1, lang)
    _question_title("Quel est votre secteur d'activite ?" if lang == "fr"
                    else "What is your sector?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for icon, fr, en, val in SECTORS:
            label = f"{icon}  {fr if lang == 'fr' else en}"
            if st.button(label, use_container_width=True, key=f"sec_{val}_{fr}"):
                st.session_state.onb_sector = val
                st.session_state.onb_sector_label = fr if lang == "fr" else en
                _goto(2)


def _screen_revenue(lang):
    _progress(2, lang)
    _question_title("Quel est votre chiffre d'affaires annuel ?" if lang == "fr"
                    else "What is your annual revenue?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for fr, en, val in REVENUE_BRACKETS:
            if st.button(fr if lang == "fr" else en,
                         use_container_width=True, key=f"rev_{val}"):
                st.session_state.onb_revenue = val
                st.session_state.onb_revenue_label = fr if lang == "fr" else en
                _goto(3)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                     key="rev_back", use_container_width=True):
            _goto(1)


def _screen_employees(lang):
    _progress(3, lang)
    _question_title("Combien de collaborateurs ?" if lang == "fr"
                    else "How many employees?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        for label, val, sort_val in EMPLOYEE_RANGES:
            if st.button(label, use_container_width=True, key=f"emp_{val}"):
                st.session_state.onb_employees = val
                st.session_state.onb_employees_sort = sort_val
                _goto(4)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                     key="emp_back", use_container_width=True):
            _goto(2)


def _screen_location(lang):
    _progress(4, lang)
    _question_title("Ou etes-vous situe ?" if lang == "fr"
                    else "Where are you located?")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        st.markdown(
            f'<div style="font-family:Syne,sans-serif;font-weight:600;font-size:0.9rem;'
            f'color:#4A6080;margin-bottom:10px;">'
            f'{"Select a city or type another" if lang == "en" else "Choisissez une ville ou saisissez la votre"}</div>',
            unsafe_allow_html=True,
        )
        options = METROPOLES + [("Autre..." if lang == "fr" else "Other...")]
        choice = st.selectbox(
            "Ville" if lang == "fr" else "City",
            options, label_visibility="collapsed", key="loc_select",
        )
        custom = ""
        if choice == ("Autre..." if lang == "fr" else "Other..."):
            custom = st.text_input(
                "Votre ville / region" if lang == "fr" else "Your city / region",
                key="loc_custom", placeholder="ex : Avignon, Bruxelles...",
            )
        final_loc = custom.strip() if custom.strip() else (
            choice if choice not in ("Autre...", "Other...") else "")

        st.markdown("<br>", unsafe_allow_html=True)
        col_back, col_next = st.columns(2)
        with col_back:
            if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                         key="loc_back", use_container_width=True):
                _goto(3)
        with col_next:
            if st.button(("Continuer →" if lang == "fr" else "Continue →"),
                         key="loc_next", use_container_width=True,
                         type="primary", disabled=not final_loc):
                st.session_state.onb_location = final_loc
                _goto(5)


def _screen_recap(lang):
    """Recapitulatif + consentement RGPD + ecriture Supabase."""
    _header()
    _question_title("Recapitulatif" if lang == "fr" else "Summary")
    _c1, fc, _c2 = st.columns([1, 2.5, 1])
    with fc:
        rows = [
            ("Secteur" if lang == "fr" else "Sector",
             st.session_state.get("onb_sector_label", "—")),
            ("Chiffre d'affaires" if lang == "fr" else "Revenue",
             st.session_state.get("onb_revenue_label", "—")),
            ("Collaborateurs" if lang == "fr" else "Employees",
             st.session_state.get("onb_employees", "—")),
            ("Localisation" if lang == "fr" else "Location",
             st.session_state.get("onb_location", "—")),
        ]
        recap_html = '<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;padding:20px;margin-bottom:16px;">'
        for k, v in rows:
            recap_html += (
                f'<div style="display:flex;justify-content:space-between;'
                f'padding:8px 0;border-bottom:1px solid #F1F5F9;">'
                f'<span style="color:#4A6080;font-size:0.9rem;">{k}</span>'
                f'<span style="color:#0B2545;font-weight:700;font-size:0.9rem;">{v}</span></div>'
            )
        recap_html += "</div>"
        st.markdown(recap_html, unsafe_allow_html=True)

        # Consentement RGPD (repris de ton onboarding actuel)
        if lang == "en":
            consent_text = """**What we do with your data:**
- Files are processed in memory only — never stored on disk
- A summary is sent to AI (OpenAI/Gemini, GDPR-compliant) for analysis
- Audit results (KPIs, summary, PDF) are stored in our EU database (Paris)
- Your data is never sold or shared with third parties
- Right to access, rectify, delete at any time: contact@logiflo.io"""
            check_label = "I accept the processing of my data as described above"
            btn_label = "CONFIRM AND START"
        else:
            consent_text = """**Ce que nous faisons de vos donnees :**
- Les fichiers sont traites en memoire uniquement — jamais stockes sur disque
- Un resume est envoye a l'IA (OpenAI/Gemini, conforme RGPD) pour l'analyse
- Les resultats (KPIs, resume, PDF) sont stockes dans notre base UE (Paris)
- Vos donnees ne sont jamais vendues ni partagees avec des tiers
- Droit d'acces, rectification, suppression a tout moment : contact@logiflo.io"""
            check_label = "J'accepte le traitement de mes donnees tel que decrit ci-dessus"
            btn_label = "CONFIRMER ET DEMARRER"

        st.markdown(
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;'
            f'padding:20px;margin-bottom:16px;font-size:0.85rem;color:#4A6080;line-height:1.7;">'
            f'{consent_text.replace(chr(10), "<br>")}</div>',
            unsafe_allow_html=True,
        )
        accept = st.checkbox(check_label, key="onb_accept")

        st.markdown("<br>", unsafe_allow_html=True)
        col_back, col_go = st.columns(2)
        with col_back:
            if st.button("← " + ("Retour" if lang == "fr" else "Back"),
                         key="recap_back", use_container_width=True):
                _goto(4)
        with col_go:
            if st.button(btn_label, use_container_width=True, type="primary",
                         disabled=not accept, key="recap_go"):
                user = st.session_state.get("current_user", "")
                ok = save_organization(
                    user,
                    sector_key=st.session_state.get("onb_sector"),
                    revenue_bracket=st.session_state.get("onb_revenue"),
                    employee_count=st.session_state.get("onb_employees_sort"),
                    location=st.session_state.get("onb_location"),
                )
                # On enchaine sur le choix de profil (Manager / Terrain) meme si
                # l'ecriture echoue (mode degrade), en gardant le secteur en session.
                st.session_state.rgpd_ok = True
                st.session_state._onboarding_done = True
                st.session_state["_user_sector"] = st.session_state.get("onb_sector")
                st.session_state.page = "profil"
                st.rerun()


# ── POINT D'ENTREE ───────────────────────────────────────────────────────
def render_onboarding():
    """Routeur interne de l'onboarding multi-pages."""
    lang = st.session_state.get("language", "fr")
    step = st.session_state.get("onb_step", 1)

    # Header affiche sur toutes les etapes sauf recap (qui a le sien)
    if step <= TOTAL_STEPS:
        _header()

    if step == 1:
        _screen_sector(lang)
    elif step == 2:
        _screen_revenue(lang)
    elif step == 3:
        _screen_employees(lang)
    elif step == 4:
        _screen_location(lang)
    else:
        _screen_recap(lang)
