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
# ════════════════════════════════════════════════════════════════════════
# FONCTIONS À AJOUTER dans services/supabase_client.py
# (à coller à la fin du fichier, avant get_historique_audits ou après —
#  l'ordre n'a pas d'importance, ce sont des fonctions indépendantes)
# ════════════════════════════════════════════════════════════════════════


def get_organization(username):
    """Recupere la fiche entreprise d'un utilisateur depuis la table organizations.
    Retourne un dict avec les infos de l'entreprise, ou None si elle n'existe pas
    (= l'utilisateur n'a jamais fait l'onboarding).

    C'est cette fonction que le routeur appelle a chaque connexion pour decider :
    - None ou onboarding_completed=False  -> montrer l'onboarding
    - onboarding_completed=True           -> aller directement a l'app
    """
    sb = get_supabase()
    if not sb:
        return None
    try:
        resp = (
            sb.table("organizations")
            .select("*")
            .eq("owner_user_id", str(username))
            .execute()
        )
        if resp and hasattr(resp, "data") and resp.data:
            _debug_supabase(f"get_organization OK -> user={username}")
            return resp.data[0]
        _debug_supabase(f"get_organization vide -> user={username} (pas d'onboarding)")
        return None
    except Exception as e:
        _debug_supabase("get_organization EXCEPTION", e)
        return None


def save_organization(username, sector_key=None, revenue_bracket=None,
                      employee_count=None, location=None, company_name=None,
                      plan=None):
    """Cree ou met a jour la fiche entreprise d'un utilisateur.
    Passe onboarding_completed a True. Appelee une seule fois, au clic
    'Confirmer' du recapitulatif d'onboarding.

    upsert sur owner_user_id : si la ligne existe deja, elle est mise a jour ;
    sinon elle est creee. Grace a la contrainte UNIQUE sur owner_user_id,
    un utilisateur ne peut avoir qu'une seule fiche entreprise.
    """
    sb = get_supabase()
    if not sb:
        _debug_supabase("save_organization : pas de client Supabase")
        return False
    try:
        payload = {
            "owner_user_id": str(username),
            "onboarding_completed": True,
            "updated_at": datetime.datetime.now().isoformat(),
        }
        # On n'ajoute que les champs reellement fournis (evite d'ecraser
        # avec des None si on rappelle la fonction plus tard)
        if sector_key is not None:
            payload["sector_key"] = str(sector_key)
        if revenue_bracket is not None:
            payload["revenue_bracket"] = str(revenue_bracket)
        if employee_count is not None:
            payload["employee_count"] = int(employee_count)
        if location is not None:
            payload["location"] = str(location)
        if company_name is not None:
            payload["company_name"] = str(company_name)
        if plan is not None:
            payload["plan"] = str(plan)

        sb.table("organizations").upsert(
            payload, on_conflict="owner_user_id"
        ).execute()
        _debug_supabase(f"save_organization OK -> user={username} sector={sector_key}")
        return True
    except Exception as e:
        _debug_supabase("save_organization EXCEPTION", e)
        return False
