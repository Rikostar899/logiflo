import streamlit as st
import pandas as pd
import datetime
import base64
import gspread
from google.oauth2.service_account import Credentials

try:
    from supabase import create_client as _supa_create
except Exception:
    _supa_create = None

try:
    SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID", "")
except Exception:
    SHEET_ID = ""


def get_supabase():
    try:
        url = st.secrets.get("SUPABASE_URL", "")
        key = st.secrets.get("SUPABASE_KEY", "")
        if url and key and _supa_create:
            return _supa_create(url, key)
    except Exception:
        pass
    return None


def save_audit_to_sheets(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
    sb = get_supabase()
    if not sb:
        return _save_audit_sheets_fallback(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes)
    try:
        now = datetime.datetime.now()
        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else ""
        _profil = st.session_state.get("stock_view", "MANAGER").lower()
        sb.table("audits").insert({
            "user_id":     username,
            "created_at":  now.isoformat(),
            "module":      module,
            "nb_lignes":   int(nb_lignes),
            "kpi_1":       round(float(kpis[0]), 2) if len(kpis) > 0 else 0,
            "kpi_2":       round(float(kpis[1]), 2) if len(kpis) > 1 else 0,
            "kpi_3":       round(float(kpis[2]), 2) if len(kpis) > 2 else 0,
            "kpi_label_1": labels[0] if len(labels) > 0 else "",
            "kpi_label_2": labels[1] if len(labels) > 1 else "",
            "kpi_label_3": labels[2] if len(labels) > 2 else "",
            "resume_ia":   (resume_ia or "")[:800],
            "pdf_base64":  pdf_b64,
            "profil":      _profil,
        }).execute()
        return True
    except Exception:
        pass
    return False


def load_archives_from_sheets(username):
    sb = get_supabase()
    if not sb:
        return _load_archives_sheets_fallback(username)
    try:
        resp = (sb.table("audits")
                  .select("*")
                  .eq("user_id", username)
                  .order("created_at", desc=False)
                  .execute())
        if not resp.data:
            return pd.DataFrame()
        df = pd.DataFrame(resp.data)
        if "created_at" in df.columns:
            df["date"]  = pd.to_datetime(df["created_at"]).dt.strftime("%d/%m/%Y")
            df["heure"] = pd.to_datetime(df["created_at"]).dt.strftime("%H:%M")
        return df
    except Exception:
        return _load_archives_sheets_fallback(username)


def load_user_prefs(username):
    sb = get_supabase()
    if not sb:
        return {}
    try:
        resp = (sb.table("user_prefs")
                  .select("*")
                  .eq("user_id", username)
                  .execute())
        if resp.data:
            return resp.data[0]
        return {}
    except Exception:
        return {}


def save_user_prefs(username, prefs_dict):
    sb = get_supabase()
    if not sb:
        return False
    try:
        prefs_dict["user_id"] = username
        prefs_dict["updated_at"] = datetime.datetime.now().isoformat()
        sb.table("user_prefs").upsert(prefs_dict).execute()
        return True
    except Exception:
        return False


def get_gsheet_client():
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except Exception:
        return None


def _save_audit_sheets_fallback(username, module, nb_lignes, kpis, labels, resume_ia, pdf_bytes):
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
            now.strftime("%d/%m/%Y"), now.strftime("%H:%M"), module, nb_lignes,
            round(kpis[0], 2) if len(kpis) > 0 else "",
            round(kpis[1], 2) if len(kpis) > 1 else "",
            round(kpis[2], 2) if len(kpis) > 2 else "",
            labels[0] if len(labels) > 0 else "",
            labels[1] if len(labels) > 1 else "",
            labels[2] if len(labels) > 2 else "",
            (resume_ia or "")[:800],
            base64.b64encode(pdf_bytes).decode("utf-8") if pdf_bytes else ""
        ])
        return True
    except Exception:
        return False


def _load_archives_sheets_fallback(username):
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
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception:
        return None


def get_historique_audits(username, module, n=6, current_kpis=None, current_labels=None):
    try:
        df = load_archives_from_sheets(username)
        if df is None or df.empty:
            return None
        df = df[df["module"] == module].copy()
        if len(df) < 2:
            return None
        for col in ["kpi_1", "kpi_2", "kpi_3"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        try:
            df["_dt"] = pd.to_datetime(df["date"] + " " + df["heure"], format="%d/%m/%Y %H:%M", errors="coerce")
            df = df.sort_values("_dt", ascending=False)
        except Exception:
            pass
        recent = df.head(n).iloc[::-1]
        history = []
        for _, row in recent.iterrows():
            history.append({
                "date":    row.get("date", "?"),
                "kpi_1":  row.get("kpi_1", 0),
                "kpi_2":  row.get("kpi_2", 0),
                "kpi_3":  row.get("kpi_3", 0),
                "label_1": row.get("kpi_label_1", "KPI1"),
                "label_2": row.get("kpi_label_2", "KPI2"),
                "label_3": row.get("kpi_label_3", "KPI3"),
                "resume":  str(row.get("resume_ia", ""))[:400],
            })
        if current_kpis and len(current_kpis) >= 2:
            cl = current_labels or ["KPI1", "KPI2", "KPI3"]
            history.append({
                "date":    datetime.date.today().strftime("%d/%m/%Y"),
                "kpi_1":  float(current_kpis[0]) if len(current_kpis) > 0 else 0,
                "kpi_2":  float(current_kpis[1]) if len(current_kpis) > 1 else 0,
                "kpi_3":  float(current_kpis[2]) if len(current_kpis) > 2 else 0,
                "label_1": cl[0] if len(cl) > 0 else "KPI1",
                "label_2": cl[1] if len(cl) > 1 else "KPI2",
                "label_3": cl[2] if len(cl) > 2 else "KPI3",
                "resume":  "",
            })
        if len(history) < 2:
            return None
        first = history[0]
        last  = history[-1]

        def delta_pct(new, old):
            try:
                new, old = float(new), float(old)
                if old == 0: return None
                return round((new - old) / abs(old) * 100, 1)
            except Exception:
                return None

        return {
            "history":    history,
            "n_audits":   len(history),
            "first_date": first["date"],
            "last_date":  last["date"],
            "delta_1":    delta_pct(last["kpi_1"], first["kpi_1"]),
            "delta_2":    delta_pct(last["kpi_2"], first["kpi_2"]),
            "delta_3":    delta_pct(last["kpi_3"], first["kpi_3"]),
        }
    except Exception:
        return None
