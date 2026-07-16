# -*- coding: utf-8 -*-
"""
Logiflo - engine/scoring.py
Version 8.0 (juillet 2026) — scoring sante entreprise
  - Plafond legal : produit perime en rayon => score global plafonne
  - 4 dimensions : Conformite (veto), Performance, Sante financiere, Fiabilite donnees
  - Niveau de fiabilite affiche separement
"""

import streamlit as st
import pandas as pd
import numpy as np

SECTORAL_POSSESSION_RATE = {
    "stock_industrie": 0.20, "stock_distribution": 0.23,
    "stock_retail": 0.30, "stock_pharma": 0.28,
    "stock_agroalim": 0.42, "stock_btp": 0.22,
    "generique": 0.23,
}

# Plafond du score global quand un produit deja perime est en rayon.
LEGAL_CAP = 40


def _pct(numerateur, denominateur):
    return (numerateur / denominateur * 100) if denominateur else 0


def _compute_data_quality(df, has_conso, has_peremption):
    """Niveau de fiabilite de l'analyse (0-100), base sur la completude des donnees.
    Un audit sur des donnees incompletes ne peut pas etre pleinement fiable."""
    if df is None or len(df) == 0:
        return 30
    n = len(df)
    score = 100.0

    # 1. Prix d'achat renseignes (le plus important : sans prix, pas de vraie analyse financiere)
    if "prix_unitaire" in df.columns:
        pct_prix = _pct((df["prix_unitaire"].fillna(0) > 0).sum(), n)
    else:
        pct_prix = 0
    # Le % de prix pese lourd : de 100% (aucune penalite) a 0% (-50 pts)
    score -= (100 - pct_prix) * 0.50

    # 2. Historique de consommation (debloque rotation, surstock, ruptures)
    if not has_conso:
        score -= 25

    # 3. Dates de peremption (pertinent surtout pour perissable, bonus si present)
    if not has_peremption:
        score -= 10

    return int(max(0, min(100, round(score)))), pct_prix


def _count_peremption(df):
    """Retourne (nb_deja_perimes, nb_perime_bientot) depuis les flags de l'ingester/workspace."""
    deja = 0
    bientot = 0
    try:
        if "_perime_critique" in df.columns:
            # _perime_critique = expire <=7j (futur). On veut distinguer deja perime.
            pass
        # Recalcul robuste depuis la date si presente
        if "date_peremption" in df.columns and "quantite" in df.columns:
            today = pd.Timestamp.now().normalize()
            d = df[df["date_peremption"].notna() & (df["quantite"].fillna(0) > 0)].copy()
            if len(d) > 0:
                jours = (d["date_peremption"] - today).dt.days
                deja = int((jours < 0).sum())
                bientot = int(((jours >= 0) & (jours <= 30)).sum())
    except Exception:
        pass
    return deja, bientot


def compute_logiflo_score(module, df=None, kpis=None, labels=None,
                           sector_key="generique", lang="fr"):
    scores = {}
    details = {}

    if module == "stock":
        # ── Recuperation des donnees de base ──
        try:
            tx_service  = float(kpis[1]) if kpis and len(kpis) > 1 else 0
            nb_ruptures = float(kpis[2]) if kpis and len(kpis) > 2 else 0
            nb_total    = len(df) if df is not None and len(df) > 0 else 1
            taux_rupture = _pct(nb_ruptures, nb_total)
        except Exception:
            tx_service = 0; taux_rupture = 0; nb_total = 1

        has_conso = bool(df["_has_conso"].iloc[0]) if (df is not None and "_has_conso" in df.columns and len(df) > 0) else False
        has_peremption = bool(df["_has_peremption"].iloc[0]) if (df is not None and "_has_peremption" in df.columns and len(df) > 0) else False

        # ══ DIMENSION 1 : CONFORMITE / RISQUE LEGAL (veto) ══
        nb_deja_perime, nb_perime_bientot = _count_peremption(df) if df is not None else (0, 0)
        if nb_deja_perime > 0:
            s_conf = 10   # infraction caracterisee
        elif nb_perime_bientot > 0:
            s_conf = 55   # risque a venir, pas encore une infraction
        else:
            s_conf = 100
        scores["conformite"] = s_conf
        d_conf = "Legal Compliance" if lang == "en" else "Conformite legale"
        details[d_conf] = s_conf

        # ══ DIMENSION 2 : PERFORMANCE OPERATIONNELLE (35%) ══
        target_service = {
            "stock_pharma": 97, "stock_industrie": 97, "stock_retail": 96,
            "stock_distribution": 95, "stock_agroalim": 96, "stock_btp": 95,
        }.get(sector_key, 93)

        if tx_service >= target_service:          s1 = 100
        elif tx_service >= target_service - 5:    s1 = 80
        elif tx_service >= target_service - 10:   s1 = 60
        elif tx_service >= 80:                    s1 = 40
        else:                                     s1 = 20
        # Ajustement rupture
        if taux_rupture > 10:   s1 = min(s1, 40)
        elif taux_rupture > 5:  s1 = min(s1, 60)
        scores["performance"] = s1
        d1 = "Operational Performance" if lang == "en" else "Performance operationnelle"
        details[d1] = s1

        # ══ DIMENSION 3 : SANTE FINANCIERE DU STOCK (35%) ══
        s3 = 70
        if df is not None:
            try:
                nb_total2 = max(len(df), 1)
                nb_dorm = len(df[df["Statut"].str.contains("Dormant", na=False)]) if "Statut" in df.columns else 0
                nb_surs = len(df[df["Statut"].str.contains("Surstock", na=False)]) if "Statut" in df.columns else 0
                taux_anom = _pct(nb_dorm + nb_surs, nb_total2)

                poss_rate = SECTORAL_POSSESSION_RATE.get(sector_key, 0.23)
                severity = poss_rate / 0.23
                taux_ajuste = taux_anom * severity

                if taux_ajuste <= 5:    s3 = 100
                elif taux_ajuste <= 10: s3 = 75
                elif taux_ajuste <= 20: s3 = 50
                elif taux_ajuste <= 35: s3 = 30
                else:                   s3 = 15

                # Penalite peremption future : de la perte programmee = du cash qui part
                taux_perime_futur = _pct(nb_perime_bientot, nb_total2)
                if taux_perime_futur > 10:   s3 = min(s3, 30)
                elif taux_perime_futur > 5:  s3 = min(s3, 50)
                elif taux_perime_futur > 2:  s3 = min(s3, 65)
            except Exception:
                s3 = 70
        scores["finance"] = s3
        d3 = "Stock Financial Health" if lang == "en" else "Sante financiere du stock"
        details[d3] = s3

        # ══ DIMENSION 4 : QUALITE DES DONNEES (fiabilite) ══
        s_data, pct_prix = _compute_data_quality(df, has_conso, has_peremption)
        scores["fiabilite"] = s_data
        d_data = "Data Reliability" if lang == "en" else "Fiabilite des donnees"
        details[d_data] = s_data

        # ══ SCORE GLOBAL ══
        # Moyenne ponderee des dimensions "sante" (hors conformite qui est un veto)
        base_score = round(s1 * 0.35 + s3 * 0.35 + s_data * 0.30)

        # Conformite = plafond, pas un poids. Si perime en rayon -> plafond legal.
        legal_capped = False
        if nb_deja_perime > 0:
            global_score = min(base_score, LEGAL_CAP)
            legal_capped = True
        else:
            global_score = base_score

        return {
            "global":       global_score,
            "details":      details,
            "scores":       scores,
            "reliability":  s_data,
            "pct_prix":     round(pct_prix),
            "legal_capped": legal_capped,
            "nb_deja_perime":   nb_deja_perime,
            "nb_perime_bientot": nb_perime_bientot,
            "has_conso":    has_conso,
            "format_pdf": "\n".join([f"- {k} : {v}/100" for k, v in details.items()]),
        }

    elif module == "transport":
        try:
            marge_pct = float(kpis[1]) if kpis and len(kpis) > 1 else 0
            nb_tox    = float(kpis[2]) if kpis and len(kpis) > 2 else 0
            nb_total  = len(df) if df is not None and len(df) > 0 else 1
            taux_tox  = _pct(nb_tox, nb_total)
        except Exception:
            marge_pct = 0; taux_tox = 0

        if marge_pct >= 10:   s1 = 100
        elif marge_pct >= 8:  s1 = 80
        elif marge_pct >= 6:  s1 = 60
        elif marge_pct >= 4:  s1 = 40
        elif marge_pct >= 0:  s1 = 20
        else:                 s1 = 5
        scores["rentabilite"] = s1
        d1 = "Profitability and Transport Yield" if lang == "en" else "Rentabilite et Yield Transport"
        details[d1] = s1

        if taux_tox <= 5:    s2 = 100
        elif taux_tox <= 10: s2 = 75
        elif taux_tox <= 20: s2 = 50
        elif taux_tox <= 35: s2 = 30
        else:                s2 = 10
        scores["efficacite"] = s2
        d2 = "Operational Efficiency" if lang == "en" else "Efficacite Operationnelle"
        details[d2] = s2

        cout_km = 0
        if df is not None and "_DS" in df.columns and "_CO" in df.columns:
            try:
                total_dist = df["_DS"].replace(0, 1).sum()
                cout_km = df["_CO"].sum() / total_dist if total_dist > 0 else 0
            except Exception:
                pass

        cnr_ref = 1.95
        if cout_km <= 0:               s3 = 70
        elif cout_km <= cnr_ref:       s3 = 100
        elif cout_km <= cnr_ref*1.10:  s3 = 80
        elif cout_km <= cnr_ref*1.25:  s3 = 60
        elif cout_km <= cnr_ref*1.50:  s3 = 35
        else:                          s3 = 15
        scores["opex"] = s3
        d3 = "OPEX Control" if lang == "en" else "Maitrise des OPEX"
        details[d3] = s3

        global_score = round(s1 * 0.40 + s2 * 0.35 + s3 * 0.25)
        return {
            "global":     global_score,
            "details":    details,
            "scores":     scores,
            "format_pdf": "\n".join([f"- {k} : {v}/100" for k, v in details.items()])
        }

    else:
        return {
            "global": 70, "details": {"Performance": 70}, "scores": {"performance": 70},
            "format_pdf": "- Performance : 70/100"
        }
