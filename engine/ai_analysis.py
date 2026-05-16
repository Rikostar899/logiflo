# -*- coding: utf-8 -*-
"""
Logiflo - engine/ai_analysis.py
Version 7.0 (mai 2026) — 10 regles metier stock implementees

Regles :
  R1  Pare-feu donnees (Mode A/B/C/D)
  R2  Classification ABC/XYZ
  R3  6 KPIs orientes cash
  R4  Cout de possession SECTORIEL
  R5  Stock mort corrige (zero sur 12 derniers mois, min 2 ans)
  R6  Analyse fournisseur (si colonne existe)
  R7  Top 5 actions chiffrees
  R8  Cout de l'inaction (90 jours)
  R9  Benchmark contextuel (vs secteur)
  R10 CTA Logiflo uniquement dans audit gratuit
"""

import streamlit as st
import re
import os
import datetime
import numpy as np

from config.sectoral_db import detect_sector, get_sector_benchmarks
from config.translations import _
from services.supabase_client import get_historique_audits


# ════════════════════════════════════════════════════════════════════════════
# CONSTANTES SECTORIELLES
# ════════════════════════════════════════════════════════════════════════════
SECTORAL_POSSESSION_RATE = {
    "stock_industrie": 0.20, "stock_distribution": 0.23,
    "stock_retail": 0.30, "stock_pharma": 0.28,
    "stock_agroalim": 0.42, "stock_btp": 0.22,
    "generique": 0.23,
}

SECTORAL_COVERAGE_THRESHOLD = {
    "stock_industrie": 3, "stock_distribution": 2,
    "stock_retail": 6, "stock_pharma": 4,
    "stock_agroalim": 1, "stock_btp": 1,
    "generique": 4,
}

SECTORAL_MARGIN_RATIO = {
    "stock_retail": 2.5, "stock_distribution": 1.4,
    "stock_industrie": 1.6, "stock_pharma": 2.0,
    "stock_agroalim": 1.5, "stock_btp": 1.3,
    "generique": 1.5,
}

DEAD_STOCK_DECOTE = {
    "lt_6m": 0.80, "6_12m": 0.60, "12_24m": 0.50,
    "gt_24m": 0.40, "fashion_gt_24m": 0.30,
}


# ════════════════════════════════════════════════════════════════════════════
# ENRICHISSEMENT STOCK (le coeur metier V7)
# ════════════════════════════════════════════════════════════════════════════
def _compute_stock_enrichment(df, sector_key, lang="fr"):
    """
    Calcule TOUTES les metriques metier stock pour injection dans le prompt IA.
    Retourne un texte structure pret a etre ajoute au data_summary.
    """
    if df is None or len(df) == 0:
        return ""

    lines = []
    _en = lang == "en"
    _poss_rate = SECTORAL_POSSESSION_RATE.get(sector_key, 0.23)
    _cov_threshold = SECTORAL_COVERAGE_THRESHOLD.get(sector_key, 4)

    # ── R1 : DETECTION MODE (A/B/C/D) ──
    has_prix = "prix_unitaire" in df.columns and df["prix_unitaire"].notna().mean() > 0.3
    cols_conso = [c for c in ["conso_an1", "conso_an2", "conso_an3", "conso_an4"] if c in df.columns]
    has_conso = len(cols_conso) >= 1
    # Aussi detecter _conso_moy (calcule par logiflo_app) ou colonnes hebdo/mensuelles
    if not has_conso and "_conso_moy" in df.columns and df["_conso_moy"].notna().sum() > 0:
        has_conso = True
    nb_annees_conso = len(cols_conso)
    # Variable safe : la colonne de conso la plus recente (ou None)
    _last_conso = cols_conso[-1] if cols_conso else ("_conso_moy" if "_conso_moy" in df.columns else None)
    _has_lc = _last_conso is not None and _last_conso in df.columns  # safe guard

    if has_prix and has_conso:
        mode = "A"
    elif has_prix and not has_conso:
        mode = "B"
    elif not has_prix and has_conso:
        mode = "C"
    else:
        mode = "D"

    # INSTRUCTION INTERNE pour l'IA — PAS visible dans le rapport client
    lines.append(f"[INTERNAL -- DO NOT MENTION THIS TO THE USER] Data mode: {mode}")

    if mode in ("C", "D"):
        lines.append(f"[INTERNAL] NO prices available. NEVER write EUR amounts in your response.")
    if mode in ("B", "D"):
        lines.append(f"[INTERNAL] NO consumption history. Cannot calculate rotation, dead stock, coverage.")

    # ── R4 : COUT DE POSSESSION SECTORIEL ──
    if mode in ("A", "B"):
        capital_total = 0
        if "valeur_totale" in df.columns:
            capital_total = df["valeur_totale"].sum()
        elif has_prix and "quantite" in df.columns:
            capital_total = (df["quantite"].fillna(0) * df["prix_unitaire"].fillna(0)).sum()

        if capital_total > 0:
            cout_poss = capital_total * _poss_rate
            lines.append(f"\n=== {'HOLDING COST' if _en else 'COUT DE POSSESSION'} ===")
            lines.append(f"{'Total capital' if _en else 'Capital total'} : {capital_total:,.0f} EUR")
            lines.append(f"{'Sectoral rate' if _en else 'Taux sectoriel'} : {_poss_rate*100:.0f}%/{'year' if _en else 'an'}")
            lines.append(f"{'Annual holding cost' if _en else 'Cout possession annuel'} : {cout_poss:,.0f} EUR/{'year' if _en else 'an'} ({cout_poss/12:,.0f} EUR/{'month' if _en else 'mois'})")

    # ── R5 : STOCK MORT (corrige : zero sur 12 derniers mois, min 2 ans) ──
    if has_conso and nb_annees_conso >= 2:
        last_conso_col = _last_conso  # conso la plus recente
        df_with_qty = df[df["quantite"].fillna(0) > 0].copy() if "quantite" in df.columns else df.copy()

        dead_mask = (df_with_qty[last_conso_col].fillna(0) == 0)
        if nb_annees_conso >= 3:
            prev_col = cols_conso[-2] if len(cols_conso) >= 2 else None
            dead_mask = dead_mask & (df_with_qty[prev_col].fillna(0) == 0)

        dead = df_with_qty[dead_mask]

        if len(dead) > 0:
            lines.append(f"\n=== {'DEAD STOCK' if _en else 'STOCK MORT'} ({len(dead)} {'refs' if _en else 'refs'}) ===")
            lines.append(f"{'Definition' if _en else 'Definition'} : {'zero consumption on last 12 months, min 2 years data' if _en else 'zero conso sur les 12 derniers mois, minimum 2 ans de donnees'}")

            if mode == "A":
                if "valeur_totale" in dead.columns:
                    cap_mort = dead["valeur_totale"].sum()
                else:
                    cap_mort = (dead["quantite"].fillna(0) * dead["prix_unitaire"].fillna(0)).sum()
                lines.append(f"{'Dead capital' if _en else 'Capital mort'} : {cap_mort:,.0f} EUR")
                lines.append(f"{'Holding cost on dead stock' if _en else 'Cout possession stock mort'} : {cap_mort * _poss_rate:,.0f} EUR/{'year' if _en else 'an'}")

                # Simulations R7/R8
                recup_fournisseur = cap_mort * 0.65
                recup_solde = cap_mort * 0.45
                lines.append(f"\n{'SIMULATION — Dead stock liquidation' if _en else 'SIMULATION — Liquidation stock mort'} :")
                lines.append(f"  {'Scenario A — Supplier return (65% credit)' if _en else 'Scenario A — Retour fournisseur (65% avoir)'} : {recup_fournisseur:,.0f} EUR")
                lines.append(f"  {'Scenario B — Clearance sale (-55%)' if _en else 'Scenario B — Vente soldee (-55%)'} : {recup_solde:,.0f} EUR")
                lines.append(f"  {'Scenario C — Do nothing' if _en else 'Scenario C — Ne rien faire'} : -{cap_mort * _poss_rate / 12 * 3:,.0f} EUR {'lost in 90 days (holding cost)' if _en else 'perdus en 90 jours (cout possession)'}")

            top_dead = dead.nlargest(5, "quantite") if "quantite" in dead.columns else dead.head(5)
            for _, row in top_dead.iterrows():
                ref = row.get("reference", "?")
                qty = row.get("quantite", 0)
                val = f" = {row.get('quantite', 0) * row.get('prix_unitaire', 0):,.0f} EUR" if mode == "A" else ""
                fournisseur = f" (fourn: {row.get('fournisseur', '?')})" if "fournisseur" in df.columns else ""
                lines.append(f"  - {ref} : {qty:.0f} {'units' if _en else 'unites'}{val}{fournisseur}")

    # ── R3 : SURSTOCK ──
    if has_conso and "quantite" in df.columns:
        df_active = df[(df["quantite"].fillna(0) > 0)].copy()
        if "_conso_moy" in df_active.columns:
            conso_col = "_conso_moy"
        elif cols_conso:
            df_active["_conso_calc"] = df_active[cols_conso].fillna(0).mean(axis=1)
            conso_col = "_conso_calc"
        else:
            conso_col = None

        if conso_col and conso_col in df_active.columns:
            df_active["_conso_mens"] = df_active[conso_col] / 12
            df_active["_couv_mois"] = np.where(
                df_active["_conso_mens"] > 0,
                df_active["quantite"] / df_active["_conso_mens"],
                9999
            )
            surstock = df_active[(df_active["_couv_mois"] > _cov_threshold) & (df_active["_conso_mens"] > 0)]

            if len(surstock) > 0:
                lines.append(f"\n=== {'OVERSTOCK' if _en else 'SURSTOCK'} ({len(surstock)} refs, {'threshold' if _en else 'seuil'} > {_cov_threshold} {'months' if _en else 'mois'}) ===")

                if mode == "A":
                    surstock_val = 0
                    for _, row in surstock.iterrows():
                        stock_cible = row["_conso_mens"] * _cov_threshold
                        excedent = max(0, row["quantite"] - stock_cible)
                        prix = row.get("prix_unitaire", 0) or 0
                        surstock_val += excedent * prix
                    lines.append(f"{'Overstock capital' if _en else 'Capital surstocke'} : {surstock_val:,.0f} EUR")
                    lines.append(f"{'Holding cost' if _en else 'Cout possession surstock'} : {surstock_val * _poss_rate:,.0f} EUR/{'year' if _en else 'an'}")

                    lines.append(f"\n{'SIMULATION — Overstock reduction' if _en else 'SIMULATION — Reduction surstock'} :")
                    lines.append(f"  {'Scenario A — Freeze reorders' if _en else 'Scenario A — Geler les appros'} : {surstock_val:,.0f} EUR {'freed progressively' if _en else 'liberes progressivement'}")
                    lines.append(f"  {'Scenario B — Promo -30%' if _en else 'Scenario B — Promo -30%'} : {surstock_val * 0.70:,.0f} EUR {'recovered in 3 months' if _en else 'recuperes en 3 mois'}")
                    lines.append(f"  {'Scenario C — Do nothing' if _en else 'Scenario C — Ne rien faire'} : -{surstock_val * _poss_rate / 4:,.0f} EUR/{'quarter' if _en else 'trimestre'}")

                top_sur = surstock.nlargest(5, "_couv_mois")
                for _, row in top_sur.iterrows():
                    ref = row.get("reference", "?")
                    cov = row["_couv_mois"]
                    qty = row.get("quantite", 0)
                    lines.append(f"  - {ref} : {cov:.0f} {'months coverage' if _en else 'mois de couverture'}, {qty:.0f} {'units' if _en else 'unites'}")

    # ── RUPTURES ACTIVES ──
    if has_conso and "quantite" in df.columns:
        rupt = df[(df["quantite"].fillna(0) <= 0)]
        if conso_col and conso_col in df.columns:
            rupt_actives = rupt[rupt[_last_conso].fillna(0) > 0] if _has_lc else rupt
        else:
            rupt_actives = rupt

        if len(rupt_actives) > 0:
            lines.append(f"\n=== {'ACTIVE STOCKOUTS' if _en else 'RUPTURES ACTIVES'} ({len(rupt_actives)} refs) ===")
            if mode == "A":
                margin_ratio = SECTORAL_MARGIN_RATIO.get(sector_key, 1.5)
                ca_perdu_mois = 0
                for _, row in rupt_actives.iterrows():
                    conso_an = row.get(_last_conso, 0) or 0
                    prix = row.get("prix_unitaire", 0) or 0
                    ca_perdu_mois += (conso_an / 12) * prix * margin_ratio
                lines.append(f"{'Estimated lost revenue' if _en else 'CA perdu estime'} : {ca_perdu_mois:,.0f} EUR/{'month' if _en else 'mois'} ({'based on sectoral margin ratio' if _en else 'base sur ratio marge sectoriel'} x{margin_ratio})")
                lines.append(f"{'Restock investment needed' if _en else 'Investissement reappro'} : {sum(r.get(_last_conso, 0) / 12 * _cov_threshold * r.get('prix_unitaire', 0) for _, r in rupt_actives.iterrows()):,.0f} EUR")

            for _, row in rupt_actives.head(5).iterrows():
                ref = row.get("reference", "?")
                conso_txt = f", {'conso' if not _en else 'consumption'} {row.get(_last_conso, 0):.0f}/{'year' if _en else 'an'}" if _has_lc else ""
                lines.append(f"  - {ref} : {'stock 0' if _en else 'stock 0'}{conso_txt}")

    # ── R6 : ANALYSE FOURNISSEUR ──
    if "fournisseur" in df.columns and df["fournisseur"].notna().sum() > 0:
        lines.append(f"\n=== {'SUPPLIER ANALYSIS' if _en else 'ANALYSE FOURNISSEUR'} ===")
        grp = df.groupby("fournisseur")

        if mode in ("A", "B") and "prix_unitaire" in df.columns:
            df["_val"] = df["quantite"].fillna(0) * df["prix_unitaire"].fillna(0)
            cap_total = df["_val"].sum()
            if cap_total > 0:
                fgrp = df.groupby("fournisseur")["_val"].sum().sort_values(ascending=False)
                lines.append(f"{'Capital concentration' if _en else 'Concentration capital'} :")
                for fn, fv in fgrp.head(5).items():
                    pct = fv / cap_total * 100
                    alerte = " [!] ALERT > 30%" if pct > 30 else ""
                    lines.append(f"  - {fn} : {fv:,.0f} EUR ({pct:.0f}%){alerte}")

        if has_conso and nb_annees_conso >= 2:
            last_col = _last_conso
            dead_by_fourn = df[(df["quantite"].fillna(0) > 0) & (df[last_col].fillna(0) == 0)]
            if len(dead_by_fourn) > 0:
                fourn_dead = dead_by_fourn.groupby("fournisseur").size().sort_values(ascending=False)
                lines.append(f"{'Dead stock by supplier' if _en else 'Stock mort par fournisseur'} :")
                for fn, cnt in fourn_dead.head(3).items():
                    lines.append(f"  - {fn} : {cnt} {'dead refs' if _en else 'refs mortes'}")

        if _has_lc and "quantite" in df.columns:
            fourn_rupt = df[(df["quantite"].fillna(0) <= 0) & (df[_last_conso].fillna(0) > 0)]
            if len(fourn_rupt) > 0:
                fr = fourn_rupt.groupby("fournisseur").size().sort_values(ascending=False)
                lines.append(f"{'Stockouts by supplier' if _en else 'Ruptures par fournisseur'} :")
                for fn, cnt in fr.head(3).items():
                    lines.append(f"  - {fn} : {cnt} {'stockouts' if _en else 'ruptures'}")

    # ── R8 : COUT DE L'INACTION (90 jours) ──
    if mode == "A":
        cost_inaction = 0
        cap_mort_total = 0
        ca_perdu_total = 0
        surstock_cost = 0

        if has_conso and nb_annees_conso >= 2:
            last_col = _last_conso
            dead_df = df[(df["quantite"].fillna(0) > 0) & (df[last_col].fillna(0) == 0)]
            cap_mort_total = (dead_df["quantite"].fillna(0) * dead_df["prix_unitaire"].fillna(0)).sum() if len(dead_df) > 0 else 0

        if has_conso:
            rupt_df = df[(df["quantite"].fillna(0) <= 0) & (df[_last_conso].fillna(0) > 0)] if _has_lc else None
            if rupt_df is not None and len(rupt_df) > 0:
                margin_ratio = SECTORAL_MARGIN_RATIO.get(sector_key, 1.5)
                for _, r in rupt_df.iterrows():
                    ca_perdu_total += (r.get(_last_conso, 0) / 12) * r.get("prix_unitaire", 0) * margin_ratio

        cost_inaction = (cap_mort_total * _poss_rate / 4) + (ca_perdu_total * 3) + surstock_cost
        if cost_inaction > 0:
            lines.append(f"\n=== {'COST OF INACTION (90 DAYS)' if _en else 'COUT DE L INACTION (90 JOURS)'} ===")
            lines.append(f"{'Dead stock holding' if _en else 'Possession stock mort'} : -{cap_mort_total * _poss_rate / 4:,.0f} EUR")
            lines.append(f"{'Lost revenue (3 months)' if _en else 'CA perdu (3 mois)'} : -{ca_perdu_total * 3:,.0f} EUR")
            lines.append(f"{'TOTAL COST OF INACTION' if _en else 'COUT TOTAL INACTION'} : -{cost_inaction:,.0f} EUR {'over 90 days' if _en else 'sur 90 jours'}")

    return "\n".join(lines) if lines else ""


# ════════════════════════════════════════════════════════════════════════════
# FORMATAGE HISTORIQUE POUR PROMPT
# ════════════════════════════════════════════════════════════════════════════
def format_historique_pour_prompt(hist, module, lang="fr"):
    """Formate l'historique des audits pour injection dans le prompt IA."""
    if not hist:
        return ""
    h = hist["history"]
    n = hist["n_audits"]

    if lang == "en":
        lines = [f"\n=== HISTORICAL TREND -- last {n} audits ==="]
        lines.append(f"Period: {hist['first_date']} -> {hist['last_date']}\n")
        for i, entry in enumerate(h):
            tag = "CURRENT" if i == len(h) - 1 else f"Audit {i+1}"
            lines.append(f"[{tag} -- {entry['date']}]")
            lines.append(
                f"  {entry['label_1'][:20]}: {entry['kpi_1']:.1f} | "
                f"{entry['label_2'][:20]}: {entry['kpi_2']:.1f} | "
                f"{entry['label_3'][:20]}: {entry['kpi_3']:.1f}"
            )
        lines.append("\nCOMPUTED TRENDS:")
        d1, d2, d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]
        if module == "transport":
            if d1 is not None: lines.append(f"  Net margin: {'improving' if d1 > 0 else 'declining'} ({d1:+.1f}%)")
            if d2 is not None: lines.append(f"  Profitability: {'improving' if d2 > 0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Toxic routes: {'improving' if d3 < 0 else 'worsening'} ({d3:+.1f}%)")
        else:
            if d1 is not None: lines.append(f"  Capital/Items: {d1:+.1f}%")
            if d2 is not None: lines.append(f"  Service level: {'improving' if d2 > 0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Stock-outs: {'worsening' if d3 > 0 else 'improving'} ({d3:+.1f}%)")
        lines.append("=== END HISTORICAL DATA ===\n")
    else:
        lines = [f"\n=== TENDANCE HISTORIQUE -- {n} derniers audits ==="]
        lines.append(f"Periode : {hist['first_date']} -> {hist['last_date']}\n")
        for i, entry in enumerate(h):
            tag = "ACTUEL" if i == len(h) - 1 else f"Audit {i+1}"
            lines.append(f"[{tag} -- {entry['date']}]")
            lines.append(
                f"  {entry['label_1'][:25]}: {entry['kpi_1']:.1f} | "
                f"{entry['label_2'][:25]}: {entry['kpi_2']:.1f} | "
                f"{entry['label_3'][:25]}: {entry['kpi_3']:.1f}"
            )
        lines.append("\nTENDANCES CALCULEES :")
        d1, d2, d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]
        if module == "transport":
            if d1 is not None: lines.append(f"  Marge nette : {'en hausse' if d1 > 0 else 'en baisse'} ({d1:+.1f}%)")
            if d2 is not None: lines.append(f"  Taux rentabilite : {'en hausse' if d2 > 0 else 'en baisse'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Trajets toxiques : {'en hausse' if d3 > 0 else 'en baisse'} ({d3:+.1f}%)")
        else:
            if d1 is not None: lines.append(f"  Capital/Articles : {d1:+.1f}%")
            if d2 is not None: lines.append(f"  Taux de service : {'en amelioration' if d2 > 0 else 'en degradation'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Ruptures : {'en hausse' if d3 > 0 else 'en baisse'} ({d3:+.1f}%)")
        lines.append("=== FIN DONNEES HISTORIQUES ===\n")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# PROMPTS STOCK MANAGER (V7 — 10 regles)
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_stock():
    lang = st.session_state.get("language", "fr")
    _n_audits = 0
    try:
        _uid = st.session_state.get("current_user", "")
        _hist = get_historique_audits(_uid, "stock", n=10)
        if _hist:
            _n_audits = _hist.get("n_audits", 0)
    except Exception:
        _n_audits = 0
    _enough = _n_audits >= 3

    if lang == "en":
        _hr = ("3+ audits available. You CAN confirm dead stock (zero consumption confirmed over multiple periods)." if _enough
               else "Fewer than 3 audits. You MUST NOT use the word 'dead' or 'dormant'. For items with zero recent consumption, write: 'No movement detected on last 12 months — to be confirmed with client (seasonal pattern? reserved stock?)'. ASK, don't assert.")

        return f"""You are a Senior Supply Chain Financial Auditor. You deliver PRESCRIPTIVE, cash-oriented reports for SME executives.

RESPOND IN ENGLISH. Full explanatory sentences. Cite exact references and exact EUR amounts.

RULES (NON-NEGOTIABLE):
1. NEVER mention data mode, analysis type, ratios used, or calculation methodology. The report must feel like natural consultant expertise, not a technical report. The client must NEVER see "mode A", "sectoral ratio", "coefficient", "estimate based on".
2. If no prices in file: NEVER write EUR amounts. Speak in quantities only. Simply say that full financial analysis requires purchase prices.
3. If service level >= sector benchmark: OPEN with congratulations.
4. NEVER invent figures. Only use data provided.
5. Each recommendation MUST cite: 1 exact reference + 1 action verb + 1 EUR amount (or quantity if no prices).
6. NEVER give generic advice ("communicate with suppliers", "optimize your stock"). ONLY cite specific references with specific amounts.
7. Use the SIMULATIONS provided in the data to build your recommendations.
8. {_hr}

### OPERATIONAL DIAGNOSIS
Service level vs sector benchmark. List critical references. If history: compare with exact numbers.

### FINANCIAL DIAGNOSIS
If prices available: quantify dead capital, overstock, holding cost with exact EUR.
ABSOLUTE RULE: every aggregate amount MUST be followed by the top 3-5 items that compose it, with detail (reference, quantity, unit price, value). The CFO needs to know WHICH items represent these amounts to take action.
If no prices: quantities only, simply say full financial analysis requires purchase prices.
If supplier analysis is provided: integrate concentration risks and dead stock by supplier.

### STOCKOUT PREDICTIONS
Only if alerts present. Delays < 2 weeks in DAYS, not weeks.

### TOP 5 PRIORITY ACTIONS
5 actions ranked by cash impact. Each action MUST name the specific references (exact name), quantity, EUR amount at stake, and action verb. Use simulations from the data. NEVER an action without a named reference.

### COST OF INACTION
If provided in the data, state the 90-day cost clearly.

STOP after the last action. NO scoring section. NO closing phrase."""

    _rh = ("3 audits ou plus disponibles. Tu PEUX confirmer les stocks morts (consommation zero confirmee sur plusieurs periodes)." if _enough
           else "Moins de 3 audits. Tu NE DOIS PAS utiliser les mots 'mort' ou 'dormant'. Pour les articles sans consommation recente, ecris : 'Aucun mouvement detecte sur les 12 derniers mois — a confirmer avec le client (saisonnalite ? stock reserve ?)'. POSE LA QUESTION, n'affirme pas.")

    return f"""Tu es un Auditeur Financier Senior Supply Chain. Tu delivres des rapports PRESCRIPTIFS et orientes cash pour les dirigeants de PME.

REPONDS EN FRANCAIS. Phrases completes et explicatives. Cite les references exactes et les montants exacts en EUR.

REGLES (NON NEGOCIABLES) :
1. JAMAIS mentionner le mode de donnees, le type d'analyse, les ratios utilises ou la methodologie de calcul. Le rapport doit paraitre comme l'expertise naturelle d'un consultant, pas comme un rapport technique. Le client ne doit JAMAIS voir "mode A", "ratio sectoriel", "coefficient", "estimation basee sur".
2. Si pas de prix dans le fichier : N'ECRIS JAMAIS de montants en EUR. Parle en quantites uniquement. Dis simplement que l'analyse financiere complete necessite les prix d'achat.
3. Si taux de service >= benchmark sectoriel : COMMENCE par une felicitation.
4. N'INVENTE AUCUN chiffre. N'utilise QUE les donnees fournies.
5. Chaque recommandation DOIT citer : 1 reference exacte + 1 verbe d'action + 1 montant EUR (ou quantite si pas de prix).
6. JAMAIS de conseil generique ("communiquer avec les fournisseurs", "optimiser le stock"). UNIQUEMENT des references specifiques avec des montants precis.
7. Utilise les SIMULATIONS fournies dans les donnees pour construire tes recommandations.
8. {_rh}

### DIAGNOSTIC OPERATIONNEL
Taux de service vs benchmark sectoriel. Liste les references critiques. Si historique : compare avec chiffres exacts.

### DIAGNOSTIC FINANCIER
Si prix disponibles : chiffre le capital mort, le surstock, le cout de possession avec EUR exacts.
REGLE ABSOLUE : chaque montant agrege DOIT etre suivi des 3 a 5 articles principaux qui le composent, avec leur detail (reference, quantite, prix unitaire, valeur). Le DAF a besoin de savoir QUELS articles representent ces montants pour agir.
Si pas de prix : quantites seulement, dis simplement que l'analyse financiere complete necessite les prix d'achat.
Si l'analyse fournisseur est fournie : integre les risques de concentration et le stock mort par fournisseur.

### PREDICTIONS DE RUPTURE
Uniquement si des alertes sont presentes. Delais < 2 semaines en JOURS, pas en semaines.

### TOP 5 ACTIONS PRIORITAIRES
5 actions classees par impact cash. Chaque action DOIT nommer les references concernees (nom exact), la quantite, le montant EUR en jeu, et le verbe d'action. Utilise les simulations fournies. JAMAIS d'action sans reference nommee.

### COUT DE L INACTION
Si fourni dans les donnees, enonce clairement le cout a 90 jours.

ARRETE-TOI apres la derniere action. PAS de section scoring. PAS de phrase de cloture."""


# ════════════════════════════════════════════════════════════════════════════
# PROMPT TERRAIN (inchange)
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_terrain():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are an experienced warehouse supervisor helping your team day-to-day.
RESPOND IN ENGLISH. Direct tone, short sentences. No financial jargon.

STRICT RULES:
1. Use ONLY the data provided. If information is not in the data, do not invent it.
2. If no annual consumption columns in the file: do NOT say "no movement for X months". Say "no consumption history in this file — cannot determine inactivity duration."
3. If weekly/monthly sales column exists: use it to identify active (sales > 0) vs inactive (sales = 0) items.
4. Each action MUST cite an EXACT REFERENCE from the file with its EXACT QUANTITY.
5. NEVER give generic advice ("check stock levels", "contact suppliers"). ONLY actions on specific references.

### WHAT IS URGENT
Items to reorder today. For each:
- Exact reference and name
- Current stock (from file)
- Sales rate (if available)
- Quantity to order
If history: flag items already out of stock last time.

### WHAT CHANGED SINCE LAST AUDIT
ONLY if historical data is available. Skip entirely if first audit.

### WHAT IS SLEEPING
Items with stock > 0 AND sales = 0 (if sales column available).
For each: exact reference, current stock, value if price available, ONE concrete action.
If no sales data: say so clearly and suggest physical count.

### WHAT TO DO NOW
IF consumption data is present: most urgent action. ONE reference, ONE justified number, ONE verb.
IF consumption data is ABSENT: do NOT order anything (you don't know the sales rate). Instead write:
"To calculate reorder needs, add consumption history (Sales_2023, Sales_2024, Sales_2025). Meanwhile, check the [X] items at zero stock: are they active stockouts or discontinued?"
NEVER invent a quantity to order without consumption data.

### YOUR 3 ACTIONS THIS WEEK
3 actions on SPECIFIC references from the file. No generalities.
IF no consumption data: actions must be verifications (count, move, check with sales team), NOT orders.
Format:
- [Exact reference]: [concrete action] — Difficulty: Easy / Medium / Hard

### SUMMARY
2 sentences max. Factual situation based on data."""

    return """Tu es un chef magasinier experimente qui aide son equipe au quotidien.
REPONDS EN FRANCAIS. Ton direct, phrases courtes. Pas de jargon financier.

REGLES STRICTES :
1. N'utilise QUE les donnees fournies. Si une information n'est pas dans les donnees, ne l'invente pas.
2. Si pas de colonnes de consommation annuelle dans le fichier : ne dis PAS "aucun mouvement depuis X mois". Dis simplement "pas d'historique de consommation dans ce fichier — impossible de determiner l'anciennete sans mouvement."
3. Si une colonne de ventes hebdomadaires ou mensuelles existe : utilise-la pour identifier les articles actifs (ventes > 0) et inactifs (ventes = 0).
4. Chaque action DOIT citer une REFERENCE EXACTE du fichier avec sa QUANTITE exacte.
5. JAMAIS de conseil generique ("verifier les stocks", "contacter les fournisseurs"). UNIQUEMENT des actions sur des references precises.

### CE QUI EST URGENT
Les articles a commander aujourd hui. Pour chaque article :
- Reference exacte et designation
- Stock actuel (du fichier)
- Rythme de vente (si dispo)
- Quantite a commander
Si historique : articles deja en rupture la derniere fois.

### CE QUI A CHANGE DEPUIS LE DERNIER AUDIT
SEULEMENT si historique disponible. Saute completement si premier audit.

### CE QUI DORT
Articles avec stock > 0 ET ventes = 0 (si colonne ventes dispo).
Pour chacun : reference exacte, stock actuel, valeur si prix dispo, et UNE action concrete (promo, retour fournisseur, deplacement zone).
Si pas de donnees de vente : dis-le clairement et suggere un comptage physique.

### A FAIRE MAINTENANT
SI les donnees de consommation sont presentes : l'action la plus urgente. UNE reference, UN chiffre justifie, UN verbe.
SI les donnees de consommation sont ABSENTES : ne commande RIEN (tu ne connais pas le rythme de vente). A la place ecris :
"Pour calculer vos besoins de reappro, ajoutez l'historique de consommation (Conso_2023, Conso_2024, Conso_2025). En attendant, verifiez les [X] articles a stock zero : sont-ils en rupture active ou en arret de commercialisation ?"
JAMAIS inventer une quantite a commander sans donnee de consommation.

### TES 3 ACTIONS POUR CETTE SEMAINE
3 actions sur des references PRECISES du fichier. Pas de generalites.
SI pas de conso : les actions doivent etre des verifications (compter, deplacer, verifier avec le commercial) pas des commandes.
Format :
- [Reference exacte] : [action concrete] — Difficulte : Facile / Moyen / Complique

### EN RESUME
2 phrases max. Situation factuelle basee sur les donnees."""


# ════════════════════════════════════════════════════════════════════════════
# PROMPT TRANSPORT (inchange — masque en V1)
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_transport():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are a Senior Road Transport and Supply Chain Strategy Auditor.
RESPOND ENTIRELY IN ENGLISH. Write in full explanatory sentences.
CRITICAL RULES:
1. Margin > 10%: OPEN with congratulations. 2. Margin 6-10%: Healthy. 3. Margin < 6%: Alert. 4. Margin < 0%: Critical.
5. NEVER invent figures. 6. CNR 2026: Long-haul 1.85-2.10 EUR/km, regional 1.40-1.65 EUR/km. 7. List routes with "- ".
### PROFITABILITY AUDIT
Verdict + worst routes + root cause. If historical: compare.
### NETWORK DIAGNOSIS
Cost/km vs CNR. Structural issues.
### WHAT TO DO - TOP PRIORITY
One sentence, most urgent action + cash impact.
### RATIONALIZATION PLAN (1-2-3)
3 strategic recommendations. STOP after last one. NO scoring."""

    return """Tu es un Auditeur Senior en Strategie Transport Routier et Supply Chain.
REPONDS EN FRANCAIS. Phrases completes et explicatives.
REGLES : 1. Marge > 10% : felicite. 2. 6-10% : sain. 3. < 6% : alerte. 4. < 0% : critique.
5. N'invente rien. 6. CNR 2026 : longue distance 1,85-2,10, regional 1,40-1,65. 7. Liste avec "- ".
### AUDIT DE RENTABILITE
Verdict + pires trajets + cause. Si historique : compare.
### DIAGNOSTIC RESEAU
Cout/km vs CNR. Problemes structurels.
### A FAIRE - PRIORITE ABSOLUE
Une phrase, action urgente + impact cash.
### PLAN DE RATIONALISATION (1-2-3)
3 recommandations. ARRETE apres la derniere. PAS de scoring."""


# ════════════════════════════════════════════════════════════════════════════
# EXTRACTION LIGNES CLES (simplifie — l'enrichissement fait le gros du travail)
# ════════════════════════════════════════════════════════════════════════════
def _extract_key_rows(df, module, lang="fr"):
    try:
        lines = []
        lbl = "KEY DATA ROWS" if lang == "en" else "LIGNES CLES DU FICHIER"
        lines.append(f"=== {lbl} ===")

        if module == "transport":
            if "Marge_Nette" in df.columns and "_CA" in df.columns:
                pires = df.nsmallest(5, "Marge_Nette")
                for _, row in pires.iterrows():
                    client_col = df.columns[0]
                    lines.append(
                        f"  - {row.get(client_col, '?')}: "
                        f"CA={row.get('_CA', 0):.0f} EUR, Cout={row.get('_CO', 0):.0f} EUR, "
                        f"Marge={row.get('Marge_Nette', 0):.0f} EUR ({row.get('Rentabilite_%', 0):.1f}%)"
                    )
        return "\n".join(lines) if len(lines) > 1 else ""
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
# NETTOYAGE SORTIE IA
# ════════════════════════════════════════════════════════════════════════════
def _strip_scoring_and_outro(texte, lang="fr"):
    if not texte:
        return texte
    pattern_scoring = r'###\s*(SCORING\s+LOGIFLO|LOGIFLO\s+SCORE)[\s\S]*?(?=###|\Z)'
    texte = re.sub(pattern_scoring, '', texte, flags=re.IGNORECASE)
    outros = [
        r'Ces recommandations visent[\s\S]*$',
        r'Je reste [àa] votre disposition[\s\S]*$',
        r'N\'h[ée]sitez pas [àa] me contacter[\s\S]*$',
        r'These recommendations aim[\s\S]*$',
        r'I remain at your disposal[\s\S]*$',
        r'Please do not hesitate[\s\S]*$',
    ]
    for pat in outros:
        texte = re.sub(pat, '', texte, flags=re.IGNORECASE | re.MULTILINE)
    return texte.rstrip() + "\n"


# ════════════════════════════════════════════════════════════════════════════
# GENERATION ANALYSE IA (point d'entree principal — V7)
# ════════════════════════════════════════════════════════════════════════════
def generate_ai_analysis(data_summary, historique_txt="", df_raw=None,
                          sector_key=None, mode_detected=None):
    lang = st.session_state.get("language", "fr")
    module = st.session_state.get("module", "stock")
    view = st.session_state.get("stock_view", "MANAGER")

    client = None
    try:
        from openai import OpenAI
        _key_oai = os.environ.get("OPENAI_API_KEY", "") or st.secrets.get("OPENAI_API_KEY", "")
        if _key_oai:
            client = OpenAI(api_key=_key_oai)
    except Exception:
        client = None

    if not sector_key:
        sector_key = detect_sector(
            df=df_raw, module=module,
            mode_detected=mode_detected or (
                st.session_state.trans_mode_detected[0]
                if st.session_state.get("trans_mode_detected") else None
            )
        )

    benchmarks = get_sector_benchmarks(sector_key, lang)

    if module == "transport":
        sys_prompt = get_prompt_transport()
    elif view == "TERRAIN":
        sys_prompt = get_prompt_terrain()
    else:
        sys_prompt = get_prompt_stock()

    # ── Construire le message user ──
    parts = []
    lbl_audit = "CURRENT AUDIT DATA" if lang == "en" else "DONNEES AUDIT ACTUEL"
    lbl_bench = "SECTOR BENCHMARKS" if lang == "en" else "BENCHMARKS SECTORIELS"
    parts.append(f"=== {lbl_audit} ===\n{data_summary}")
    parts.append(f"=== {lbl_bench} ===\n{benchmarks}")

    # ── V7 : ENRICHISSEMENT STOCK (10 regles metier) ──
    if module == "stock" and df_raw is not None:
        enrichment = _compute_stock_enrichment(df_raw, sector_key, lang)
        if enrichment:
            parts.append(enrichment)

    if historique_txt and historique_txt.strip():
        parts.append(historique_txt)
    else:
        if lang == "en":
            parts.append("=== HISTORY ===\nFirst audit -- no historical comparison. Do NOT use 'dead' or 'dormant'. ASK if high-stock levels are expected.")
        else:
            parts.append("=== HISTORIQUE ===\nPremier audit -- pas de comparaison. N'utilise PAS 'mort' ou 'dormant'. POSE LA QUESTION pour les niveaux eleves.")

    # Transport key rows (stock key rows now handled by enrichment)
    if module == "transport" and df_raw is not None:
        try:
            key_data = _extract_key_rows(df_raw, module, lang)
            if key_data:
                parts.append(key_data)
        except Exception:
            pass

    # Contexte saisonnier
    try:
        from engine.ingester import detect_periode
        _periode = detect_periode(df_raw) if df_raw is not None else None
        if _periode:
            _ctx_key = "contexte_fr" if lang == "fr" else "contexte_en"
            _ctx = _periode.get(_ctx_key)
            if _ctx:
                _lbl = _periode.get("label", "")
                parts.append(f"=== {'SEASONAL CONTEXT' if lang == 'en' else 'CONTEXTE SAISONNIER'} ===\n{_lbl}\n{_ctx}")
    except Exception:
        pass

    # Predictions rupture
    try:
        from engine.pdf_gen import predict_ruptures, format_predictions_pour_prompt
        if module == "stock" and df_raw is not None:
            _alertes = predict_ruptures(df_raw, lang=lang)
            _pred_txt = format_predictions_pour_prompt(_alertes, lang)
            if _pred_txt:
                parts.append(_pred_txt)
    except Exception:
        pass

    # Alerte BFR
    try:
        from engine.pdf_gen import compute_alerte_bfr
        if module == "stock" and df_raw is not None:
            _bfr = compute_alerte_bfr(df_raw, lang=lang)
            if _bfr.get("available") and _bfr.get("texte"):
                parts.append(f"=== {'BFR ALERT' if lang == 'en' else 'ALERTE BFR'} ===\n{_bfr['texte']}")
    except Exception:
        pass

    user_msg = "\n\n".join(parts)

    # ── APPEL OPENAI ──
    if client:
        try:
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.30, max_tokens=3000, timeout=35,
            )
            texte = r.choices[0].message.content
            try:
                texte = texte.encode('latin-1').decode('utf-8')
            except Exception:
                pass
            return _strip_scoring_and_outro(texte, lang)
        except Exception:
            pass

    # ── FALLBACK GEMINI ──
    try:
        import google.generativeai as _genai
        _key_gem = os.environ.get("GEMINI_API_KEY", "") or st.secrets.get("GEMINI_API_KEY", "")
        if _key_gem:
            _genai.configure(api_key=_key_gem)
            _gem = _genai.GenerativeModel("gemini-1.5-flash")
            _resp = _gem.generate_content(
                f"{sys_prompt}\n\n{user_msg}",
                generation_config=_genai.types.GenerationConfig(temperature=0.30, max_output_tokens=3000),
            )
            texte = _resp.text
            try:
                texte = texte.encode('latin-1').decode('utf-8')
            except Exception:
                pass
            return _strip_scoring_and_outro(texte, lang)
    except Exception:
        pass

    return _rapport_sans_ia(data_summary, sector_key or "generique", lang)


def _rapport_sans_ia(data_summary, sector_key, lang="fr"):
    benchmarks = get_sector_benchmarks(sector_key or "generique", lang)
    if lang == "en":
        return f"""### AUTOMATIC DIAGNOSIS
AI analysis temporarily unavailable.

**Computed data:**
{data_summary}

**Sector benchmarks:**
{benchmarks}

### WHAT TO DO - TOP PRIORITY
Compare your indicators to the benchmarks above. Any negative gap above 5 points requires action this week."""

    return f"""### DIAGNOSTIC AUTOMATIQUE
L analyse IA est temporairement indisponible.

**Donnees calculees :**
{data_summary}

**Benchmarks sectoriels :**
{benchmarks}

### A FAIRE - PRIORITE ABSOLUE
Comparez vos indicateurs aux benchmarks ci-dessus. Tout ecart negatif de plus de 5 points merite une action cette semaine."""
