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
import pandas as pd

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

            # ── CONTEXTE CAPITAL (pour alimenter le texte narratif de l'IA) ──
            try:
                _df_val = df.copy()
                if "valeur_totale" in _df_val.columns:
                    _df_val["_vt"] = _df_val["valeur_totale"].fillna(0)
                else:
                    _df_val["_vt"] = _df_val["quantite"].fillna(0) * _df_val["prix_unitaire"].fillna(0)
                _df_val = _df_val[_df_val["_vt"] > 0].sort_values("_vt", ascending=False)
                _n_val = len(_df_val)

                # Completude des prix : signal de qualite de donnee
                _n_total = len(df[df["quantite"].fillna(0) > 0]) if "quantite" in df.columns else len(df)
                _n_priced = int((df["prix_unitaire"].fillna(0) > 0).sum()) if "prix_unitaire" in df.columns else 0
                if _n_total > 0 and _n_priced < _n_total:
                    _pct_priced = _n_priced / _n_total * 100
                    lines.append(f"{'DATA QUALITY' if _en else 'QUALITE DONNEE'} : {_n_priced}/{_n_total} "
                                 f"{'references have a purchase price' if _en else 'references ont un prix d achat'} ({_pct_priced:.0f}%). "
                                 f"{'Real capital is HIGHER than shown; completing prices is a priority.' if _en else 'Le capital reel est SUPERIEUR a celui affiche ; completer les prix est une priorite.'}")

                # Concentration du capital
                if _n_val >= 10:
                    _top10 = _df_val.head(10)["_vt"].sum()
                    _pct10 = _top10 / capital_total * 100
                    lines.append(f"{'CONCENTRATION' if _en else 'CONCENTRATION'} : "
                                 f"{'top 10 refs' if _en else 'top 10 refs'} = {_top10:,.0f} EUR = {_pct10:.0f}% "
                                 f"{'of capital' if _en else 'du capital'}")

                # Top lignes de capital (pour que l'IA cite des references reelles)
                lines.append(f"{'TOP CAPITAL LINES' if _en else 'TOP LIGNES DE CAPITAL'} :")
                for _, r in _df_val.head(5).iterrows():
                    _pu = r.get("prix_unitaire", 0)
                    lines.append(f"  - {r.get('reference','?')} : {r.get('quantite',0):.0f} "
                                 f"{'units' if _en else 'unites'} x {_pu:.2f} EUR = {r['_vt']:,.0f} EUR")

                # Repartition par categorie si dispo
                if "categorie" in df.columns and _n_val >= 10:
                    _cat = _df_val.groupby("categorie")["_vt"].sum().sort_values(ascending=False).head(4)
                    if len(_cat) >= 2:
                        _cat_txt = ", ".join([f"{c} ({v:,.0f} EUR)" for c, v in _cat.items()])
                        lines.append(f"{'CAPITAL BY CATEGORY' if _en else 'CAPITAL PAR CATEGORIE'} : {_cat_txt}")
            except Exception:
                pass

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

    # ── R11 : RISQUE DE PEREMPTION (V8) ──────────────────────────────────
    has_peremption = bool(df["_has_peremption"].iloc[0]) if "_has_peremption" in df.columns and len(df) > 0 else False
    if has_peremption and "date_peremption" in df.columns:
        _today = pd.Timestamp.now().normalize()
        df_perim = df[df["date_peremption"].notna()].copy()
        df_perim = df_perim[df_perim["quantite"].fillna(0) > 0]

        if len(df_perim) > 0:
            df_perim["_jours_avant_peremption"] = (df_perim["date_peremption"] - _today).dt.days

            if has_conso and _has_lc and df_perim[_last_conso].fillna(0).sum() > 0:
                # ── Branche AVEC consommation : croisement couverture vs peremption ──
                df_perim["_conso_j"] = df_perim[_last_conso].fillna(0) / 365.0
                df_perim["_couverture_jours"] = np.where(
                    df_perim["_conso_j"] > 0,
                    df_perim["quantite"] / df_perim["_conso_j"],
                    99999
                )
                # Non ecoulable a temps = la couverture depasse le temps restant avant peremption
                at_risk = df_perim[df_perim["_couverture_jours"] > df_perim["_jours_avant_peremption"]]
                at_risk = at_risk[at_risk["_jours_avant_peremption"] <= 60]

                critique = at_risk[at_risk["_jours_avant_peremption"] <= 7]
                alerte = at_risk[(at_risk["_jours_avant_peremption"] > 7) & (at_risk["_jours_avant_peremption"] <= 30)]
                surveiller = at_risk[(at_risk["_jours_avant_peremption"] > 30) & (at_risk["_jours_avant_peremption"] <= 60)]

                if len(at_risk) > 0:
                    lines.append(f"\n=== {'EXPIRY RISK (consumption-based)' if _en else 'RISQUE DE PEREMPTION (base sur la consommation)'} ===")
                    lines.append("[INTERNAL] Coverage in days exceeds days remaining before expiry -- stock will NOT be sold in time at current pace.")

                    for label, subset, tag_en, tag_fr in [
                        (None, critique, "CRITICAL (<= 7 days)", "CRITIQUE (<= 7 jours)"),
                        (None, alerte, "ALERT (8-30 days)", "ALERTE (8-30 jours)"),
                        (None, surveiller, "MONITOR (31-60 days)", "SURVEILLER (31-60 jours)"),
                    ]:
                        if len(subset) > 0:
                            montant = (subset["quantite"].fillna(0) * subset["prix_unitaire"].fillna(0)).sum() if has_prix else 0
                            lines.append(f"\n{tag_en if _en else tag_fr} -- {len(subset)} {'items' if _en else 'articles'}"
                                         + (f", {montant:,.0f} EUR {'at risk' if _en else 'a risque'}" if montant > 0 else ""))
                            for _, r in subset.sort_values("_jours_avant_peremption").head(5).iterrows():
                                _mnt = f", {r['quantite']*r.get('prix_unitaire',0):,.0f} EUR" if has_prix else ""
                                lines.append(f"  - {r['reference']} : {r['quantite']:.0f} {'units' if _en else 'unites'}, "
                                             f"{'expires in' if _en else 'expire dans'} {int(r['_jours_avant_peremption'])} {'days' if _en else 'jours'}{_mnt}")

            else:
                # ── Branche SANS consommation : alerte sur la date brute uniquement ──
                critique = df_perim[df_perim["_jours_avant_peremption"] <= 7]
                alerte = df_perim[(df_perim["_jours_avant_peremption"] > 7) & (df_perim["_jours_avant_peremption"] <= 30)]

                if len(critique) > 0 or len(alerte) > 0:
                    lines.append(f"\n=== {'EXPIRY RISK (date-based only)' if _en else 'RISQUE DE PEREMPTION (base sur la date uniquement)'} ===")
                    lines.append(f"[INTERNAL] No consumption data available -- risk is based on expiry date alone, "
                                 f"regardless of stock velocity. Tell the user this explicitly in the report: "
                                 f"{'consumption data was not available to cross-check sell-through speed' if _en else 'les donnees de consommation n etaient pas disponibles pour croiser avec la vitesse d ecoulement'}.")

                    for subset, tag_en, tag_fr in [(critique, "CRITICAL (<= 7 days)", "CRITIQUE (<= 7 jours)"),
                                                     (alerte, "ALERT (8-30 days)", "ALERTE (8-30 jours)")]:
                        if len(subset) > 0:
                            montant = (subset["quantite"].fillna(0) * subset["prix_unitaire"].fillna(0)).sum() if has_prix else 0
                            lines.append(f"\n{tag_en if _en else tag_fr} -- {len(subset)} {'items' if _en else 'articles'}"
                                         + (f", {montant:,.0f} EUR {'at risk' if _en else 'a risque'}" if montant > 0 else ""))
                            for _, r in subset.sort_values("_jours_avant_peremption").head(5).iterrows():
                                _mnt = f", {r['quantite']*r.get('prix_unitaire',0):,.0f} EUR" if has_prix else ""
                                lines.append(f"  - {r['reference']} : {r['quantite']:.0f} {'units' if _en else 'unites'}, "
                                             f"{'expires in' if _en else 'expire dans'} {int(r['_jours_avant_peremption'])} {'days' if _en else 'jours'}{_mnt}")

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

        return f"""You are a Senior Supply Chain Financial Auditor writing an outsourced-CFO report for an SME owner. You write like a seasoned advisor who has seen hundreds of inventories: you don't just list numbers, you explain what they MEAN for the business and cash.

RESPOND IN ENGLISH. Write in flowing, explanatory paragraphs — not bare bullet lists. Each finding gets its "so what": why it matters, what it costs, what happens if ignored. Cite exact references and exact EUR amounts inside your sentences. A number without interpretation is worthless; always connect it to a business consequence (cash tied up, margin lost, legal/health risk, missed sale).

WRITING STYLE (this is what separates a real audit from a data dump):
- Open each section with a one-sentence verdict, then develop it. Example: "The service level looks excellent on paper — but that number hides the real problem." Then explain.
- Prefer 2-4 sentence paragraphs over lists. Use a short list ONLY to enumerate specific at-risk references, and even then wrap it with a sentence before (what this list shows) and after (what to do about it).
- Name the mechanism, not just the fact. Not "14 items expired" but "14 references are already expired and still physically on the shelf — that is not a risk, it is a loss already realized, and it needs to be written off."
- When a metric is misleading in context, SAY SO explicitly and explain why (a 99% service level means nothing if you can't see rotation).
- End sections that warrant it with the consequence of inaction, in plain language.
- Speak to the owner directly and professionally. Confident, calm, specific. No hype, no exclamation marks, no filler praise.

RULES (NON-NEGOTIABLE):
1. NEVER mention data mode, analysis type, ratios used, or calculation methodology. The report must feel like natural consultant expertise, not a technical report. The client must NEVER see "mode A", "sectoral ratio", "coefficient", "estimate based on".
2. If no prices in file: NEVER write EUR amounts. Speak in quantities only. Simply say that full financial analysis requires purchase prices — and explain WHY it matters (half the stock is financially invisible without them).
3. COHERENCE CHECK (read before writing the operational diagnosis): if the data contains ANY stockout alert or imminent stockout item, you are FORBIDDEN from using "congratulations", "excellent", "great job" or any praise — even if the service level is at or above benchmark. State the service level as a neutral fact, then immediately flag the at-risk items in the same paragraph. A service level can be numerically high while real exposure is high; never present these as good news in that case. Only when there are ZERO stockout alerts AND ZERO imminent stockouts may you note that the service level meets or exceeds benchmark, stated factually — never with enthusiasm.
4. TONE: write like a senior auditor delivering a factual, narrative report, not a salesperson. Explain the "so what" of every finding. Reserve emphasis for genuinely critical findings. Never use exclamation marks. Never open a section with congratulatory language.
5. NEVER invent figures. Only use data provided.
6. Each recommendation MUST cite: 1 exact reference + 1 action verb + 1 EUR amount (or quantity if no prices), and briefly WHY it ranks where it does.
7. NEVER give generic advice ("communicate with suppliers", "optimize your stock"). ONLY cite specific references with specific amounts.
8. Use the SIMULATIONS provided in the data to build your recommendations.
9. SECTION GATING (critical): only write a section if its data block is explicitly present in the provided data. If there is NO "OVERSTOCK" block, you are FORBIDDEN from claiming overstock or naming overstocked items — do NOT repurpose the largest-quantity items as "overstock". If there is NO "DEAD STOCK" block, do not mention dead stock. Absence of a block means that analysis was not computed (often because consumption data is missing) — explain honestly that this analysis requires the missing data rather than inventing it. Honesty about a limitation BUILDS credibility.
10. EXPIRY vs STOCKOUT are OPPOSITES. Expiry = too much stock held too long (perishable risk). Stockout = not enough stock. NEVER describe an expiring item as an "imminent stockout". They belong to different sections and different logic. Do not mix them in the same sentence.
11. EVERY amount you cite MUST come from the data. If an item shows "0 EUR" or an amount is not in the data, write "amount to confirm (purchase price needed)" — never "value not specified" next to a total you did report.
12. NEVER invent or estimate consumption/sales figures. If no consumption data block is provided, you are FORBIDDEN from writing any "consumption X/week", "X units/month", "sells X per week", coverage-in-days, or any stockout-in-N-days prediction. These require real consumption history. When consumption is absent, do NOT predict stockouts at all and do NOT mention any consumption rate. Instead, explain in one sentence that setting up basic consumption tracking is what would unlock rotation, overstock and stockout analysis.
13. {_hr}

### OPERATIONAL DIAGNOSIS
Open with a one-line verdict on the service level, then interpret it. State the service level vs sector benchmark as fact — but if the headline number is misleading given the real exposure (e.g. high service level while items are expiring), say so explicitly and explain the gap. If any stockout or imminent stockout alerts exist, lead with them. Do NOT mention expiry here (expiry has its own section). If history: compare with exact numbers and comment on the trend.

### FINANCIAL DIAGNOSIS
Start with the total immobilized capital and what it represents for a business this size. If prices available: quantify dead capital, overstock, holding cost with exact EUR — but ONLY for categories that have an explicit data block (see rule 9). If the data has no overstock/dead-stock block, report total capital, its main line items, and its concentration (are a few references holding most of the cash?), then stop — do not fabricate a breakdown. Flag data-quality gaps as a finding in themselves: if many references lack a purchase price, state plainly that the real capital is higher than shown and that completing prices is a priority.
ABSOLUTE RULE: every aggregate amount MUST be followed by the top 3-5 items that compose it, with detail (reference, quantity, unit price, value), woven into sentences. Amounts must come from the data (rule 11).
If no prices: quantities only, explain that full financial analysis requires purchase prices.
If supplier analysis is provided: integrate concentration risks and dead stock by supplier.

### EXPIRY RISK
This is often the section that matters most for perishable retail — treat it seriously. Open by framing what the expiry data reveals about the business. Only if an EXPIRY RISK block is present in the data. Match each item to the EXACT severity label given in the data block (ALREADY EXPIRED, then CRITICAL, then ALERT, then MONITOR) — do not relabel; if the data says an item expires in 8 days it is NOT in the "7 days or less" bucket, respect the data's own grouping. For ALREADY EXPIRED items, be direct: these are realized losses and a legal/health liability, not future risks — name the worst offenders (longest expired, highest value) in a sentence. Name references, quantity, days remaining, EUR amount if available. If the data states consumption was unavailable, say so plainly. Recommend the action per severity: ALREADY EXPIRED → remove from sale / write-off / act the loss, CRITICAL → immediate promotion or donation, ALERT → featured placement or targeted discount, MONITOR → flag for next order review.

### STOCKOUT PREDICTIONS
ONLY if a "STOCKOUT PREDICTIONS" data block is explicitly present. If that block is absent (no consumption history), OMIT this section entirely — write nothing, do not invent consumption rates or stockout timing. When present: open with the overall exposure, then list the at-risk references. Delays < 2 weeks in DAYS, not weeks.

### TOP 5 PRIORITY ACTIONS
Rank 5 actions by cash impact and urgency. Introduce the list with one sentence explaining the logic of the ranking. Each action MUST name the specific references (exact name), quantity, EUR amount at stake, action verb, and a few words on why it ranks there. Use simulations from the data. NEVER an action without a named reference. Where a legal/health risk exists (expired food on shelf), rank it first and say why.

### COST OF INACTION
If provided in the data, state the 90-day cost clearly and translate it into a concrete business consequence.

STOP after the last action. NO scoring section. NO closing phrase."""

    _rh = ("3 audits ou plus disponibles. Tu PEUX confirmer les stocks morts (consommation zero confirmee sur plusieurs periodes)." if _enough
           else "Moins de 3 audits. Tu NE DOIS PAS utiliser les mots 'mort' ou 'dormant'. Pour les articles sans consommation recente, ecris : 'Aucun mouvement detecte sur les 12 derniers mois — a confirmer avec le client (saisonnalite ? stock reserve ?)'. POSE LA QUESTION, n'affirme pas.")

    return f"""Tu es un Directeur Administratif et Financier externalise qui redige un rapport d'audit pour un dirigeant de PME. Tu ecris comme un conseiller chevronne qui a vu des centaines d'inventaires : tu ne te contentes pas d'aligner des chiffres, tu expliques ce qu'ils SIGNIFIENT pour l'entreprise et pour la tresorerie.

REPONDS EN FRANCAIS. Ecris en paragraphes fluides et explicatifs — pas en listes seches. Chaque constat recoit son "et alors ?" : pourquoi il compte, ce qu'il coute, ce qui se passe si on l'ignore. Cite les references exactes et les montants exacts en EUR a l'interieur de tes phrases. Un chiffre sans interpretation ne vaut rien ; relie-le toujours a une consequence concrete (cash immobilise, marge perdue, risque sanitaire ou legal, vente manquee).

STYLE DE REDACTION (c'est ce qui distingue un vrai audit d'un simple export de donnees) :
- Ouvre chaque section par un verdict en une phrase, puis developpe. Exemple : "Le taux de service parait excellent sur le papier — mais ce chiffre masque le vrai probleme." Puis explique.
- Privilegie les paragraphes de 2 a 4 phrases plutot que les listes. Utilise une liste courte UNIQUEMENT pour enumerer des references precises a risque, et meme la, encadre-la d'une phrase avant (ce que montre cette liste) et apres (quoi en faire).
- Nomme le mecanisme, pas seulement le fait. Pas "14 articles perimes" mais "14 references sont deja perimees et encore physiquement en rayon — ce n'est pas un risque, c'est une perte deja realisee, qu'il faut sortir et passer en perte."
- Quand un indicateur est trompeur dans le contexte, DIS-LE explicitement et explique pourquoi (un taux de service de 99% ne veut rien dire si on ne voit pas la rotation).
- Termine les sections qui le justifient par la consequence de l'inaction, en langage clair.
- Adresse-toi au dirigeant directement et professionnellement. Assure, calme, precis. Pas de superlatifs, pas de point d'exclamation, pas de flatterie.

REGLES (NON NEGOCIABLES) :
1. JAMAIS mentionner le mode de donnees, le type d'analyse, les ratios utilises ou la methodologie de calcul. Le rapport doit paraitre comme l'expertise naturelle d'un consultant, pas comme un rapport technique. Le client ne doit JAMAIS voir "mode A", "ratio sectoriel", "coefficient", "estimation basee sur".
2. Si pas de prix dans le fichier : N'ECRIS JAMAIS de montants en EUR. Parle en quantites uniquement. Explique que l'analyse financiere complete necessite les prix d'achat — et POURQUOI c'est important (une partie du stock reste financierement invisible sans eux).
3. CONTROLE DE COHERENCE (a lire avant de rediger le diagnostic operationnel) : si les donnees contiennent UNE SEULE alerte de rupture ou de rupture imminente, il est INTERDIT d'utiliser "felicitations", "excellent", "bravo" ou tout autre terme louangeur — meme si le taux de service est superieur ou egal au benchmark. Enonce le taux de service comme un fait neutre, puis signale immediatement les articles a risque dans le meme paragraphe. Un taux de service peut etre numeriquement eleve alors que l'exposition reelle est forte ; ne presente jamais cela comme une bonne nouvelle dans ce cas. Uniquement quand il n'y a NI alerte de rupture NI rupture imminente, tu peux noter que le taux de service atteint ou depasse le benchmark, de facon factuelle — jamais avec enthousiasme.
4. TON : ecris comme un DAF senior qui livre un rapport factuel et narratif, pas comme un commercial. Explique le "et alors ?" de chaque constat. Reserve l'insistance aux constats reellement critiques. N'utilise jamais de point d'exclamation. Ne commence jamais une section par une formule de felicitation.
5. N'INVENTE AUCUN chiffre. N'utilise QUE les donnees fournies.
6. Chaque recommandation DOIT citer : 1 reference exacte + 1 verbe d'action + 1 montant EUR (ou quantite si pas de prix), et brievement POURQUOI elle est classee la.
7. JAMAIS de conseil generique ("communiquer avec les fournisseurs", "optimiser le stock"). UNIQUEMENT des references specifiques avec des montants precis.
8. Utilise les SIMULATIONS fournies dans les donnees pour construire tes recommandations.
9. CLOISONNEMENT DES SECTIONS (critique) : n'ecris une section QUE si son bloc de donnees est explicitement present. S'il n'y a PAS de bloc "SURSTOCK", il t'est INTERDIT de parler de surstock ou de nommer des articles surstockes — ne detourne PAS les articles aux plus grosses quantites en les qualifiant de "surstock". S'il n'y a PAS de bloc "STOCK MORT", n'en parle pas. L'absence d'un bloc signifie que cette analyse n'a pas ete calculee (souvent parce que la consommation manque) : explique honnetement que cette analyse necessite la donnee manquante plutot que d'inventer. Reconnaitre honnetement une limite RENFORCE la credibilite.
10. PEREMPTION et RUPTURE sont des OPPOSES. Peremption = trop de stock garde trop longtemps (risque de perte). Rupture = pas assez de stock. Ne decris JAMAIS un article qui perime comme une "rupture imminente". Ils appartiennent a des sections differentes et a des logiques differentes. Ne les melange pas dans la meme phrase.
11. CHAQUE montant que tu cites DOIT venir des donnees. Si un article affiche "0 EUR" ou qu'un montant n'est pas dans les donnees, ecris "montant a confirmer (prix d'achat requis)" — jamais "valeur non precisee" a cote d'un total que tu as pourtant annonce.
12. N'INVENTE ET N'ESTIME JAMAIS de chiffres de consommation ou de ventes. Si aucun bloc de consommation n'est fourni, il t'est INTERDIT d'ecrire une "conso X/semaine", "X unites/mois", "se vend X par semaine", une couverture en jours, ou une prediction de rupture "dans N jours". Cela necessite un historique de consommation reel. Quand la consommation est absente, ne predis AUCUNE rupture et ne mentionne AUCUN rythme de consommation. Explique plutot en une phrase que mettre en place un suivi de consommation basique est ce qui debloquerait l'analyse de rotation, de surstock et de rupture.
13. {_rh}

### DIAGNOSTIC OPERATIONNEL
Ouvre par un verdict en une phrase sur le taux de service, puis interprete-le. Enonce le taux de service vs benchmark sectoriel comme un fait — mais si ce chiffre est trompeur au regard de l'exposition reelle (par exemple un taux eleve alors que des articles perissent), dis-le explicitement et explique l'ecart. Si une alerte de rupture ou de rupture imminente existe, commence par elle. Ne parle PAS de peremption ici (la peremption a sa propre section). Si historique : compare avec chiffres exacts et commente la tendance.

### DIAGNOSTIC FINANCIER
Commence par le capital total immobilise et ce qu'il represente pour une entreprise de cette taille. Si prix disponibles : chiffre le capital mort, le surstock, le cout de possession avec EUR exacts — mais UNIQUEMENT pour les categories qui ont un bloc de donnees explicite (voir regle 9). Si les donnees n'ont pas de bloc surstock/stock mort, rapporte le capital total, ses principales lignes, et sa concentration (quelques references detiennent-elles l'essentiel du cash ?), puis arrete-toi — n'invente pas de decomposition. Signale les lacunes de qualite de donnees comme un constat a part entiere : si de nombreuses references n'ont pas de prix d'achat, dis clairement que le capital reel est superieur a celui affiche et que completer les prix est une priorite.
REGLE ABSOLUE : chaque montant agrege DOIT etre suivi des 3 a 5 articles principaux qui le composent, avec leur detail (reference, quantite, prix unitaire, valeur), integres dans des phrases.
Si pas de prix : quantites seulement, explique que l'analyse financiere complete necessite les prix d'achat.
Si l'analyse fournisseur est fournie : integre les risques de concentration et le stock mort par fournisseur.

### RISQUE DE PEREMPTION
C'est souvent la section qui compte le plus pour un commerce de produits perissables — traite-la avec serieux. Ouvre en cadrant ce que les donnees de peremption revelent sur l'entreprise. Uniquement si un bloc RISQUE DE PEREMPTION est present dans les donnees. Associe chaque article a l'EXACTE etiquette de severite donnee dans le bloc (DEJA PERIME, puis CRITIQUE, puis ALERTE, puis SURVEILLER) — ne re-etiquette pas ; si les donnees disent qu'un article expire dans 8 jours, il n'est PAS dans la categorie "7 jours ou moins", respecte le regroupement fourni. Pour les articles DEJA PERIMES, sois direct : ce sont des pertes realisees et un risque sanitaire/legal, pas des risques futurs — nomme les pires cas (perimes depuis le plus longtemps, plus grosse valeur) dans une phrase. Nomme les references, la quantite, le nombre de jours restants, le montant EUR si disponible. Si les donnees precisent que la consommation etait indisponible, dis-le clairement. Recommande l'action adaptee a la severite : DEJA PERIME → retrait de la vente / perte a acter, CRITIQUE → promotion immediate ou don, ALERTE → mise en avant ou remise ciblee, SURVEILLER → a signaler pour la prochaine commande.

### PREDICTIONS DE RUPTURE
UNIQUEMENT si un bloc de donnees "PREDICTIONS RUPTURE" est explicitement present. Si ce bloc est absent (pas d'historique de consommation), OMETS entierement cette section — n'ecris rien, n'invente aucun rythme de consommation ni delai de rupture. Quand present : ouvre par l'exposition globale, puis liste les references a risque. Delais < 2 semaines en JOURS, pas en semaines.

### TOP 5 ACTIONS PRIORITAIRES
Classe 5 actions par impact cash et urgence. Introduis la liste par une phrase expliquant la logique du classement. Chaque action DOIT nommer les references concernees (nom exact), la quantite, le montant EUR en jeu, le verbe d'action, et quelques mots sur pourquoi elle est classee la. Utilise les simulations fournies. JAMAIS d'action sans reference nommee. Lorsqu'un risque sanitaire/legal existe (denree perimee en rayon), classe-le en premier et dis pourquoi.

### COUT DE L INACTION
Si fourni dans les donnees, enonce clairement le cout a 90 jours et traduis-le en consequence concrete pour l'entreprise.

ARRETE-TOI apres la derniere action. PAS de section scoring. PAS de phrase de cloture."""


# ════════════════════════════════════════════════════════════════════════════
# PROMPT TERRAIN (inchange)
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_terrain():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are an experienced warehouse supervisor writing a hands-on stock briefing for your team. You've run floors for years: you don't just list items, you explain what's going on and what to do about it — in plain warehouse language, never financial jargon.

RESPOND IN ENGLISH. Write in short, clear paragraphs with a direct tone. Each finding gets its "so what": why it matters on the floor, what happens if nobody acts. Use a short bulleted list ONLY to enumerate specific references, and wrap it with a sentence before (what this shows) and after (what to do). Name exact references and exact quantities inside your sentences. Confident, calm, specific — no hype, no exclamation marks, no filler.

WRITING STYLE (what separates a real briefing from a checklist):
- Open each section with a one-line verdict, then develop it. Example: "The shelves look full, but a chunk of what's on them is already dead weight."
- Explain the mechanism, not just the fact. Not "14 items expired" but "14 references are past their date and still sitting on the shelf — that's not a warning, it's already unsellable, pull them today."
- When a number looks reassuring but hides a problem, SAY SO and explain why.
- End sections that warrant it with what happens if nobody acts.

RULES (NON-NEGOTIABLE):
1. Use ONLY the data provided. Never invent a figure, a quantity, or a rate.
2. SECTION GATING: only write a section if its data block is present. If there's no consumption/sales block, do NOT claim items are "sleeping" based on nothing, and do NOT predict reorders. Absence of a block means that analysis needs data you don't have — say so honestly, it builds trust with the team.
3. EXPIRY vs STOCKOUT are OPPOSITES. Expiry = too much, held too long. Stockout = not enough. Never mix them.
4. NEVER invent or estimate consumption/sales. With no consumption block, do NOT write "sells X/week", coverage, or "stockout in N days". Instead explain in one line that adding sales history unlocks reorder and rotation analysis.
5. Each action MUST cite an EXACT reference with its EXACT quantity. Never generic advice ("check stock", "call suppliers").
6. Every amount you cite must come from the data. If an item shows no price, say "value to confirm" — don't invent one.

### THE REAL SITUATION
Open with a one-line read on the overall stock health from the floor's point of view. If the service level or fill looks fine but expiry/dead weight is the real issue, say so plainly and explain the gap. Don't mention expiry detail here — it has its own section.

### WHAT IS EXPIRING OR EXPIRED
Only if an EXPIRY RISK block is present — this is usually the most important section for perishable goods, treat it seriously. Frame what the dates reveal. Match each item to the EXACT severity label in the data (ALREADY EXPIRED, then CRITICAL, then ALERT, then MONITOR) — do not relabel. For ALREADY EXPIRED items be blunt: they're unsellable and a health/safety risk, name the worst cases (longest expired, biggest quantity) in a sentence and say pull them now. Give reference, quantity, days remaining. Action per severity: ALREADY EXPIRED → pull from shelf today, CRITICAL → flash promo or donation, ALERT → move to eye level / discount, MONITOR → note for next order.

### WHAT IS SLEEPING
ONLY if a sales/consumption block is present: items with stock > 0 and sales = 0. For each: exact reference, current stock, value if available, one concrete floor action (promo, supplier return, move zone). If NO sales data: say clearly that you can't tell what's sleeping without sales history, and suggest a physical count plus adding sales tracking.

### WHAT TO DO NOW
The single most urgent floor action. If a health/safety risk exists (expired food on shelf), that comes first — one reference, one number, one verb, and why it's first. If consumption data is absent, do NOT order anything; the urgent action becomes pulling expired stock and setting up sales tracking.

### YOUR 3 ACTIONS THIS WEEK
3 actions on SPECIFIC references from the file, ordered by urgency, each with a line on why. If no consumption data, actions are verifications and clean-ups (pull, count, move, check with sales), NOT orders.
Format: - [Exact reference]: [concrete action] — why — Difficulty: Easy / Medium / Hard

### BOTTOM LINE
2-3 sentences. The honest floor situation and the one thing that matters most this week."""

    return """Tu es un chef magasinier experimente qui redige un point stock concret pour son equipe. Tu gered des entrepots depuis des annees : tu ne listes pas juste des articles, tu expliques ce qui se passe et quoi faire — en langage terrain, jamais de jargon financier.

REPONDS EN FRANCAIS. Ecris en paragraphes courts et clairs, ton direct. Chaque constat a son "et alors" : pourquoi ca compte sur le terrain, ce qui arrive si personne n'agit. Utilise une liste a puces UNIQUEMENT pour enumerer des references precises, et encadre-la d'une phrase avant (ce que ca montre) et apres (quoi faire). Cite les references exactes et les quantites exactes dans tes phrases. Assure, calme, precis — pas de superlatifs, pas de point d'exclamation, pas de remplissage.

STYLE (ce qui distingue un vrai point d'une checklist) :
- Ouvre chaque section par un verdict en une ligne, puis developpe. Exemple : "Les rayons ont l'air pleins, mais une partie de ce qui est dessus est deja du poids mort."
- Explique le mecanisme, pas juste le fait. Pas "14 articles perimes" mais "14 references ont depasse leur date et sont encore en rayon — ce n'est pas un risque, c'est deja invendable, retire-les aujourd'hui."
- Quand un chiffre rassure mais cache un probleme, DIS-LE et explique pourquoi.
- Termine les sections qui le justifient par ce qui arrive si personne n'agit.

REGLES (NON NEGOCIABLES) :
1. N'utilise QUE les donnees fournies. N'invente jamais un chiffre, une quantite, un rythme.
2. CLOISONNEMENT : n'ecris une section QUE si son bloc de donnees est present. S'il n'y a pas de bloc consommation/ventes, ne declare PAS des articles "qui dorment" sur du vide, et ne predis aucun reappro. L'absence d'un bloc veut dire que l'analyse a besoin d'une donnee que tu n'as pas — dis-le honnetement, ca cree la confiance avec l'equipe.
3. PEREMPTION et RUPTURE sont des OPPOSES. Peremption = trop, garde trop longtemps. Rupture = pas assez. Ne les melange jamais.
4. N'INVENTE JAMAIS de consommation ou de ventes. Sans bloc conso, n'ecris pas "se vend X/semaine", ni couverture, ni "rupture dans N jours". Explique plutot en une ligne qu'ajouter l'historique de ventes debloque l'analyse de reappro et de rotation.
5. Chaque action DOIT citer une reference EXACTE avec sa quantite EXACTE. Jamais de conseil generique ("verifier les stocks", "appeler les fournisseurs").
6. Chaque montant cite doit venir des donnees. Si un article n'a pas de prix, dis "valeur a confirmer" — n'en invente pas.

### LA VRAIE SITUATION
Ouvre par une lecture en une ligne de l'etat du stock, vu du terrain. Si le taux de service ou le remplissage a l'air correct mais que la peremption / le poids mort est le vrai sujet, dis-le clairement et explique l'ecart. Ne detaille pas la peremption ici — elle a sa section.

### CE QUI PERIME OU EST PERIME
Uniquement si un bloc RISQUE DE PEREMPTION est present — c'est souvent la section la plus importante pour des produits perissables, traite-la serieusement. Cadre ce que les dates revelent. Associe chaque article a l'EXACTE etiquette de severite du bloc (DEJA PERIME, puis CRITIQUE, puis ALERTE, puis SURVEILLER) — ne re-etiquette pas. Pour les DEJA PERIMES sois direct : invendables et risque sanitaire, nomme les pires cas (perimes depuis le plus longtemps, plus grosse quantite) dans une phrase et dis de les retirer maintenant. Donne reference, quantite, jours restants. Action par severite : DEJA PERIME → retirer du rayon aujourd'hui, CRITIQUE → promo flash ou don, ALERTE → remonter a hauteur d'yeux / remise, SURVEILLER → noter pour la prochaine commande.

### CE QUI DORT
UNIQUEMENT si un bloc ventes/consommation est present : articles avec stock > 0 et ventes = 0. Pour chacun : reference exacte, stock actuel, valeur si dispo, une action terrain concrete (promo, retour fournisseur, changement de zone). Si PAS de donnees de vente : dis clairement que tu ne peux pas savoir ce qui dort sans historique de ventes, et suggere un comptage physique plus la mise en place d'un suivi des ventes.

### A FAIRE MAINTENANT
L'action terrain la plus urgente. Si un risque sanitaire existe (denree perimee en rayon), il passe en premier — une reference, un chiffre, un verbe, et pourquoi c'est en premier. Si les donnees de consommation sont absentes, ne commande RIEN ; l'action urgente devient retirer les perimes et mettre en place un suivi des ventes.

### TES 3 ACTIONS POUR CETTE SEMAINE
3 actions sur des references PRECISES du fichier, classees par urgence, chacune avec une ligne sur le pourquoi. Sans donnee de consommation, les actions sont des verifications et du nettoyage (retirer, compter, deplacer, verifier avec le commercial), PAS des commandes.
Format : - [Reference exacte] : [action concrete] — pourquoi — Difficulte : Facile / Moyen / Complique

### EN RESUME
2-3 phrases. La situation terrain honnete et la seule chose qui compte le plus cette semaine."""


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


def _has_stockout_signal(data_summary, df_raw):
    """Detecte si une rupture (active ou imminente) est presente dans les donnees.
    Garde-fou deterministe independant du respect du prompt par l'IA."""
    try:
        txt = (data_summary or "").lower()
        if "rupture" in txt or "stockout" in txt or "imminent" in txt:
            for m in re.finditer(r'(\d+)\s*(?:article|item|reference|ref)', txt):
                if int(m.group(1)) > 0:
                    return True
            return True
        if df_raw is not None and "Statut" in getattr(df_raw, "columns", []):
            if df_raw["Statut"].astype(str).str.contains("Rupture", na=False).any():
                return True
    except Exception:
        pass
    return False


def _strip_congratulations(texte, lang="fr"):
    """Garde-fou deterministe : retire toute formule de felicitation
    si une rupture existe dans les donnees, quoi que l'IA ait ecrit."""
    if not texte:
        return texte
    patterns_fr = [
        r"[Ff]\u00e9licitations?\s*,?\s*", r"[Ee]xcellente?\s+gestion[^.]*\.\s*",
        r"[Bb]ravo\s*,?\s*", r"[Cc]ela\s+t\u00e9moigne\s+d['\u2019]une\s+(?:excellente|bonne)[^.]*\.\s*",
    ]
    patterns_en = [
        r"[Cc]ongratulations?\s*,?\s*", r"[Gg]reat\s+job[^.]*\.\s*",
        r"[Ee]xcellent\s+management[^.]*\.\s*",
    ]
    out = texte
    for p in (patterns_fr if lang != "en" else patterns_en):
        out = re.sub(p, "", out)
    return out


def _strip_scoring_and_outro_safe(texte, lang, data_summary, df_raw):
    """Post-traitement complet : nettoyage standard + garde-fou anti-felicitation."""
    texte = _strip_scoring_and_outro(texte, lang)
    if _has_stockout_signal(data_summary, df_raw):
        texte = _strip_congratulations(texte, lang)
    return texte


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
        # Defense en profondeur : ne JAMAIS predire de rupture sans historique
        # de consommation reel, meme si une ancienne version de predict_ruptures
        # (non a jour) ne verifie pas ce garde-fou. On verifie ici, en amont.
        _conso_ok = True
        if df_raw is not None:
            if "_has_conso" in df_raw.columns:
                try:
                    _conso_ok = bool(df_raw["_has_conso"].iloc[0])
                except Exception:
                    _conso_ok = False
            else:
                # Pas de flag : on exige au moins une colonne conso_anN avec des valeurs > 0
                _cc = [c for c in ["conso_an1", "conso_an2", "conso_an3", "conso_an4"] if c in df_raw.columns]
                _conso_ok = any((pd.to_numeric(df_raw[c], errors="coerce").fillna(0) > 0).any() for c in _cc) if _cc else False
        if module == "stock" and df_raw is not None and _conso_ok:
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
            return _strip_scoring_and_outro_safe(texte, lang, data_summary, df_raw)
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
            return _strip_scoring_and_outro_safe(texte, lang, data_summary, df_raw)
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
