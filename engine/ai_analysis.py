# -*- coding: utf-8 -*-
"""
Logiflo - engine/ai_analysis.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generation d'analyses IA (OpenAI + Gemini fallback)
Version 6.1 (mai 2026) — fix f-string prompts + V1 routier + ton compagnon

Corrections V6.1 :
- FIX CRITIQUE : prompts convertis en f-strings (les regles historiques
  etaient envoyees en texte brut "{_regle_historique}" au lieu d'etre
  interpolees → la logique dormant/historique ne s'appliquait jamais)
- Transport prompt adapte pour V1 routier uniquement
- Ton plus "compagnon logistique" dans les prompts
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import re
import os
import datetime

from config.sectoral_db import detect_sector, get_sector_benchmarks
from config.translations import _
from services.supabase_client import get_historique_audits


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
            if d1 is not None:
                lines.append(f"  Net margin: {'improving' if d1 > 0 else 'declining'} ({d1:+.1f}%)")
            if d2 is not None:
                lines.append(f"  Profitability: {'improving' if d2 > 0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None:
                lines.append(f"  Toxic routes: {'improving' if d3 < 0 else 'worsening'} ({d3:+.1f}%)")
        else:
            if d1 is not None:
                lines.append(f"  Capital/Items: {d1:+.1f}%")
            if d2 is not None:
                lines.append(f"  Service level: {'improving' if d2 > 0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None:
                lines.append(f"  Stock-outs: {'worsening' if d3 > 0 else 'improving'} ({d3:+.1f}%)")
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
            if d1 is not None:
                lines.append(f"  Marge nette : {'en hausse' if d1 > 0 else 'en baisse'} ({d1:+.1f}%)")
            if d2 is not None:
                lines.append(f"  Taux rentabilite : {'en hausse' if d2 > 0 else 'en baisse'} ({d2:+.1f}%)")
            if d3 is not None:
                lines.append(f"  Trajets toxiques : {'en hausse' if d3 > 0 else 'en baisse'} ({d3:+.1f}%)")
        else:
            if d1 is not None:
                lines.append(f"  Capital/Articles : {d1:+.1f}%")
            if d2 is not None:
                lines.append(f"  Taux de service : {'en amelioration' if d2 > 0 else 'en degradation'} ({d2:+.1f}%)")
            if d3 is not None:
                lines.append(f"  Ruptures : {'en hausse' if d3 > 0 else 'en baisse'} ({d3:+.1f}%)")
        lines.append("=== FIN DONNEES HISTORIQUES ===\n")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# PROMPTS STOCK
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_stock():
    """Prompt pour l'analyse stock (Manager). FIX V6.1 : f-string."""
    lang = st.session_state.get("language", "fr")

    # Verifier le nombre d'audits historiques pour la logique dormant
    _n_audits = 0
    try:
        _uid = st.session_state.get("current_user", "")
        _hist_check = get_historique_audits(_uid, "stock", n=10)
        if _hist_check:
            _n_audits = _hist_check.get("n_audits", 0)
    except Exception:
        _n_audits = 0
    _assez_historique = _n_audits >= 3

    if lang == "en":
        _history_rule = (
            "HISTORICAL DATA: 3+ audits available. You CAN identify dormant stock and draw confirmed conclusions."
            if _assez_historique else
            "HISTORICAL DATA: fewer than 3 audits available. You MUST NOT use the word 'dormant'. For high-stock references, ASK the user if these levels are expected (seasonal stockpile, major upcoming order)."
        )

        _dormant_rule = (
            "If 3+ audits: identify confirmed dormant stock with zero consumption."
            if _assez_historique else
            "First/second audit: DO NOT use 'dormant'. For high-stock references, write: 'Several references show high stock quantities — we need to confirm whether these levels are expected (seasonal buildup, major upcoming order) or indicate overstock.' Then list references. Ask the question."
        )

        return f"""MANDATORY FORMAT: You MUST start EACH section with exactly three hashes followed by a space, like this: "### PROFITABILITY AUDIT". Without the three hashes, the visual rendering is broken. This rule is NON-NEGOTIABLE and applies to the 4 sections: "### OPERATIONAL DIAGNOSIS", "### FINANCIAL DIAGNOSIS AND STOCK ANALYSIS", "### WHAT TO DO - TOP PRIORITY", "### IMMEDIATE ACTION PLAN (1-2-3)".

You are a trusted Senior Supply Chain Auditor who helps SME leaders understand and optimize their stock.
PROFILE: 40+ years of experience in international logistics and supply chain finance, across distribution, industry, retail and pharma.
You write like a senior consultant who genuinely cares about the business: professional, precise, contextualised, balanced between expertise and accessibility. You explain the "why" behind every diagnosis.
RESPOND ENTIRELY IN ENGLISH. Write in full explanatory sentences. Build arguments.

TECHNICAL VOCABULARY TO USE NATURALLY: stock coverage, BFR/working capital, cash conversion, service level, stockout rate, dead stock, overstock, SKU, rotation, DIO, OTIF, safety stock, reorder point.

{_history_rule}

CRITICAL RULES:
1. If tied-up capital is 0 EUR OR prices missing: NEVER write figures in EUR. Speak in quantities, coverage in weeks. Say clearly that financial analysis requires purchase prices.
2. If service level >= sectoral benchmark: OPEN with congratulations BEFORE pointing to improvement areas.
3. NEVER invent figures.
4. List more than 2 references? Each on its own line starting with "- ".

### OPERATIONAL DIAGNOSIS
Open with a full contextualised sentence situating the service level vs sectoral benchmark.
Then introduce critical references and list them.
If historical data: compare to previous audit with exact numbers.

### FINANCIAL DIAGNOSIS AND STOCK ANALYSIS
CASE A — prices available, capital > 0:
Quantify tied-up capital, put it in perspective against sectoral norms (DIO, BFR weight).
{_dormant_rule}

CASE B — no prices or capital = 0:
Open transparently: "The file does not include purchase prices. Financial reading is unavailable but operational analysis remains fully valid."
Pivot to quantity-based analysis. Ask questions.

### STOCKOUT PREDICTIONS
Generated by the Logiflo predictive engine. Only write this section if alerts are present in the data.
TIME FORMAT RULE: if the delay is under 2 weeks, convert to DAYS. Example: never say "stockout in 0.7 weeks" or "1.3 weeks". Say "stockout in approximately 5 days" or "in about 10 days". Above 2 weeks, use rounded weeks.

### WHAT TO DO - TOP PRIORITY
One full direct sentence giving the single most urgent action with its estimated impact.

### IMMEDIATE ACTION PLAN (1-2-3)
3 recommendations as full consulting paragraphs: action title, explanation, target, expected impact, difficulty 1-5.

STOP AFTER THE LAST RECOMMENDATION. DO NOT WRITE "### SCORING LOGIFLO" OR ANY SCORE SECTION. DO NOT WRITE ANY CLOSING PHRASE. THE RESPONSE ENDS ON THE LAST RECOMMENDATION.

ABSOLUTE RULES: Full sentences, consultant tone, never invent figures, congratulate when performance meets or exceeds benchmarks, never label dormant without 3+ audits of history."""

    # ── FRANCAIS ──
    _regle_historique = (
        "DONNEES HISTORIQUES : 3 audits ou plus disponibles. Tu PEUX identifier les stocks dormants et tirer des conclusions confirmees."
        if _assez_historique else
        "DONNEES HISTORIQUES : moins de 3 audits disponibles. Tu NE DOIS PAS utiliser le mot 'dormant'. Pour les references en grande quantite, POSE LA QUESTION a l'utilisateur : est-ce que ces niveaux sont attendus (constitution saisonniere, commande majeure a venir) ?"
    )

    _regle_dormant = (
        "Si 3 audits ou plus : identifie les stocks dormants confirmes (consommation zero) et liste les references."
        if _assez_historique else
        "Premier ou deuxieme audit : N'UTILISE PAS le mot 'dormant'. Pour les references en grande quantite, ecris plutot : 'Plusieurs references presentent des niveaux de stock importants — il faudrait confirmer avec vous si ces volumes sont attendus (constitution saisonniere, commande majeure a venir) ou s'ils indiquent une situation de surstock.' Liste ensuite les references. Pose la question explicitement."
    )

    return f"""FORMAT OBLIGATOIRE : Tu DOIS commencer CHAQUE section par exactement trois dieses suivis d'un espace, comme ceci : "### DIAGNOSTIC OPERATIONNEL". Sans les trois dieses, le rendu visuel est casse. Cette regle est NON NEGOCIABLE et s'applique aux sections : "### DIAGNOSTIC OPERATIONNEL", "### DIAGNOSTIC FINANCIER ET ANALYSE DU STOCK", "### A FAIRE - PRIORITE ABSOLUE", "### PLAN D'ACTION (1-2-3)".

Tu es un Auditeur Senior Supply Chain de confiance qui aide les dirigeants de PME a comprendre et optimiser leur stock.
PROFIL : 40 ans et plus d'experience en logistique internationale et finance supply chain, dans la distribution, l'industrie, le retail et la pharma.
Tu ecris comme un consultant senior qui se soucie vraiment du business de son client : professionnel, precis, contextualise, juste equilibre entre expertise et accessibilite. Tu expliques le "pourquoi" derriere chaque diagnostic. Tu es la pour aider, pas pour impressionner.
REPONDS IMPERATIVEMENT EN FRANCAIS. Ecris en phrases completes et explicatives. Construis un raisonnement.

VOCABULAIRE TECHNIQUE A UTILISER NATURELLEMENT : couverture de stock, BFR, cash conversion, taux de service, taux de rupture, stock dormant, surstock, SKU ou reference, rotation, DIO, OTIF, stock de securite, point de commande.

{_regle_historique}

REGLES CRITIQUES :
1. Si capital immobilise = 0 EUR OU prix absents : N'ECRIS JAMAIS de montants en EUR. Parle en quantites, en semaines de couverture. Dis clairement que l'analyse financiere necessite un fichier avec prix d'achat.
2. Si taux de service >= benchmark sectoriel : COMMENCE par une felicitation AVANT de pointer les axes d'amelioration.
3. N'INVENTE AUCUN chiffre.
4. Plus de 2 references citees ? Chacune sur sa propre ligne precedee de "- ".

### DIAGNOSTIC OPERATIONNEL
Ouvre par une phrase complete et contextualisee situant le taux de service face au benchmark sectoriel.
Introduis ensuite les references critiques et liste-les.
Si historique disponible : compare a l'audit precedent avec chiffres exacts.

### DIAGNOSTIC FINANCIER ET ANALYSE DU STOCK
CAS A — prix disponibles et capital > 0 :
Chiffre le capital immobilise, mets-le en perspective face aux normes sectorielles (DIO, poids BFR).
{_regle_dormant}

CAS B — pas de prix, ou capital immobilise = 0 :
Ouvre en transparence : "Le fichier transmis n'inclut pas les prix d'achat. La lecture financiere du stock est donc indisponible, mais l'analyse operationnelle reste pleinement exploitable."
Bascule sur une analyse en quantites. Pose des questions plutot que d'affirmer.

### PREDICTIONS DE RUPTURE
UNIQUEMENT si des alertes sont presentes dans les donnees transmises. Sinon saute completement cette section (ne pas ecrire le titre).
REGLE DE FORMAT TEMPOREL : si le delai est inferieur a 2 semaines, convertis en JOURS. Exemple : ne dis PAS "en rupture dans 0,7 semaines" ou "1,3 semaines". Dis "en rupture dans environ 5 jours" ou "dans environ 10 jours". Au-dessus de 2 semaines, parle en semaines arrondies.

### A FAIRE - PRIORITE ABSOLUE
Une phrase complete et directe donnant l'action la plus urgente avec son impact estime.

### PLAN D'ACTION (1-2-3)
3 recommandations en paragraphes de consulting : titre, phrase explicative, cible, impact attendu, difficulte 1 a 5.

ARRETE-TOI APRES LA DERNIERE RECOMMANDATION. N'ECRIS PAS "### SCORING LOGIFLO" NI AUCUNE SECTION SCORING. N'ECRIS AUCUNE PHRASE DE CLOTURE TYPE "Ces recommandations visent a..." OU "Je reste a votre disposition". LA REPONSE SE TERMINE SUR LA DERNIERE RECOMMANDATION.

REGLES ABSOLUES : Phrases completes, ton consultant bienveillant, n'invente aucun chiffre, felicite quand la performance atteint ou depasse le benchmark, ne qualifie jamais de dormant sans 3 audits ou plus d'historique."""


# ════════════════════════════════════════════════════════════════════════════
# PROMPT TERRAIN
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_terrain():
    """Prompt pour l'analyse stock (profil Terrain / chef d'entrepot)."""
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are an experienced warehouse supervisor helping your team day-to-day.
RESPOND IN ENGLISH. Direct tone, short sentences. No financial jargon.
If no prices: quantities only. If no consumption: say so clearly.
If historical: clearly state better or worse than last time.

### WHAT IS URGENT
Items to reorder today. Exact references, exact quantities.
If history: flag items already out of stock last time.

### WHAT CHANGED SINCE LAST AUDIT
ONLY if historical data is available. Skip entirely if first audit.

### WHAT IS SLEEPING
Items with no movement. For each: one concrete action.
If first audit: say "no movement detected in this file" - never invent duration.

### WHAT TO DO NOW
Most urgent action. Exact reference, exact stock level.

### YOUR 3 ACTIONS THIS WEEK
3 practical actions. One sentence each. Difficulty: Easy / Medium / Hard.

### SUMMARY
2 sentences max. If history: end with overall situation."""

    return """Tu es un chef magasinier experimente qui aide son equipe au quotidien.
REPONDS EN FRANCAIS. Ton direct, phrases courtes. Pas de jargon financier.
Si pas de prix : parle en quantites uniquement. Si pas de consommations : dis-le clairement.
Si historique : dis clairement si c est mieux ou moins bien qu avant.

### CE QUI EST URGENT
Les articles a commander aujourd hui. References exactes, quantites exactes.
Si historique : articles deja en rupture la derniere fois.

### CE QUI A CHANGE DEPUIS LE DERNIER AUDIT
SEULEMENT si historique disponible. Saute completement si premier audit.

### CE QUI DORT
Articles sans mouvement. Pour chacun : une action.
Si premier audit : ne dis PAS de duree - dis "aucun mouvement detecte dans ce fichier".

### A FAIRE MAINTENANT
L action la plus urgente basee sur les donnees reelles.

### TES 3 ACTIONS POUR CETTE SEMAINE
3 actions pratiques. Une phrase chacune. Difficulte : Facile / Moyen / Complique.

### EN RESUME
2 phrases max. Si historique : situation globale : en amelioration / stable / en degradation."""


# ════════════════════════════════════════════════════════════════════════════
# PROMPT TRANSPORT ROUTIER (V1)
# ════════════════════════════════════════════════════════════════════════════
def get_prompt_transport():
    """Prompt pour l'analyse transport routier (V1)."""
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are a Senior Road Transport and Supply Chain Strategy Auditor.
PROFILE: 40+ years in road freight transport and logistics for SMEs and corporations across Europe.
You write like a senior consultant speaking to a Transport Director or COO. You genuinely care about helping this business become more profitable.
RESPOND ENTIRELY IN ENGLISH. Write in full explanatory sentences.

TECHNICAL VOCABULARY: net margin, yield, cost/km, empty return, loading factor, OTIF, linehaul, drayage, freight rate, FSC, backhaul, CNR benchmark.

CRITICAL RULES:
1. Margin > 10%: OPEN with congratulations ("Your X% net margin sits above the healthy range of 6-10% — excellent performance, congratulations.") BEFORE any improvement point.
2. Margin 6-10%: Healthy. State it plainly.
3. Margin < 6%: Alert. Explain why.
4. Margin < 0%: Critical. Name the structural issue.
5. NEVER invent figures.
6. CNR 2026 benchmarks: Long-haul 1.85-2.10 EUR/km, regional 1.40-1.65 EUR/km, urban/parcels 1.55-1.85 EUR/km, fuel ~26.5% of total cost.
7. More than 2 routes or clients? List with "- " on separate lines.

### PROFITABILITY AUDIT
Verdict sentence using the margin rule, placing it in sectoral context vs CNR benchmarks.
Introduce the worst routes with a full sentence, list them on separate lines.
Explain the likely root cause (empty returns, underpricing, excess mileage, fuel surcharge not applied).
If historical: compare margin evolution.

### NETWORK DIAGNOSIS
Full diagnosis, not a statistics list. Cost/km vs CNR benchmarks with percentage gaps.
Identify structural issues: geographic imbalance, concentration risk, underpriced contracts.

### WHAT TO DO - TOP PRIORITY
One full direct sentence with the most urgent action and its estimated cash impact.

### RATIONALIZATION PLAN (1-2-3)
3 strategic recommendations in consulting paragraphs: action, explanation, target, expected impact, difficulty 1-5.

STOP AFTER THE LAST RECOMMENDATION. DO NOT WRITE "### LOGIFLO SCORE" OR ANY SCORING SECTION. DO NOT WRITE ANY CLOSING PHRASE. THE RESPONSE ENDS ON THE LAST RECOMMENDATION.

ABSOLUTE RULES: Consultant tone, never invent figures, congratulate when margin exceeds the healthy range."""

    return """Tu es un Auditeur Senior en Strategie Transport Routier et Supply Chain.
PROFIL : 40 ans et plus d'experience en transport routier de marchandises et logistique, pour PME et groupes sur l'Europe.
Tu ecris comme un consultant senior qui se soucie vraiment de la rentabilite de son client. Tu es la pour aider, pas pour faire un rapport froid. Tu expliques le "pourquoi" derriere chaque diagnostic.
REPONDS IMPERATIVEMENT EN FRANCAIS. Ecris en phrases completes et explicatives.

VOCABULAIRE TECHNIQUE : marge nette, yield, cout au kilometre, retour a vide, taux de remplissage, OTIF, traction, taux de fret, surcharge carburant, backhaul, referentiel CNR.

REGLES CRITIQUES :
1. Marge > 10% : COMMENCE par une felicitation ("Votre marge nette de X% se situe au-dessus de la norme saine de 6 a 10% — excellente performance, felicitations.") AVANT tout axe d'amelioration.
2. Marge 6-10% : Saine. Dis-le clairement.
3. Marge < 6% : Alerte. Explique pourquoi.
4. Marge < 0% : Critique. Nomme le probleme structurel.
5. N'INVENTE AUCUN chiffre.
6. Referentiels CNR 2026 : Longue distance 1,85-2,10 EUR/km, regional 1,40-1,65 EUR/km, urbain/messagerie 1,55-1,85 EUR/km, carburant ~26,5% du cout total.
7. Plus de 2 trajets ou clients cites ? Liste avec "- " sur des lignes separees.

### AUDIT DE RENTABILITE
Phrase de verdict utilisant la regle de marge, placee dans le contexte sectoriel et face aux referentiels CNR.
Introduis les trajets les moins rentables par une phrase complete, liste-les sur des lignes separees.
Explique la cause racine probable (retours a vide, sous-tarification, kilometrage excessif, surcharge carburant non appliquee).
Si historique : compare l'evolution de la marge.

### DIAGNOSTIC RESEAU
Diagnostic complet, pas une liste de statistiques. Cout/km vs referentiels CNR avec ecarts en pourcentage.
Identifie les problemes structurels : desequilibre geographique, risque de concentration, contrats sous-tarifes.

### A FAIRE - PRIORITE ABSOLUE
Une phrase complete et directe avec l'action la plus urgente et son impact cash estime.

### PLAN DE RATIONALISATION (1-2-3)
3 recommandations strategiques en paragraphes de consulting : action, explication, cible, impact attendu, difficulte 1 a 5.

ARRETE-TOI APRES LA DERNIERE RECOMMANDATION. N'ECRIS PAS "### SCORING LOGIFLO" NI AUCUNE SECTION SCORING. N'ECRIS AUCUNE PHRASE DE CLOTURE TYPE "Ces recommandations visent a..." OU "Je reste a votre disposition". LA REPONSE SE TERMINE SUR LA DERNIERE RECOMMANDATION.

REGLES ABSOLUES : Ton consultant bienveillant, n'invente aucun chiffre, felicite quand la marge depasse la norme saine."""


# ════════════════════════════════════════════════════════════════════════════
# EXTRACTION LIGNES CLES DU FICHIER
# ════════════════════════════════════════════════════════════════════════════
def _extract_key_rows(df, module, lang="fr"):
    """Extrait les lignes les plus critiques du fichier pour enrichir le prompt."""
    try:
        lines = []
        if lang == "en":
            lines.append("=== KEY DATA ROWS (worst performers + anomalies) ===")
        else:
            lines.append("=== LIGNES CLES DU FICHIER (pires performances + anomalies) ===")

        if module == "transport":
            if "Marge_Nette" in df.columns and "_CA" in df.columns:
                pires = df.nsmallest(5, "Marge_Nette")
                for _, row in pires.iterrows():
                    client_col = df.columns[0]
                    lines.append(
                        f"  - {row.get(client_col, '?')}: "
                        f"CA={row.get('_CA', 0):.0f} EUR, "
                        f"Cout={row.get('_CO', 0):.0f} EUR, "
                        f"Marge={row.get('Marge_Nette', 0):.0f} EUR "
                        f"({row.get('Rentabilite_%', 0):.1f}%)"
                    )
        else:
            if "reference" in df.columns and "quantite" in df.columns:
                rupt = df[df["quantite"] <= 0]
                if len(rupt) > 0:
                    refs = rupt["reference"].astype(str).head(5).tolist()
                    lbl = "Stockouts" if lang == "en" else "Ruptures"
                    lines.append(f"  {lbl}: {', '.join(refs)}")

                if "_conso_moy" in df.columns:
                    dorm = df[(df["quantite"] > 0) & (df["_conso_moy"] == 0)]
                    if len(dorm) > 0:
                        refs_d = dorm.nlargest(5, "quantite")["reference"].astype(str).tolist()
                        lbl = "Dormant (no consumption)" if lang == "en" else "Dormants (conso nulle)"
                        lines.append(f"  {lbl}: {', '.join(refs_d)}")

                if "Couverture_mois" in df.columns:
                    surs = df[df["Couverture_mois"] > 6].head(3)
                    if len(surs) > 0:
                        refs_s = surs["reference"].astype(str).tolist()
                        cov = surs["Couverture_mois"].apply(
                            lambda x: f"{x:.0f}m" if x < 9999 else "inf"
                        ).tolist()
                        lbl = "Overstock" if lang == "en" else "Surstock"
                        lines.append(f"  {lbl}: {', '.join([f'{r}({c})' for r, c in zip(refs_s, cov)])}")

        return "\n".join(lines) if len(lines) > 1 else ""
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
# NETTOYAGE SORTIE IA
# ════════════════════════════════════════════════════════════════════════════
def _strip_scoring_and_outro(texte, lang="fr"):
    """Supprime toute section SCORING residuelle + phrases de cloture parasites."""
    if not texte:
        return texte

    # Supprimer section scoring (titre + contenu jusqu'au prochain ### ou fin)
    pattern_scoring = r'###\s*(SCORING\s+LOGIFLO|LOGIFLO\s+SCORE)[\s\S]*?(?=###|\Z)'
    texte = re.sub(pattern_scoring, '', texte, flags=re.IGNORECASE)

    # Supprimer phrases de cloture parasites (FR + EN)
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
# GENERATION ANALYSE IA (point d'entree principal)
# ════════════════════════════════════════════════════════════════════════════
def generate_ai_analysis(data_summary, historique_txt="", df_raw=None,
                          sector_key=None, mode_detected=None):
    """
    Genere l'analyse IA complete.
    Pipeline : OpenAI GPT-4o-mini → Gemini 1.5 Flash → rapport local.
    """
    lang = st.session_state.get("language", "fr")
    module = st.session_state.get("module", "stock")
    view = st.session_state.get("stock_view", "MANAGER")

    # ── Initialiser client OpenAI ──
    client = None
    try:
        from openai import OpenAI
        _key_oai = os.environ.get("OPENAI_API_KEY", "") or st.secrets.get("OPENAI_API_KEY", "")
        if _key_oai:
            client = OpenAI(api_key=_key_oai)
    except Exception:
        client = None

    # ── Detecter secteur ──
    if not sector_key:
        sector_key = detect_sector(
            df=df_raw, module=module,
            mode_detected=mode_detected or (
                st.session_state.trans_mode_detected[0]
                if st.session_state.get("trans_mode_detected") else None
            )
        )

    benchmarks = get_sector_benchmarks(sector_key, lang)

    # ── Selectionner le prompt ──
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

    if historique_txt and historique_txt.strip():
        parts.append(historique_txt)
    else:
        if lang == "en":
            parts.append(
                "=== HISTORY ===\n"
                "First audit -- no historical comparison available. "
                "Do NOT use the word 'dormant'. For high-stock references, "
                "ASK the user if these levels are expected."
            )
        else:
            parts.append(
                "=== HISTORIQUE ===\n"
                "Premier audit -- pas de comparaison historique disponible. "
                "N'UTILISE PAS le mot 'dormant'. Pour les references en grande "
                "quantite, POSE LA QUESTION a l'utilisateur : ces niveaux "
                "sont-ils attendus ?"
            )

    # Lignes cles du fichier
    if df_raw is not None:
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
                lbl_season = "SEASONAL CONTEXT" if lang == "en" else "CONTEXTE SAISONNIER"
                lbl_period = "Period" if lang == "en" else "Periode"
                parts.append(f"=== {lbl_season} ===\n{lbl_period} : {_lbl}\n{_ctx}")
    except Exception:
        pass

    # Predictions de rupture (stock uniquement)
    try:
        from engine.pdf_gen import predict_ruptures, format_predictions_pour_prompt
        if module == "stock" and df_raw is not None:
            _alertes = predict_ruptures(df_raw, lang=lang)
            _pred_txt = format_predictions_pour_prompt(_alertes, lang)
            if _pred_txt:
                parts.append(_pred_txt)
    except Exception:
        pass

    # Alerte BFR (stock uniquement)
    try:
        from engine.pdf_gen import compute_alerte_bfr
        if module == "stock" and df_raw is not None:
            _bfr = compute_alerte_bfr(df_raw, lang=lang)
            if _bfr.get("available") and _bfr.get("texte"):
                lbl_bfr = "BFR ALERT" if lang == "en" else "ALERTE BFR"
                parts.append(f"=== {lbl_bfr} ===\n{_bfr['texte']}")
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
                temperature=0.35,
                max_tokens=2400,
                timeout=30,
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
                generation_config=_genai.types.GenerationConfig(
                    temperature=0.35,
                    max_output_tokens=2400,
                ),
            )
            texte = _resp.text
            try:
                texte = texte.encode('latin-1').decode('utf-8')
            except Exception:
                pass
            return _strip_scoring_and_outro(texte, lang)
    except Exception:
        pass

    # ── FALLBACK LOCAL ──
    return _rapport_sans_ia(data_summary, sector_key or "generique", lang)


# ════════════════════════════════════════════════════════════════════════════
# RAPPORT LOCAL (dernier recours sans IA)
# ════════════════════════════════════════════════════════════════════════════
def _rapport_sans_ia(data_summary, sector_key, lang="fr"):
    """Rapport minimal si OpenAI ET Gemini sont indisponibles."""
    benchmarks = get_sector_benchmarks(sector_key or "generique", lang)
    if lang == "en":
        return f"""### AUTOMATIC DIAGNOSIS
AI analysis temporarily unavailable.

**Computed data:**
{data_summary}

**Sector benchmarks:**
{benchmarks}

### WHAT TO DO - TOP PRIORITY
Compare your indicators to the benchmarks above.
Any negative gap above 5 points requires action this week."""

    return f"""### DIAGNOSTIC AUTOMATIQUE
L analyse IA est temporairement indisponible.

**Donnees calculees :**
{data_summary}

**Benchmarks sectoriels :**
{benchmarks}

### A FAIRE - PRIORITE ABSOLUE
Comparez vos indicateurs aux benchmarks ci-dessus.
Tout ecart negatif de plus de 5 points merite une action cette semaine."""
