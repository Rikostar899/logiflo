import streamlit as st
import re
import datetime

from config.sectoral_db import detect_sector, get_sector_benchmarks
from config.translations import _
from services.supabase_client import get_historique_audits


def format_historique_pour_prompt(hist, module, lang="fr"):
    if not hist:
        return ""
    h = hist["history"]
    n = hist["n_audits"]
    if lang == "en":
        lines = [f"\n=== HISTORICAL TREND -- last {n} audits ==="]
        lines.append(f"Period: {hist['first_date']} -> {hist['last_date']}\n")
        for i, entry in enumerate(h):
            tag = "CURRENT" if i == len(h)-1 else f"Audit {i+1}"
            lines.append(f"[{tag} -- {entry['date']}]")
            lines.append(f"  {entry['label_1'][:20]}: {entry['kpi_1']:.1f} | {entry['label_2'][:20]}: {entry['kpi_2']:.1f} | {entry['label_3'][:20]}: {entry['kpi_3']:.1f}")
        lines.append("\nCOMPUTED TRENDS:")
        d1, d2, d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]
        if module == "transport":
            if d1 is not None: lines.append(f"  Net margin: {'improving' if d1>0 else 'declining'} ({d1:+.1f}%)")
            if d2 is not None: lines.append(f"  Profitability: {'improving' if d2>0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Toxic routes: {'improving' if d3<0 else 'worsening'} ({d3:+.1f}%)")
        else:
            if d1 is not None: lines.append(f"  Capital/Items: {d1:+.1f}%")
            if d2 is not None: lines.append(f"  Service level: {'improving' if d2>0 else 'declining'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Stock-outs: {'worsening' if d3>0 else 'improving'} ({d3:+.1f}%)")
        lines.append("=== END HISTORICAL DATA ===\n")
    else:
        lines = [f"\n=== TENDANCE HISTORIQUE -- {n} derniers audits ==="]
        lines.append(f"Periode : {hist['first_date']} -> {hist['last_date']}\n")
        for i, entry in enumerate(h):
            tag = "ACTUEL" if i == len(h)-1 else f"Audit {i+1}"
            lines.append(f"[{tag} -- {entry['date']}]")
            lines.append(f"  {entry['label_1'][:25]}: {entry['kpi_1']:.1f} | {entry['label_2'][:25]}: {entry['kpi_2']:.1f} | {entry['label_3'][:25]}: {entry['kpi_3']:.1f}")
        lines.append("\nTENDANCES CALCULEES :")
        d1, d2, d3 = hist["delta_1"], hist["delta_2"], hist["delta_3"]
        if module == "transport":
            if d1 is not None: lines.append(f"  Marge nette : {'en hausse' if d1>0 else 'en baisse'} ({d1:+.1f}%)")
            if d2 is not None: lines.append(f"  Taux rentabilite : {'en hausse' if d2>0 else 'en baisse'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Trajets toxiques : {'en hausse' if d3>0 else 'en baisse'} ({d3:+.1f}%)")
        else:
            if d1 is not None: lines.append(f"  Capital/Articles : {d1:+.1f}%")
            if d2 is not None: lines.append(f"  Taux de service : {'en amelioration' if d2>0 else 'en degradation'} ({d2:+.1f}%)")
            if d3 is not None: lines.append(f"  Ruptures : {'en hausse' if d3>0 else 'en baisse'} ({d3:+.1f}%)")
        lines.append("=== FIN DONNEES HISTORIQUES ===\n")
    return "\n".join(lines)


def get_prompt_stock():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are a Senior Financial Auditor and Supply Chain Director for Logiflo.io.
RESPOND ENTIRELY IN ENGLISH. Write in full, explanatory sentences — not keywords.
Tone: professional but accessible, like a senior colleague briefing a manager.

RULES FOR LISTING REFERENCES:
When you cite products, SKUs, routes or any list of items, ALWAYS format them as a bullet list.
Each item on its own line, starting with "- " (dash space).
Never cite more than 2 references inline in a sentence — use a list instead.

If prices available: full financial analysis. If NO prices: operational analysis only.
If consumption history: calculate coverage. If NO consumption: flag the blind spot explicitly.
If historical data present: MANDATORY trend integration with exact numbers.

### OPERATIONAL DIAGNOSIS
Start with a complete sentence that describes the overall stock health and places it against the benchmark.
Example: "Your current situation shows a service level of 83.3%, which is below the sectoral benchmark of 90-95%. This 6.7-point gap indicates insufficient capacity to meet client demand."
Then introduce the critical references with a full sentence, and list them on separate lines:
"The three most critical references are currently out of stock:
- Midi Pleated Skirt
- Summer Linen Shirt
- Leather Card Holder
These stockouts can lead to significant lost sales and hurt customer satisfaction."
If historical data: explicitly say whether the situation improves or declines vs previous audit.

### FINANCIAL DIAGNOSIS AND DORMANT STOCK
Open with a complete sentence that quantifies and contextualises the tied-up capital.
Example: "Tied-up capital amounts to 29,765 EUR, which is relatively high against sectoral norms."
Explain what this means concretely for the business.
If dormant references exist (only if consumption data shows zero), list them on separate lines:
"The references flagged as dormant are:
- White Logo T-shirt
- Black Logo T-shirt
- Wool Socks"
If no consumption data: say explicitly that dormant status cannot be confirmed without consumption history.
CRITICAL RULE: never label a reference as dormant if there is no consumption column showing zero.

### STOCKOUT PREDICTIONS
Generated by the Logiflo predictive engine. Only write this section if alerts are present in the data.
Write one full sentence per alert in this format:
"- [reference] will reach stockout in approximately [X] weeks at current consumption."
Rank by urgency: critical (under 1 week), urgent (under 2 weeks), alert (under 4 weeks).
If no predictions available: skip this section entirely, do not write the title.

### WHAT TO DO - TOP PRIORITY
Write a complete, direct sentence that gives the single most urgent action.
Example: "Urgent action: immediately restock the Midi Pleated Skirt to prevent further lost sales. Estimated impact: 5,000 EUR of potential revenue lost if the stockout persists."
Then give one fallback if the budget is constrained.

### IMMEDIATE ACTION PLAN (1-2-3)
3 concrete recommendations, each written as a full paragraph with action, target, expected impact, and difficulty 1-5.
Use this format:
"1. [Action title]: [full sentence explaining what to do and why, with the targeted references or categories]. Expected impact: [quantified or qualified]. Difficulty: [1 to 5]."
If you cite multiple references inside a recommendation, put them on separate lines under the paragraph.

### LOGIFLO SCORE
- Stock Performance and Rotation: XX/100
- Stock-out Risk: XX/100
- Supply Chain Resilience: XX/100

ABSOLUTE RULES: Full sentences, engaging tone. Never invent figures. Dormant only with zero-consumption data. Always use "- " bullet lists when citing multiple references."""
    return """Tu es l Auditeur Financier et Directeur Supply Chain Senior pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS. Ecris en phrases completes et explicatives, pas en mots-cles.
Ton : professionnel mais accessible, comme un collegue senior qui briefe un responsable.

REGLES POUR CITER DES REFERENCES :
Quand tu cites des produits, des SKUs, des trajets ou n importe quelle liste d elements, formate-les TOUJOURS en liste a puces.
Chaque element sur sa propre ligne, precede de "- " (tiret espace).
Ne cite JAMAIS plus de 2 references a la suite dans une meme phrase — utilise une liste a la place.

Si prix disponibles : analyse financiere complete. Si PAS de prix : analyse operationnelle pure.
Si consommations disponibles : calcule la couverture. Si PAS : signale explicitement l angle mort.
Si donnees historiques presentes : integre OBLIGATOIREMENT la tendance avec chiffres exacts.

### DIAGNOSTIC OPERATIONNEL
Commence par une phrase complete qui decrit l etat global du stock et le situe par rapport au benchmark.
Exemple : "Votre situation actuelle presente un taux de service de 83,3%, ce qui est en dessous du benchmark sectoriel qui se situe entre 90% et 95%. Cet ecart de 6,7 points indique une insuffisance dans la capacite a repondre aux demandes clients."
Introduis ensuite les references critiques avec une phrase complete, puis liste-les sur des lignes separees :
"Les trois references les plus critiques sont actuellement en rupture de stock :
- Jupe Plissee Midi
- Chemise Lin Ete
- Porte-carte Cuir
Ces ruptures peuvent entrainer des pertes de ventes significatives et nuire a la satisfaction client."
Si historique disponible : dis explicitement si la situation s ameliore ou se degrade par rapport a l audit precedent.

### DIAGNOSTIC FINANCIER ET STOCKS DORMANTS
Ouvre par une phrase complete qui chiffre et contextualise le capital immobilise.
Exemple : "Le capital immobilise s eleve a 29 765 EUR, ce qui est relativement eleve par rapport aux normes sectorielles."
Explique ce que cela signifie concretement pour l entreprise.
Si des references dormantes existent (uniquement si la colonne consommation montre zero), liste-les sur des lignes separees :
"Les references identifiees comme dormantes sont :
- T-shirt Logo Blanc
- T-shirt Logo Noir
- Chaussettes Laine"
Si pas de donnees de consommation : dis explicitement que le statut dormant ne peut pas etre confirme sans historique de consommation.
REGLE CRITIQUE : ne jamais qualifier une reference de dormante s il n y a pas de colonne consommation montrant zero.

### PREDICTIONS DE RUPTURE
Genere par le moteur predictif Logiflo. N ecris cette section QUE si des alertes sont presentes dans les donnees.
Ecris une phrase complete par alerte avec ce format :
"- L article [reference] sera en rupture dans environ [X] semaines au rythme de consommation actuel."
Classe par urgence : critique (moins de 1 semaine), urgent (moins de 2 semaines), alerte (moins de 4 semaines).
Si aucune prediction disponible : ne pas ecrire cette section, ne pas ecrire le titre.

### A FAIRE - PRIORITE ABSOLUE
Ecris une phrase complete et directe qui donne l action la plus urgente.
Exemple : "Action urgente : reapprovisionner immediatement la Jupe Plissee Midi pour eviter des pertes de ventes. Impact estime : 5 000 EUR de chiffre d affaires potentiel perdu si la rupture persiste."
Donne ensuite une alternative si le budget est contraint.

### PLAN D ACTION (1-2-3)
3 recommandations concretes, chacune redigee en paragraphe complet avec action, cible, impact attendu et difficulte 1 a 5.
Utilise ce format :
"1. [Titre de l action] : [phrase complete expliquant quoi faire et pourquoi, avec les references ou categories ciblees]. Impact attendu : [chiffre ou qualitatif]. Difficulte : [1 a 5]."
Si tu cites plusieurs references dans une recommandation, mets-les sur des lignes separees sous le paragraphe.

### SCORING LOGIFLO
- Performance et Rotation stock : XX/100
- Risque de rupture : XX/100
- Resilience supply chain : XX/100

REGLES ABSOLUES : Phrases completes, ton engageant. N invente AUCUN chiffre. Dormant uniquement avec conso a zero. Utilise TOUJOURS des listes "- " quand tu cites plusieurs references."""


def get_prompt_terrain():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are an experienced warehouse supervisor helping your team day-to-day.
RESPOND IN ENGLISH. Direct, complete sentences. No financial jargon.
Speak like a trusted colleague at the warehouse, not a robot.

RULES FOR LISTING ITEMS:
When you cite items, SKUs, routes or any list, ALWAYS format them as a bullet list.
Each item on its own line, starting with "- " (dash space).
Never cite more than 2 items inline — use a list instead.

If no prices: quantities only. If no consumption: say so clearly.
If historical: clearly state better or worse than last time, with numbers.

### WHAT IS URGENT
Start with a complete sentence that states how many items are out of stock today and which is the most critical.
Example: "14 items are out of stock today. The most critical is the Performance Bra, with 0 units left against a weekly consumption of 30."
Then list the items to reorder on separate lines:
"Items to reorder today:
- Performance Bra (stock: 0)
- Midi Pleated Skirt (stock: 0)
- Leather Card Holder (stock: 0)"
If history: flag items that were already out of stock last time — it is a serious signal.

### WHAT CHANGED SINCE LAST AUDIT
ONLY if historical data is available. Skip entirely if first audit.
Write two lists: what improved and what got worse, each item on its own line starting with "- ".

### WHAT IS SLEEPING
Items with no movement detected in this file.
Open with a complete sentence, then list them:
"- [reference] (stock: [X], no movement detected)"
If first audit: say "no movement detected in this file" — never invent a duration.

### WHAT TO DO NOW
Write one complete sentence naming the most urgent action, the exact reference, the current stock, and why it cannot wait.
Do not invent order quantities unless consumption history is available.

### YOUR 3 ACTIONS THIS WEEK
3 practical actions in complete sentences, ranked by urgency.
Format: "1. [Full sentence describing the action]. Difficulty: Easy / Medium / Hard."

### SUMMARY
2 sentences max to brief the manager in 30 seconds.
If historical data: end with "Overall situation: improving / stable / worsening."""
    return """Tu es un chef magasinier experimente qui aide son equipe au quotidien.
REPONDS EN FRANCAIS. Phrases completes et directes. Pas de jargon financier.
Parle comme un collegue de confiance au depot, pas comme un robot.

REGLES POUR CITER DES ARTICLES :
Quand tu cites des articles, des SKUs, des trajets ou n importe quelle liste, formate-les TOUJOURS en liste a puces.
Chaque element sur sa propre ligne, precede de "- " (tiret espace).
Ne cite JAMAIS plus de 2 articles a la suite dans une phrase — utilise une liste a la place.

Si pas de prix : parle en quantites uniquement. Si pas de consommations : dis-le clairement.
Si historique : dis clairement si c est mieux ou moins bien qu avant, avec des chiffres.

### CE QUI EST URGENT
Commence par une phrase complete qui dit combien d articles sont en rupture aujourd hui et lequel est le plus critique.
Exemple : "14 articles sont en rupture aujourd hui. Le plus critique est la Brassiere Performance, avec 0 unite restante pour une consommation hebdomadaire de 30 unites."
Liste ensuite les articles a commander sur des lignes separees :
"Articles a commander aujourd hui :
- Brassiere Performance (stock : 0)
- Jupe Plissee Midi (stock : 0)
- Porte-carte Cuir (stock : 0)"
Si historique : signale les articles deja en rupture la derniere fois — c est un signal serieux.

### CE QUI A CHANGE DEPUIS LE DERNIER AUDIT
SEULEMENT si historique disponible. Saute completement si premier audit.
Ecris deux listes : ce qui s est ameliore et ce qui s est degrade, chaque element sur sa propre ligne precede de "- ".

### CE QUI DORT
Articles sans mouvement detectes dans ce fichier.
Ouvre par une phrase complete, puis liste-les :
"- [reference] (stock : [X], aucun mouvement detecte)"
Si premier audit : dis "aucun mouvement detecte dans ce fichier" — ne jamais inventer de duree.

### A FAIRE MAINTENANT
Ecris une phrase complete nommant l action la plus urgente, la reference exacte, le stock actuel, et pourquoi ca ne peut pas attendre.
Ne pas inventer de quantites a commander sauf si un historique de consommation est disponible.

### TES 3 ACTIONS POUR CETTE SEMAINE
3 actions pratiques en phrases completes, classees par urgence.
Format : "1. [Phrase complete decrivant l action]. Difficulte : Facile / Moyen / Complique."

### EN RESUME
2 phrases max pour briefer le responsable en 30 secondes.
Si historique : termine par "Situation globale : en amelioration / stable / en degradation."""


def get_prompt_transport():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are a Senior Transport and Supply Chain Strategy Auditor for Logiflo.io.
RESPOND ENTIRELY IN ENGLISH. Write in full, explanatory sentences.
Tone: professional but accessible, like a senior colleague briefing a transport director.

RULES FOR LISTING ROUTES OR CLIENTS:
When you cite routes, clients, trips or any list of items, ALWAYS format them as a bullet list.
Each item on its own line, starting with "- " (dash space).
Never cite more than 2 routes inline — use a list instead.

CNR benchmarks 2026: Long-haul road: 1.85-2.10 EUR/km. Regional: 1.40-1.65 EUR/km. Fuel: ~26.5%.

NET MARGIN BENCHMARK - READ BEFORE WRITING:
- Margin > 10% -> EXCELLENT. Say: "your X% margin is excellent."
- Margin 6-10% -> HEALTHY. Say: "your X% margin is within the healthy range."
- Margin < 6%  -> ALERT. Say: "your X% margin is below the 6% minimum."
- Margin < 0%  -> LOSS. Say: "your negative X% margin signals a critical situation."
NEVER say a margin above 10% is concerning.

### PROFITABILITY AUDIT
Start with a full verdict sentence using the benchmark rule above and contextualising the result.
Example: "Your network shows a net margin of 4.2%, which is below the healthy range of 6-10%. This gap indicates that some routes are dragging down overall profitability."
Then introduce the worst-performing routes with a complete sentence and list them separately:
"The three most unprofitable routes in your portfolio are:
- Paris-Bordeaux: -168 EUR per trip
- Bordeaux-Paris (return leg): -94 EUR per trip
- Lyon-Toulouse: -55 EUR per trip
These routes generate a cumulative monthly leak that can be addressed."
Explain the root cause (empty returns, underpricing, excess mileage).
If historical data: state whether margin is improving or worsening with exact figures.

### NETWORK DIAGNOSIS
Write a complete diagnosis, not a list of statistics.
Compare cost/km to CNR benchmarks with percentage gaps. Analyse spatial coherence. If weight data is available, assess load efficiency.

### WHAT TO DO - TOP PRIORITY
Write one complete, direct sentence giving the single most urgent action.
Example: "Priority action: renegotiate or drop the Paris-Bordeaux return leg. Recovering that margin alone saves approximately 1,680 EUR per month."
Then give one fallback if the commercial relationship is too important to touch.

### RATIONALIZATION PLAN (1-2-3)
3 strategic recommendations in complete paragraph form.
Format: "1. [Action title]: [full sentence explaining the action and the targeted client or route]. Expected impact: [EUR]. Difficulty: [1 to 5]."
If multiple routes or clients are cited, put them on separate lines under the paragraph.

### LOGIFLO SCORE
- Profitability and Transport Yield: XX/100
- Operational Efficiency: XX/100
- OPEX Control: XX/100

ABSOLUTE RULES: Full sentences, engaging tone. Never invent figures. Always use "- " bullet lists when citing multiple routes or clients."""
    return """Tu es un Auditeur Senior en Strategie Transport et Supply Chain pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS. Ecris en phrases completes et explicatives.
Ton : professionnel mais accessible, comme un collegue senior qui briefe un directeur transport.

REGLES POUR CITER DES TRAJETS OU CLIENTS :
Quand tu cites des trajets, des clients, des voyages ou n importe quelle liste d elements, formate-les TOUJOURS en liste a puces.
Chaque element sur sa propre ligne, precede de "- " (tiret espace).
Ne cite JAMAIS plus de 2 trajets a la suite dans une phrase — utilise une liste a la place.

Referentiels CNR 2026 : Longue distance : 1,85-2,10 EUR/km. Regional : 1,40-1,65 EUR/km. Carburant : ~26,5%.

BENCHMARK MARGE NETTE - LIS AVANT D ECRIRE :
- Marge > 10%  -> EXCELLENTE. Ecris : "votre marge de X% est excellente."
- Marge 6-10%  -> SAINE. Ecris : "votre marge de X% est dans la norme sectorielle."
- Marge < 6%   -> ALERTE. Ecris : "votre marge de X% est en dessous du seuil de 6%."
- Marge < 0%   -> PERTE. Ecris : "votre marge negative de X% signale une situation critique."
NE DIS JAMAIS qu une marge superieure a 10% est preoccupante.

### AUDIT DE RENTABILITE
Commence par une phrase de verdict complete en utilisant la regle benchmark ci-dessus et en contextualisant le resultat.
Exemple : "Votre reseau affiche une marge nette de 4,2%, ce qui est en dessous de la norme saine de 6 a 10%. Cet ecart indique que certains trajets tirent la rentabilite globale vers le bas."
Introduis ensuite les trajets les moins rentables avec une phrase complete et liste-les separement :
"Les trois trajets les plus deficitaires de votre portefeuille sont :
- Paris-Bordeaux : -168 EUR par voyage
- Bordeaux-Paris (retour) : -94 EUR par voyage
- Lyon-Toulouse : -55 EUR par voyage
Ces trajets generent une fuite mensuelle cumulee qui peut etre traitee."
Explique la cause racine (retours a vide, sous-tarification, kilometrage excessif).
Si historique disponible : indique si la marge s ameliore ou se degrade avec des chiffres exacts.

### DIAGNOSTIC RESEAU
Ecris un diagnostic complet, pas une liste de statistiques.
Compare le cout/km aux referentiels CNR avec les ecarts en pourcentage. Analyse la coherence spatiale. Si le poids est disponible, evalue le taux de remplissage.

### A FAIRE - PRIORITE ABSOLUE
Ecris une phrase complete et directe donnant l action la plus urgente.
Exemple : "Action prioritaire : renegocier ou abandonner le retour Paris-Bordeaux. Recuperer cette marge seule economise environ 1 680 EUR par mois."
Donne ensuite une alternative si la relation commerciale est trop importante a toucher.

### PLAN DE RATIONALISATION (1-2-3)
3 recommandations strategiques redigees en paragraphes complets.
Format : "1. [Titre de l action] : [phrase complete expliquant l action et le client ou trajet cible]. Impact attendu : [EUR]. Difficulte : [1 a 5]."
Si plusieurs trajets ou clients sont cites, mets-les sur des lignes separees sous le paragraphe.

### SCORING LOGIFLO
- Rentabilite et Yield Transport : XX/100
- Efficacite Operationnelle : XX/100
- Maitrise des OPEX : XX/100

REGLES ABSOLUES : Phrases completes, ton engageant. N invente AUCUN chiffre. Utilise TOUJOURS des listes "- " quand tu cites plusieurs trajets ou clients."""


def _extract_key_rows(df, module, lang="fr"):
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
                    lines.append(f"  - {row.get(client_col,'?')}: CA={row.get('_CA',0):.0f} EUR, Cout={row.get('_CO',0):.0f} EUR, Marge={row.get('Marge_Nette',0):.0f} EUR ({row.get('Rentabilite_%',0):.1f}%)")
        else:
            if "reference" in df.columns and "quantite" in df.columns:
                rupt = df[df["quantite"] <= 0]
                if len(rupt) > 0:
                    refs = rupt["reference"].astype(str).head(5).tolist()
                    lines.append(f"  {'Stockouts' if lang=='en' else 'Ruptures'}: {', '.join(refs)}")
                if "_conso_moy" in df.columns:
                    dorm = df[(df["quantite"] > 0) & (df["_conso_moy"] == 0)]
                    if len(dorm) > 0:
                        refs_d = dorm.nlargest(5, "quantite")["reference"].astype(str).tolist()
                        lines.append(f"  {'Dormant (no consumption)' if lang=='en' else 'Dormants (conso nulle)'}: {', '.join(refs_d)}")
                if "Couverture_mois" in df.columns:
                    surs = df[df["Couverture_mois"] > 6].head(3)
                    if len(surs) > 0:
                        refs_s = surs["reference"].astype(str).tolist()
                        cov = surs["Couverture_mois"].apply(lambda x: f"{x:.0f}m" if x < 9999 else "inf").tolist()
                        lines.append(f"  {'Overstock' if lang=='en' else 'Surstock'}: {', '.join([f'{r}({c})' for r,c in zip(refs_s,cov)])}")
        return "\n".join(lines) if len(lines) > 1 else ""
    except Exception:
        return ""


def generate_ai_analysis(data_summary, historique_txt="", df_raw=None,
                          sector_key=None, mode_detected=None):
    from openai import OpenAI
    lang   = st.session_state.get("language", "fr")
    module = st.session_state.get("module", "stock")
    view   = st.session_state.get("stock_view", "MANAGER")

    try:
        client = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY", ""))
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

    if module == "transport":    sys_prompt = get_prompt_transport()
    elif view == "TERRAIN":      sys_prompt = get_prompt_terrain()
    else:                        sys_prompt = get_prompt_stock()

    parts = []
    parts.append(f"=== {'CURRENT AUDIT DATA' if lang=='en' else 'DONNEES AUDIT ACTUEL'} ===\n{data_summary}")
    parts.append(f"=== {'SECTOR BENCHMARKS' if lang=='en' else 'BENCHMARKS SECTORIELS'} ===\n{benchmarks}")

    if historique_txt and historique_txt.strip():
        parts.append(historique_txt)
    else:
        if lang == "en":
            parts.append("=== HISTORY ===\nFirst audit -- no historical comparison available.")
        else:
            parts.append("=== HISTORIQUE ===\nPremier audit -- pas de comparaison historique disponible.")

    if df_raw is not None:
        try:
            key_data = _extract_key_rows(df_raw, module, lang)
            if key_data:
                parts.append(key_data)
        except Exception:
            pass

    try:
        from engine.ingester import detect_periode
        _periode = detect_periode(df_raw) if df_raw is not None else None
        if _periode and _periode.get("contexte_fr" if lang=="fr" else "contexte_en"):
            _ctx = _periode.get("contexte_fr") if lang=="fr" else _periode.get("contexte_en")
            _lbl = _periode.get("label", "")
            parts.append(f"=== {'SEASONAL CONTEXT' if lang=='en' else 'CONTEXTE SAISONNIER'} ===\n{'Period' if lang=='en' else 'Periode'} : {_lbl}\n{_ctx}")
    except Exception:
        pass

    try:
        from engine.pdf_gen import predict_ruptures, format_predictions_pour_prompt
        if module == "stock" and df_raw is not None:
            _alertes = predict_ruptures(df_raw, lang=lang)
            _pred_txt = format_predictions_pour_prompt(_alertes, lang)
            if _pred_txt:
                parts.append(_pred_txt)
    except Exception:
        pass

    try:
        from engine.pdf_gen import compute_alerte_bfr
        if module == "stock" and df_raw is not None:
            _bfr = compute_alerte_bfr(df_raw, lang=lang)
            if _bfr.get("available") and _bfr.get("texte"):
                parts.append(f"=== {'BFR ALERT' if lang=='en' else 'ALERTE BFR'} ===\n{_bfr['texte']}")
    except Exception:
        pass

    try:
        from engine.scoring import compute_logiflo_score
        _kpis_ctx = st.session_state.get("last_kpis", [])
        _labels_ctx = st.session_state.get("last_labels", [])
        _score_ctx = compute_logiflo_score(module=module, df=df_raw, kpis=_kpis_ctx,
                                            labels=_labels_ctx, sector_key=sector_key or "generique", lang=lang)
        if _score_ctx.get("global", 0) > 0:
            parts.append(f"=== {'PRE-COMPUTED LOGIFLO SCORE' if lang=='en' else 'SCORING LOGIFLO PRE-CALCULE'} ===\n"
                         f"{'Global score' if lang=='en' else 'Score global'} : {_score_ctx['global']}/100\n"
                         f"{_score_ctx.get('format_pdf', '')}")
    except Exception:
        pass

    user_msg = "\n\n".join(parts)

    if client:
        try:
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user",   "content": user_msg}
                ],
                temperature=0.35, max_tokens=2400, timeout=30
            )
            texte = r.choices[0].message.content
            try:    return texte.encode('latin-1').decode('utf-8')
            except: return texte
        except Exception:
            pass

    try:
        import google.generativeai as _genai
        _genai.configure(api_key=st.secrets.get("GEMINI_API_KEY", ""))
        _gem = _genai.GenerativeModel("gemini-1.5-flash")
        _resp = _gem.generate_content(
            f"{sys_prompt}\n\n{user_msg}",
            generation_config=_genai.types.GenerationConfig(temperature=0.35, max_output_tokens=2400)
        )
        texte = _resp.text
        try:    return texte.encode('latin-1').decode('utf-8')
        except: return texte
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
