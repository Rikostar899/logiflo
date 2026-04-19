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
RESPOND ENTIRELY IN ENGLISH.
If prices available: full financial analysis. If NO prices: operational analysis only.
If consumption history: calculate coverage in months. If NO consumption: flag BLIND SPOT.
If historical data present: MANDATORY trend integration.

### OPERATIONAL DIAGNOSIS
Service level vs benchmark. Name 3 critical references with exact figures.
If historical: compare to previous audit.

### FINANCIAL DIAGNOSIS AND DORMANT STOCK
Analyze tied-up capital. CRITICAL: only label dormant if consumption column shows zero.
Never label dormant based solely on absence of prices.

### WHAT TO DO - TOP PRIORITY
Single most urgent action. One direct sentence: reference, action, estimated impact.
One fallback if budget constrained.

### IMMEDIATE ACTION PLAN (1-2-3)
3 strategic recommendations. Action, reference, impact, difficulty 1-5.

### LOGIFLO SCORE
- Stock Performance and Rotation: XX/100
- Stock-out Risk: XX/100
- Supply Chain Resilience: XX/100

RULES: Simple language only. Never invent figures. Only label dormant with zero-consumption data."""
    return """Tu es l Auditeur Financier et Directeur Supply Chain Senior pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS.
Si prix disponibles : analyse financiere complete. Si PAS de prix : analyse operationnelle pure.
Si consommations disponibles : calcule couverture en mois. Si PAS : signale ANGLE MORT.
Si donnees historiques presentes : integre OBLIGATOIREMENT la tendance.

### DIAGNOSTIC OPERATIONNEL
Taux de service vs benchmark. Nomme les 3 references critiques avec chiffres exacts.
Si historique : compare a l audit precedent.

### DIAGNOSTIC FINANCIER ET STOCKS DORMANTS
Si prix : capital immobilise, dormants, cash trap.
Si pas de prix : velocite par reference, articles a rotation nulle.
REGLE CRITIQUE : une reference est dormante UNIQUEMENT si une colonne consommation existe ET montre zero.

### A FAIRE - PRIORITE ABSOLUE
L action la plus urgente. Une phrase directe : reference, action, impact estime.
Une alternative si budget contraint.

### PLAN D ACTION (1-2-3)
3 recommandations strategiques. Action, reference ciblee, impact, difficulte 1 a 5.

### SCORING LOGIFLO
- Performance et Rotation stock : XX/100
- Risque de rupture : XX/100
- Resilience supply chain : XX/100

REGLES : Francais simple uniquement. N invente AUCUN chiffre. Dormant uniquement avec conso a zero."""


def get_prompt_terrain():
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


def get_prompt_transport():
    lang = st.session_state.get("language", "fr")
    if lang == "en":
        return """You are a Senior Transport and Supply Chain Strategy Auditor for Logiflo.io.
RESPOND ENTIRELY IN ENGLISH.
CNR benchmarks 2026: Long-haul road: 1.85-2.10 EUR/km. Regional: 1.40-1.65 EUR/km. Fuel: ~26.5%.

NET MARGIN BENCHMARK - READ BEFORE WRITING:
- Margin > 10% -> EXCELLENT. Say: "your X% margin is excellent."
- Margin 6-10% -> HEALTHY. Say: "your X% margin is within the healthy range."
- Margin < 6%  -> ALERT. Say: "your X% margin is below the 6% minimum."
- Margin < 0%  -> LOSS. Say: "your negative X% margin signals a critical situation."
NEVER say a margin above 10% is concerning.

### PROFITABILITY AUDIT
One verdict sentence using the benchmark rule.
Name 3 worst routes with EXACT figures from the data.
Root cause hypothesis.
If historical: state improving or worsening with exact numbers.

### NETWORK DIAGNOSIS
Cost/km vs CNR benchmarks. Spatial coherence. Load efficiency if weight available.

### WHAT TO DO - TOP PRIORITY
Single most urgent action. Name the client or route, the action, estimated cash recovery.

### RATIONALIZATION PLAN (1-2-3)
3 strategic recommendations. Action, client/route, impact EUR, difficulty 1-5.

### LOGIFLO SCORE
- Profitability and Transport Yield: XX/100
- Operational Efficiency: XX/100
- OPEX Control: XX/100

RULES: Simple language. Never invent figures."""
    return """Tu es un Auditeur Senior en Strategie Transport et Supply Chain pour Logiflo.io.
REPONDS IMPERATIVEMENT EN FRANCAIS.
Referentiels CNR 2026 : Longue distance : 1,85-2,10 EUR/km. Regional : 1,40-1,65 EUR/km. Carburant : ~26,5%.

BENCHMARK MARGE NETTE - LIS AVANT D ECRIRE :
- Marge > 10%  -> EXCELLENTE. Ecris : "votre marge de X% est excellente."
- Marge 6-10%  -> SAINE. Ecris : "votre marge de X% est dans la norme sectorielle."
- Marge < 6%   -> ALERTE. Ecris : "votre marge de X% est en dessous du seuil de 6%."
- Marge < 0%   -> PERTE. Ecris : "votre marge negative de X% signale une situation critique."
NE DIS JAMAIS qu une marge superieure a 10% est preoccupante.

### AUDIT DE RENTABILITE
Une phrase de verdict avec la regle benchmark.
Nomme les 3 pires trajets avec chiffres EXACTS du fichier.
Hypothese sur la cause racine.
Si historique : indique si la marge s ameliore ou se degrade.

### DIAGNOSTIC RESEAU
Cout/km vs referentiels CNR. Coherence spatiale. Taux de remplissage si poids disponible.

### A FAIRE - PRIORITE ABSOLUE
L action la plus urgente. Nomme le client ou trajet, l action, l impact cash estime.

### PLAN DE RATIONALISATION (1-2-3)
3 recommandations strategiques. Action, client/trajet cible, impact EUR, difficulte 1 a 5.

### SCORING LOGIFLO
- Rentabilite et Yield Transport : XX/100
- Efficacite Operationnelle : XX/100
- Maitrise des OPEX : XX/100

REGLES : Francais simple uniquement. N invente AUCUN chiffre."""


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
