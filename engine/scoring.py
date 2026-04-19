import streamlit as st

def compute_logiflo_score(module, df=None, kpis=None, labels=None,
                           sector_key="generique", lang="fr"):
    scores = {}
    details = {}

    if module == "stock":
        try:
            tx_service  = float(kpis[1]) if kpis and len(kpis) > 1 else 0
            nb_ruptures = float(kpis[2]) if kpis and len(kpis) > 2 else 0
            nb_total    = len(df) if df is not None and len(df) > 0 else 1
            taux_rupture = (nb_ruptures / nb_total) * 100 if nb_total > 0 else 0
        except Exception:
            tx_service = 0; taux_rupture = 0

        target_service = {
            "stock_pharma": 97, "stock_industrie": 97, "stock_retail": 96,
            "stock_distribution": 95, "stock_agroalim": 96, "stock_btp": 95,
        }.get(sector_key, 93)

        if tx_service >= target_service:          s1 = 100
        elif tx_service >= target_service - 5:    s1 = 80
        elif tx_service >= target_service - 10:   s1 = 60
        elif tx_service >= 80:                    s1 = 40
        else:                                     s1 = 20
        scores["service"] = s1
        d1 = "Stock Performance & Rotation" if lang == "en" else "Performance et Rotation stock"
        details[d1] = s1

        if taux_rupture <= 1:    s2 = 100
        elif taux_rupture <= 3:  s2 = 80
        elif taux_rupture <= 5:  s2 = 60
        elif taux_rupture <= 10: s2 = 40
        else:                    s2 = 20
        scores["rupture"] = s2
        d2 = "Stock-out Risk" if lang == "en" else "Risque de rupture"
        details[d2] = s2

        s3 = 70
        if df is not None:
            try:
                nb_total2 = max(len(df), 1)
                nb_dorm = len(df[df["Statut"].str.contains("Dormant", na=False)]) if "Statut" in df.columns else 0
                nb_surs = len(df[df["Statut"].str.contains("Surstock", na=False)]) if "Statut" in df.columns else 0
                taux_anom = ((nb_dorm + nb_surs) / nb_total2) * 100
                if taux_anom <= 5:    s3 = 100
                elif taux_anom <= 10: s3 = 75
                elif taux_anom <= 20: s3 = 50
                else:                 s3 = 25
            except Exception:
                s3 = 70
        scores["resilience"] = s3
        d3 = "Supply Chain Resilience" if lang == "en" else "Resilience supply chain"
        details[d3] = s3

        global_score = round(s1 * 0.40 + s2 * 0.35 + s3 * 0.25)

    elif module == "transport":
        try:
            marge_pct = float(kpis[1]) if kpis and len(kpis) > 1 else 0
            nb_tox    = float(kpis[2]) if kpis and len(kpis) > 2 else 0
            nb_total  = len(df) if df is not None and len(df) > 0 else 1
            taux_tox  = (nb_tox / nb_total) * 100 if nb_total > 0 else 0
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

    else:
        scores = {"performance": 70}
        details = {"Performance": 70}
        global_score = 70

    return {
        "global":     global_score,
        "details":    details,
        "scores":     scores,
        "format_pdf": "\n".join([f"- {k} : {v}/100" for k, v in details.items()])
    }
