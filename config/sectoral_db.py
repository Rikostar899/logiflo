SECTORAL_DB = {
    "transport_routier":{"keywords":["routier","camion","transport","trajet","fret","livraison","tournee","chauffeur","vehicule","traction","semiremorque","porteur","messagerie"],"fr":"""BENCHMARKS TRANSPORT ROUTIER CNR 2026:\n- Cout/km longue distance articule gazole: 1,85-2,10 EUR/km\n- Cout/km regional porteur: 1,40-1,65 EUR/km\n- Part carburant: ~26,5% du cout total\n- Marge nette saine PME transport: 6-10% | Alerte < 6% | Perte < 0%\n- Taux de remplissage sain: > 80%""","en":"""CNR 2026 ROAD TRANSPORT:\n- Long-haul cost/km: 1.85-2.10 EUR/km\n- Regional cost/km: 1.40-1.65 EUR/km\n- Fuel share: ~26.5%\n- Healthy net margin: 6-10% | Alert < 6% | Loss < 0%\n- Load factor: > 80%"""},
    "transport_maritime":{"keywords":["maritime","port","navire","conteneur","teu","fcl","lcl","shipping","fos","havre","marseille"],"fr":"""BENCHMARKS TRANSPORT MARITIME 2026:\n- Transit Europe-Asie via Suez: 28-35 jours\n- Demurrage moyen: 120-180 EUR/jour/conteneur\n- Marge transitaire maritime: 15-25%\n- Taux ponctualite armateurs: 55-70%""","en":"""MARITIME BENCHMARKS 2026:\n- Europe-Asia transit: 28-35 days\n- Avg demurrage: 120-180 EUR/day/container\n- Forwarder margin: 15-25%\n- Carrier on-time: 55-70%"""},
    "transport_aerien":{"keywords":["aerien","air","cargo","fret aerien","awb","airline","avion","roissy","cdg"],"fr":"""BENCHMARKS FRET AERIEN 2026:\n- Taux Europe-Asie: 2,20-3,80 EUR/kg\n- Europe-Amerique: 2,80-4,50 EUR/kg\n- Surcharge FSC: 25-40%\n- Marge transitaire: 20-35%""","en":"""AIR FREIGHT BENCHMARKS 2026:\n- Europe-Asia: 2.20-3.80 EUR/kg\n- Europe-Americas: 2.80-4.50 EUR/kg\n- FSC: 25-40%\n- Forwarder margin: 20-35%"""},
    "stock_industrie":{"keywords":["industrie","manufacturing","usine","production","piece","composant","matiere","cable","machine","outil"],"fr":"""BENCHMARKS STOCK INDUSTRIEL 2026:\n- Taux de service cible: > 97%\n- Couverture stock saine: 1-3 mois\n- Stock dormant: alerte si > 5% du capital\n- Cout possession: 18-25% valeur/an\n- BFR cible: < 45 jours de CA""","en":"""INDUSTRIAL STOCK BENCHMARKS 2026:\n- Target service level: > 97%\n- Healthy coverage: 1-3 months\n- Dormant stock: alert if > 5% of capital\n- Holding cost: 18-25% value/year\n- Target WCR: < 45 days revenue"""},
    "stock_distribution":{"keywords":["distribution","grossiste","negoce","commerce","import","export","vente"],"fr":"""BENCHMARKS STOCK DISTRIBUTION 2026:\n- Taux de service cible: > 95%\n- Couverture stock saine: 1-2 mois\n- Rotation annuelle saine: > 6 fois\n- Stock dormant: alerte si > 8% du capital\n- Cout possession: 20-28% valeur/an""","en":"""DISTRIBUTION STOCK BENCHMARKS 2026:\n- Target service level: > 95%\n- Healthy coverage: 1-2 months\n- Annual turns: > 6x\n- Dormant stock: alert if > 8% of capital\n- Holding cost: 20-28% value/year"""},
    "stock_pharma":{"keywords":["pharma","medicament","lot","dluo","dlc","peremption","sante","molecule","laboratoire"],"fr":"""BENCHMARKS STOCK PHARMACEUTIQUE 2026:\n- Taux de service: > 99% (critique)\n- Gestion lots FEFO obligatoire\n- Alertes peremption: > 6 mois avant DLC\n- Stock dormant: < 2% des references""","en":"""PHARMACEUTICAL STOCK BENCHMARKS 2026:\n- Service level: > 99% (critical)\n- FEFO mandatory\n- Expiry alerts: > 6 months before expiry\n- Dormant stock: < 2% of references"""},
    "stock_retail":{"keywords":["retail","magasin","boutique","fashion","mode","soldes","enseigne","rayon"],"fr":"""BENCHMARKS STOCK RETAIL 2026:\n- Taux de service: > 96%\n- Rotation fashion: > 4 fois/an\n- Stock dormant: alerte si > 6 mois sans mouvement\n- BFR cible: < 60 jours de CA""","en":"""RETAIL STOCK BENCHMARKS 2026:\n- Service level: > 96%\n- Fashion turns: > 4x/year\n- Dormant: alert if > 6 months no movement\n- Target WCR: < 60 days revenue"""},
    "stock_agroalim":{"keywords":["alimentaire","agroalimentaire","dlc","dluo","frais","surgele","epicerie","boisson","conserve"],"fr":"""BENCHMARKS STOCK AGROALIMENTAIRE 2026:\n- Taux de service: > 96%\n- FEFO obligatoire\n- Couverture stock frais: < 5 jours\n- Couverture stock sec: < 30 jours\n- Perte / casse cible: < 1,5% du CA""","en":"""FOOD & BEVERAGE BENCHMARKS 2026:\n- Service level: > 96%\n- FEFO mandatory\n- Fresh coverage: < 5 days\n- Dry coverage: < 30 days\n- Waste target: < 1.5% revenue"""},
    "stock_btp":{"keywords":["btp","chantier","construction","materiaux","beton","acier","menuiserie","electricite","plomberie"],"fr":"""BENCHMARKS STOCK BTP 2026:\n- Taux de service cible: > 95%\n- Couverture stock: 2-4 semaines chantier\n- Perte et casse: < 3%\n- Stock dormant: alerte si > 4 mois""","en":"""CONSTRUCTION STOCK BENCHMARKS 2026:\n- Target service level: > 95%\n- Coverage: 2-4 weeks per site\n- Waste: < 3%\n- Dormant: alert if > 4 months"""},
    "transport_maritime_intl":{"keywords":["container","conteneur","teu","fcl","lcl","bl","vessel","shipping","freight","ocean"],"fr":"""BENCHMARKS MARITIME INTERNATIONAL 2026:\n- 20' Europe-Asie: 800-2500 USD/TEU\n- Mediterranee-Afrique Ouest: 1200-2800 USD/TEU\n- Demurrage: 120-180 EUR/jour\n- Ponctualite: 55-70%""","en":"""INTERNATIONAL MARITIME BENCHMARKS 2026:\n- 20' Europe-Asia: 800-2500 USD/TEU\n- Med-West Africa: 1200-2800 USD/TEU\n- Demurrage: 120-180 EUR/day\n- On-time: 55-70%"""},
    "transport_aerien_intl":{"keywords":["airfreight","air cargo","awb","iata","airline","express","dhl","fedex","ups"],"fr":"""BENCHMARKS FRET AERIEN INTERNATIONAL 2026:\n- Europe-Asie: 2,20-3,80 EUR/kg\n- Europe-Amerique: 2,80-4,50 EUR/kg\n- Europe-Afrique: 2,50-4,20 EUR/kg\n- FSC: 25-40%""","en":"""INTERNATIONAL AIR FREIGHT BENCHMARKS 2026:\n- Europe-Asia: 2.20-3.80 EUR/kg\n- Europe-Americas: 2.80-4.50 EUR/kg\n- Europe-Africa: 2.50-4.20 EUR/kg\n- FSC: 25-40%"""},
    "transport_routier_eu":{"keywords":["international","europe","cross-border","export","import","incoterm","douane"],"fr":"""BENCHMARKS TRANSPORT ROUTIER EUROPEEN 2026:\n- France-Espagne FTL: 1800-2400 EUR/trajet\n- France-Allemagne: 1600-2200 EUR/trajet\n- France-Maroc: 3200-4500 EUR/trajet\n- Cout/km EU: 1,85-2,30 EUR/km""","en":"""EUROPEAN ROAD BENCHMARKS 2026:\n- France-Spain FTL: 1800-2400 EUR/trip\n- France-Germany: 1600-2200 EUR/trip\n- France-Morocco: 3200-4500 EUR/trip\n- Cost/km: 1.85-2.30 EUR/km"""},
    "supply_chain_maghreb":{"keywords":["maroc","morocco","algerie","tunisie","casablanca","rabat","maghreb"],"fr":"""BENCHMARKS SUPPLY CHAIN MAGHREB 2026:\n- LPI Maroc: 3,2/5 (Banque Mondiale)\n- Transport Casablanca-Agadir: 1800-2500 MAD\n- Taux service PME Maroc: 85-92%\n- Delai dedouanement: 3-7 jours""","en":"""MAGHREB SUPPLY CHAIN BENCHMARKS 2026:\n- Morocco LPI: 3.2/5 (World Bank)\n- Casablanca-Agadir: 1800-2500 MAD\n- SME service level: 85-92%\n- Customs clearance: 3-7 days"""},
    "supply_chain_afrique":{"keywords":["afrique","africa","cote d'ivoire","ivory coast","senegal","abidjan","dakar","ghana","nigeria"],"fr":"""BENCHMARKS SUPPLY CHAIN AFRIQUE 2026:\n- LPI Cote d'Ivoire: 3,1/5 | Senegal: 2,9/5\n- Transport Abidjan-Bouake: 180000-250000 FCFA\n- Taux service distribution: 75-88%\n- Delai dedouanement: 5-12 jours""","en":"""SUB-SAHARAN AFRICA BENCHMARKS 2026:\n- Ivory Coast LPI: 3.1/5 | Senegal: 2.9/5\n- Abidjan-Bouake: 180000-250000 FCFA\n- Distribution service: 75-88%\n- Customs: 5-12 days"""},
    "generique":{"keywords":[],"fr":"""BENCHMARKS GENERIQUES SUPPLY CHAIN 2026:\n- Taux de service B2B minimum: > 93% | B2C: > 96%\n- Cout possession stock: 18-28% valeur/an\n- Rotation annuelle saine: > 4 fois/an\n- Marge transport saine: > 6%\n- BFR cible: < 60 jours de CA""","en":"""GENERIC SUPPLY CHAIN BENCHMARKS 2026:\n- B2B service level: > 93% | B2C: > 96%\n- Stock holding cost: 18-28% value/year\n- Annual turns: > 4x/year\n- Transport margin: > 6%\n- Target WCR: < 60 days revenue"""},
}


def detect_sector(df=None, module="stock", mode_detected=None):
    if module == "transport":
        if mode_detected:
            m = str(mode_detected).lower()
            if "maritime" in m or "sea" in m or "ocean" in m:
                if df is not None:
                    all_t = " ".join([str(v).lower() for v in df.values.flatten()[:100]])
                    intl_kw = ["rotterdam","anvers","hambourg","barcelona","tanger","abidjan","shanghai","container"]
                    if any(k in all_t for k in intl_kw):
                        return "transport_maritime_intl"
                return "transport_maritime"
            if "aerien" in m or "air" in m or "cargo" in m:
                return "transport_aerien_intl"
            if df is not None:
                all_t = " ".join([str(v).lower() for v in df.values.flatten()[:100]])
                eu_kw = ["espagne","spain","allemagne","germany","italie","belgique","maroc","morocco","export","import"]
                if any(k in all_t for k in eu_kw):
                    return "transport_routier_eu"
        return "transport_routier"
    if df is not None:
        all_text = " ".join([str(c).lower() for c in df.columns])
        if len(df) > 0:
            all_text += " " + " ".join(df.iloc[:, 0].astype(str).str.lower().head(30).tolist())
            all_text += " " + " ".join([str(v).lower() for v in df.values.flatten()[:200]])
        scores = {}
        for sk, sd in SECTORAL_DB.items():
            if sk in ("generique","transport_maritime_intl","transport_aerien_intl","transport_routier_eu","supply_chain_maghreb","supply_chain_afrique"):
                continue
            hits = sum(1 for kw in sd["keywords"] if kw in all_text)
            if hits >= 2:
                scores[sk] = hits
        if scores:
            return max(scores, key=scores.get)
        maghreb_kw = ["maroc","morocco","algerie","casablanca","rabat","tanger","tunis"]
        afrique_kw = ["abidjan","dakar","cote d'ivoire","ivory coast","accra","ghana","nigeria"]
        if any(k in all_text for k in afrique_kw):
            return "supply_chain_afrique"
        if any(k in all_text for k in maghreb_kw):
            return "supply_chain_maghreb"
    return "generique"


def get_sector_benchmarks(sector_key, lang="fr"):
    s = SECTORAL_DB.get(sector_key, SECTORAL_DB["generique"])
    return s.get(lang, s.get("fr", ""))
