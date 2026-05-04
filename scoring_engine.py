"""
scoring_engine.py — Calcul du score de tendance final (0-100)
Corrections v2.1 :
- Score de base relevé pour les sources uniques (évite médiane à 35)
- Bonus Amazon RSS (données réelles = plus fiables que fallback)
- Meilleure gestion quand une seule source disponible
"""

import pandas as pd
from config import RECOMMENDATION_SCORE_MIN

HIGH_POTENTIAL_KEYWORDS = [
    "rosemary oil", "castor oil", "rice water", "edge control",
    "lace front", "hd lace", "glueless wig", "kojic acid",
    "african black soap", "shea butter", "twist out", "deep conditioner",
    "protein treatment", "scalp treatment", "niacinamide", "tranexamic",
    "snail mucin", "curl cream", "bonnet", "silk pillowcase",
    "peppermint", "braided wig", "scalp scrub", "glycolic", "azelaic",
]

SATURATED_KEYWORDS = ["coconut oil", "argan oil", "biotin"]
SEASONAL_KEYWORDS  = ["summer hair", "winter moisturizer", "holiday wig"]

# Scores de base par source quand c'est la seule disponible
# (fallback TikTok = données statiques, score de base plus bas)
BASE_SCORE_GOOGLE_ONLY  = 42   # Google Trends seul
BASE_SCORE_AMAZON_ONLY  = 55   # Amazon RSS = données réelles → plus fiable
BASE_SCORE_TIKTOK_ONLY  = 38   # TikTok fallback statique → score plus conservateur


def compute_scores(df: pd.DataFrame, weights: dict) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    w_g = weights.get("google", 40) / 100
    w_t = weights.get("tiktok", 35) / 100
    w_a = weights.get("amazon", 25) / 100

    def compute_single(row):
        g = float(row.get("raw_score_google", 0))
        t = float(row.get("raw_score_tiktok", 0))
        a = float(row.get("raw_score_amazon", 0))
        source = str(row.get("source", ""))

        # Score pondéré brut
        score = g * w_g + t * w_t + a * w_a

        # Score de base minimum selon la source
        # (évite les scores à 35 quand une seule source à score faible)
        num_sources = sum([g > 0, t > 0, a > 0])
        if num_sources == 1:
            if g > 0:
                score = max(score, BASE_SCORE_GOOGLE_ONLY)
            elif a > 0:
                # Amazon RSS = données réelles → score de base plus élevé
                score = max(score, BASE_SCORE_AMAZON_ONLY)
                # Bonus supplémentaire si c'est un Movers & Shakers
                if a >= 80:
                    score = min(score + 10, 100)
            elif t > 0:
                # TikTok : distinguer vrai scraping (score élevé) vs fallback (score modéré)
                if t >= 70:
                    score = max(score, 55)   # vrai scraping TikTok
                else:
                    score = max(score, BASE_SCORE_TIKTOK_ONLY)
        elif num_sources >= 2:
            # Présent sur plusieurs sources = signal fort
            score = max(score, 55)

        # Bonus produits à fort potentiel EBM
        text = str(row.get("keyword", "")).lower() + " " + str(row.get("product", "")).lower()
        for hp in HIGH_POTENTIAL_KEYWORDS:
            if hp in text:
                score = min(score + 8, 100)
                break

        # Pénalité marché saturé
        for sat in SATURATED_KEYWORDS:
            if sat in text:
                score = max(score - 5, 0)
                break

        return round(min(score, 100), 1)

    df["trend_score"] = df.apply(compute_single, axis=1)

    # Source combinée
    source_map = {}
    for _, row in df.iterrows():
        p = row["product"]
        source_map.setdefault(p, set()).add(row.get("source", ""))
    df["source_combined"] = df["product"].apply(
        lambda p: " + ".join(sorted(source_map.get(p, set())))
    )

    # Recommandation
    def recommend(row):
        if row["statut"] == "Disponible":
            return "NON"
        return "OUI" if row["trend_score"] >= RECOMMENDATION_SCORE_MIN else "NON"

    df["recommendation"] = df.apply(recommend, axis=1)
    return df


def get_score_label(score: float) -> str:
    if score >= 75:   return "🔥 Très fort"
    elif score >= 55: return "📈 Fort"
    elif score >= 40: return "📊 Moyen"
    else:             return "📉 Faible"
