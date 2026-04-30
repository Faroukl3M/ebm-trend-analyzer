"""
scoring_engine.py — Calcul du score de tendance final (0-100)

Formule : Score = Google*w_g + TikTok*w_t + Amazon*w_a
+ bonus de fraîcheur, pénalité marché saturé
"""

import pandas as pd
from config import RECOMMENDATION_SCORE_MIN


# Produits à potentiel élevé pour EBM (boost de score)
HIGH_POTENTIAL_KEYWORDS = [
    "rosemary oil", "castor oil", "rice water", "edge control",
    "lace front", "HD lace", "glueless wig", "kojic acid",
    "african black soap", "shea butter", "twist out", "deep conditioner",
    "protein treatment", "scalp treatment", "niacinamide", "tranexamic",
    "snail mucin", "curl cream", "bonnet", "silk pillowcase",
]

# Produits sur marché saturé (légère pénalité)
SATURATED_KEYWORDS = [
    "coconut oil", "argan oil", "biotin",
]

# Produits saisonniers (commentaire de risque ajouté)
SEASONAL_KEYWORDS = [
    "summer hair", "winter moisturizer", "holiday wig",
]


def compute_scores(df: pd.DataFrame, weights: dict) -> pd.DataFrame:
    """
    Calcule le score de tendance final pour chaque produit.

    Args:
        df: DataFrame issu de match_trends_to_catalogue
        weights: dict {"google": int, "tiktok": int, "amazon": int}

    Retourne df avec colonne "trend_score" (0-100).
    """
    if df.empty:
        return df

    df = df.copy()

    w_g = weights.get("google", 40) / 100
    w_t = weights.get("tiktok", 35) / 100
    w_a = weights.get("amazon", 25) / 100

    def compute_single_score(row):
        g = float(row.get("raw_score_google", 0))
        t = float(row.get("raw_score_tiktok", 0))
        a = float(row.get("raw_score_amazon", 0))

        # Score pondéré de base
        score = g * w_g + t * w_t + a * w_a

        # Si une seule source, partir d'un score de base raisonnable
        num_sources = sum([g > 0, t > 0, a > 0])
        if num_sources == 1:
            score = max(score, 35)

        # Bonus produits à fort potentiel EBM
        kw = str(row.get("keyword", row.get("product", ""))).lower()
        product_lower = str(row.get("product", "")).lower()
        text = kw + " " + product_lower
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

    df["trend_score"] = df.apply(compute_single_score, axis=1)

    # Source consolidée (liste les sources uniques)
    source_map = {}
    for idx, row in df.iterrows():
        src = row.get("source", "")
        prod = row["product"]
        if prod not in source_map:
            source_map[prod] = set()
        source_map[prod].add(src)

    df["source_combined"] = df["product"].apply(
        lambda p: " + ".join(sorted(source_map.get(p, set())))
    )

    # Recommandation (OUI si absent/proche ET score suffisant)
    def recommend(row):
        if row["statut"] == "Disponible":
            return "NON"
        if row["trend_score"] >= RECOMMENDATION_SCORE_MIN:
            return "OUI"
        return "NON"

    df["recommendation"] = df.apply(recommend, axis=1)

    return df


def get_score_label(score: float) -> str:
    if score >= 75:
        return "🔥 Très fort"
    elif score >= 55:
        return "📈 Fort"
    elif score >= 35:
        return "📊 Moyen"
    else:
        return "📉 Faible"
