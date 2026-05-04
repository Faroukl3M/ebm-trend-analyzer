"""
matcher.py — Matching intelligent tendances ↔ catalogue EBM
Corrections v2.1 :
- Seuils relevés (58% Proche, 78% Disponible)
- Vérification de cohérence catégorie (évite Lip Gloss → Hair Gloss)
- "hair" et "skin" ne sont plus des stopwords (causaient faux matches)
- Bonus si même catégorie, pénalité si catégories incompatibles
"""

import re
import pandas as pd
from typing import Tuple, Optional
from config import (STOPWORDS, MATCH_THRESHOLD_STRONG, MATCH_THRESHOLD_MEDIUM,
                    MATCH_REQUIRE_SAME_CATEGORY)

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

# Dictionnaire FR→EN
FR_EN_DICT = {
    "huile": "oil", "huiles": "oils", "capillaire": "hair",
    "cheveux": "hair", "soin": "care", "soins": "care",
    "crème": "cream", "creme": "cream", "masque": "mask",
    "shampoing": "shampoo", "après-shampoing": "conditioner",
    "apres-shampoing": "conditioner", "beurre": "butter",
    "romarin": "rosemary", "ricin": "castor", "argan": "argan",
    "jojoba": "jojoba", "coco": "coconut", "menthe": "peppermint",
    "sérum": "serum", "peau": "skin", "nourrissant": "nourishing",
    "hydratant": "moisturizing", "pousse": "growth", "croissance": "growth",
    "perruque": "wig", "perruques": "wigs", "tresse": "braid",
    "lacet": "lace", "savon": "soap", "noir": "black", "blanc": "white",
    "karité": "shea", "karite": "shea", "avocat": "avocado",
    "fortifiant": "strengthening", "réparateur": "repairing",
    "bouclés": "curly", "naturels": "natural", "secs": "dry",
    "complément": "supplement", "supplément": "supplement",
    "maquillage": "makeup", "fond de teint": "foundation",
    "gommage": "scrub", "exfoliant": "exfoliant",
    "dentelle": "lace", "coloration": "color", "lèvres": "lips",
    "brillance": "gloss", "eclat": "glow", "éclat": "glow",
}

# Catégories incompatibles — un match entre ces catégories est forcément faux
INCOMPATIBLE_CATEGORIES = [
    {"Haircare", "Skincare"},
    {"Haircare", "Makeup"},
    {"Haircare", "Supplements beauté"},
    {"Wigs", "Skincare"},
    {"Wigs", "Makeup"},
    {"Wigs", "Supplements beauté"},
    {"Wigs", "Oils"},
    {"Makeup", "Supplements beauté"},
]


def _categories_compatible(cat1: str, cat2: str) -> bool:
    """Vérifie si deux catégories sont compatibles pour un match."""
    if not cat1 or not cat2:
        return True
    if cat1 == cat2:
        return True
    pair = {cat1, cat2}
    for incompatible in INCOMPATIBLE_CATEGORIES:
        if pair == incompatible:
            return False
    return True


def normalize_for_matching(text: str) -> str:
    """Normalise un texte pour le matching (FR→EN, suppression stopwords)."""
    if not isinstance(text, str):
        return ""
    text = text.lower().strip()
    for fr, en in FR_EN_DICT.items():
        text = re.sub(r"\b" + re.escape(fr) + r"\b", en, text)
    text = re.sub(r"[^\w\s]", " ", text)
    # Supprime les tailles/volumes qui causent des faux matches
    text = re.sub(r"\b\d+\s*(ml|g|oz|mg|cl|l)\b", "", text)
    tokens = [t for t in text.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(tokens)


def token_overlap_score(text1: str, text2: str) -> float:
    tokens1 = set(text1.split())
    tokens2 = set(text2.split())
    if not tokens1 or not tokens2:
        return 0.0
    return (len(tokens1 & tokens2) / len(tokens1 | tokens2)) * 100


def fuzzy_score(text1: str, text2: str) -> float:
    if RAPIDFUZZ_AVAILABLE:
        return max(
            fuzz.ratio(text1, text2),
            fuzz.partial_ratio(text1, text2),
            fuzz.token_sort_ratio(text1, text2),
            fuzz.token_set_ratio(text1, text2),
        )
    return token_overlap_score(text1, text2)


def tag_match_score(trend_keyword: str, tags_list: list) -> float:
    if not tags_list:
        return 0.0
    kw_tokens = set(normalize_for_matching(trend_keyword).split())
    for tag in tags_list:
        tag_tokens = set(normalize_for_matching(tag).split())
        if kw_tokens & tag_tokens:
            return 50.0
    return 0.0


def find_best_match(
    trend_product: str,
    trend_keyword: str,
    trend_category: str,
    catalogue_df: pd.DataFrame,
) -> Tuple[float, Optional[str], Optional[str], Optional[str], Optional[float]]:
    """
    Cherche le meilleur match dans le catalogue.
    Applique une pénalité si les catégories sont incompatibles.
    """
    trend_norm = normalize_for_matching(trend_product)
    kw_norm    = normalize_for_matching(trend_keyword)

    best_score        = 0.0
    best_title        = None
    best_handle       = None
    best_stock_status = None
    best_inventory    = None

    if catalogue_df.empty:
        return 0.0, None, None, None, None

    # Pré-filtrage RapidFuzz
    if RAPIDFUZZ_AVAILABLE and len(catalogue_df) > 30:
        titles_norm = catalogue_df["title_normalized"].tolist()
        candidates = process.extract(
            trend_norm, titles_norm,
            scorer=fuzz.token_set_ratio,
            score_cutoff=25,
            limit=15,
        )
        candidate_indices = []
        for c in candidates:
            try:
                candidate_indices.append(titles_norm.index(c[0]))
            except ValueError:
                pass
        sub_df = catalogue_df.iloc[candidate_indices] if candidate_indices else catalogue_df
    else:
        sub_df = catalogue_df

    for _, row in sub_df.iterrows():
        cat_norm     = row["title_normalized"]
        row_category = row.get("category", "")

        # ── Score brut ────────────────────────────────────────────────────────
        score_fuzzy  = fuzzy_score(trend_norm, cat_norm)
        score_kw     = fuzzy_score(kw_norm, cat_norm)
        score_tokens = token_overlap_score(trend_norm, cat_norm)
        score_tags   = tag_match_score(trend_keyword, row.get("tags_list", []))

        combined = (
            score_fuzzy  * 0.45 +
            score_kw     * 0.25 +
            score_tokens * 0.20 +
            score_tags   * 0.10
        )

        # ── Bonus même catégorie ──────────────────────────────────────────────
        if trend_category and row_category:
            if trend_category == row_category:
                combined = min(combined * 1.08, 100)  # +8% bonus
            elif not _categories_compatible(trend_category, row_category):
                # Catégories incompatibles → pénalité forte
                combined = combined * 0.45

        if combined > best_score:
            best_score        = combined
            best_title        = row["Title"]
            best_handle       = row["Handle"]
            best_stock_status = row.get("stock_status", "")
            best_inventory    = row.get("Total Inventory Qty", 0)

    return (
        round(best_score, 1),
        best_title,
        best_handle,
        best_stock_status,
        best_inventory,
    )


def match_trends_to_catalogue(
    trends_df: pd.DataFrame,
    catalogue_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Statuts :
    - Disponible        : match >= 78% + catégories compatibles + en stock
    - Proche            : match >= 58% + catégories compatibles + en stock
    - Rupture (tendance): match fort MAIS stock = 0
    - Absent            : match < 58% OU catégories incompatibles
    """
    if trends_df.empty:
        return trends_df

    match_scores    = []
    matched_titles  = []
    matched_handles = []
    stock_statuses  = []
    inventories     = []
    statuts         = []

    for _, row in trends_df.iterrows():
        score, title, handle, stock_status, inventory = find_best_match(
            row["product"],
            row.get("keyword", row["product"]),
            row.get("category", ""),
            catalogue_df,
        )

        match_scores.append(score)
        matched_titles.append(title or "")
        matched_handles.append(handle or "")
        stock_statuses.append(stock_status or "")
        inventories.append(float(inventory) if inventory is not None else 0.0)

        if score >= MATCH_THRESHOLD_STRONG:
            if stock_status in ("Rupture", "Stock faible"):
                statuts.append("Rupture (tendance)")
            else:
                statuts.append("Disponible")
        elif score >= MATCH_THRESHOLD_MEDIUM:
            if stock_status == "Rupture":
                statuts.append("Rupture (tendance)")
            else:
                statuts.append("Proche")
        else:
            statuts.append("Absent")

    result = trends_df.copy()
    result["match_score"]           = match_scores
    result["matched_title"]         = matched_titles
    result["matched_handle"]        = matched_handles
    result["matched_stock_status"]  = stock_statuses
    result["matched_inventory_qty"] = inventories
    result["statut"]                = statuts

    print(f"[matcher] Résultats (seuils: Disponible>={MATCH_THRESHOLD_STRONG}%, Proche>={MATCH_THRESHOLD_MEDIUM}%):")
    for s in ["Disponible", "Proche", "Absent", "Rupture (tendance)"]:
        print(f"  {s:22s}: {statuts.count(s)}")

    return result
