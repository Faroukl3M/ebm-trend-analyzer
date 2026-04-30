"""
matcher.py — Matching intelligent tendances ↔ catalogue EBM
Compatible RapidFuzz 3.x (process.extract au lieu de extractBests)
Intègre la notion de stock : Disponible / Proche / Absent / Rupture (tendance)
"""

import re
import pandas as pd
from typing import Tuple, Optional
from config import STOPWORDS, MATCH_THRESHOLD_STRONG, MATCH_THRESHOLD_MEDIUM

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    print("[matcher] RapidFuzz non installé — matching basique activé.")

# Dictionnaire de traduction FR→EN
FR_EN_DICT = {
    "huile": "oil", "huiles": "oils", "huile de": "oil",
    "cheveux": "hair", "capillaire": "hair", "soin": "care",
    "crème": "cream", "creme": "cream", "masque": "mask",
    "shampoing": "shampoo", "après-shampoing": "conditioner",
    "apres-shampoing": "conditioner", "beurre": "butter",
    "romarin": "rosemary", "ricin": "castor", "argan": "argan",
    "jojoba": "jojoba", "coco": "coconut", "menthe": "peppermint",
    "sérum": "serum", "peau": "skin", "nourrissant": "nourishing",
    "hydratant": "moisturizing", "pousse": "growth", "croissance": "growth",
    "perruque": "wig", "perruques": "wigs", "tresse": "braid",
    "lacet": "lace", "frontal": "frontal", "savon": "soap",
    "noir": "black", "blanc": "white", "karité": "shea", "karite": "shea",
    "avocat": "avocado", "brillant": "shine", "éclat": "glow",
    "fortifiant": "strengthening", "réparateur": "repairing",
    "bouclés": "curly", "naturels": "natural", "secs": "dry",
    "complément": "supplement", "supplément": "supplement",
    "maquillage": "makeup", "fond de teint": "foundation",
    "spray": "spray", "gel": "gel", "lotion": "lotion",
    "gommage": "scrub", "exfoliant": "exfoliant",
    "dentelle": "lace", "coloration": "color",
}


def normalize_for_matching(text: str) -> str:
    """Normalise un texte pour le matching (suppression stopwords, FR→EN)."""
    if not isinstance(text, str):
        return ""
    text = text.lower().strip()
    for fr, en in FR_EN_DICT.items():
        text = re.sub(r"\b" + re.escape(fr) + r"\b", en, text)
    text = re.sub(r"[^\w\s]", " ", text)
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
            return 60.0
    return 0.0


def find_best_match(
    trend_product: str,
    trend_keyword: str,
    catalogue_df: pd.DataFrame,
) -> Tuple[float, Optional[str], Optional[str], Optional[str], Optional[float]]:
    """
    Cherche le meilleur match dans le catalogue.

    Retourne : (score_match, titre_catalogue, handle, stock_status, inventory_qty)
    """
    trend_norm = normalize_for_matching(trend_product)
    kw_norm = normalize_for_matching(trend_keyword)

    best_score = 0.0
    best_title = None
    best_handle = None
    best_stock_status = None
    best_inventory = None

    if catalogue_df.empty:
        return 0.0, None, None, None, None

    # Pré-filtrage RapidFuzz 3.x — process.extract (remplace extractBests)
    if RAPIDFUZZ_AVAILABLE and len(catalogue_df) > 30:
        titles_norm = catalogue_df["title_normalized"].tolist()
        candidates = process.extract(
            trend_norm,
            titles_norm,
            scorer=fuzz.token_set_ratio,
            score_cutoff=20,
            limit=10,
        )
        candidate_indices = []
        for c in candidates:
            title_val = c[0]
            try:
                candidate_indices.append(titles_norm.index(title_val))
            except ValueError:
                pass
        sub_df = catalogue_df.iloc[candidate_indices] if candidate_indices else catalogue_df
    else:
        sub_df = catalogue_df

    for _, row in sub_df.iterrows():
        cat_norm = row["title_normalized"]
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
    Pour chaque produit tendance, calcule le statut par rapport au catalogue.

    Statuts possibles :
    - Disponible        : match fort (≥75%) + en stock
    - Proche            : match moyen (40-74%) + en stock
    - Rupture (tendance): match fort/moyen MAIS stock = 0 ou rupture
    - Absent            : aucun match (<40%)
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
            catalogue_df,
        )

        match_scores.append(score)
        matched_titles.append(title or "")
        matched_handles.append(handle or "")
        stock_statuses.append(stock_status or "")
        inventories.append(float(inventory) if inventory is not None else 0.0)

        # Logique statut avec stock
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

    print(f"[matcher] Résultats :")
    for s in ["Disponible", "Proche", "Absent", "Rupture (tendance)"]:
        print(f"  {s:22s}: {statuts.count(s)}")

    return result
