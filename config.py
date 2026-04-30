"""
config.py — Configuration centrale de l'outil EBM Trend Analyzer
"""

# ── PONDÉRATION DES SOURCES (doit totaliser 100) ──────────────────────────────
WEIGHT_GOOGLE = 40   # Google Trends
WEIGHT_TIKTOK = 35   # TikTok
WEIGHT_AMAZON = 25   # Amazon

# ── CATÉGORIES PRODUITS ───────────────────────────────────────────────────────
CATEGORIES = [
    "Haircare",
    "Skincare",
    "Makeup",
    "Wigs",
    "Oils",
    "Supplements beauté",
]

# ── MOTS-CLÉS PAR CATÉGORIE POUR GOOGLE TRENDS ───────────────────────────────
CATEGORY_KEYWORDS = {
    "Haircare": [
        "black hair care", "natural hair products", "hair growth oil",
        "deep conditioner", "twist out cream", "edge control",
        "rice water hair", "rosemary hair oil", "hair butter",
        "protective style", "curl defining cream", "jamaican black castor oil",
    ],
    "Skincare": [
        "black skin care", "hyperpigmentation cream", "vitamin C serum",
        "niacinamide dark spots", "shea butter moisturizer", "kojic acid soap",
        "retinol for dark skin", "african black soap", "turmeric face mask",
        "snail cream", "hyaluronic acid serum", "glycolic acid toner",
    ],
    "Makeup": [
        "foundation for dark skin", "melanin makeup", "concealer dark skin",
        "black owned makeup", "eyeshadow palette brown skin",
        "lip liner dark skin", "highlighter dark skin",
    ],
    "Wigs": [
        "lace front wig", "HD lace wig", "glueless wig",
        "braided wig", "synthetic wig", "human hair wig",
        "bob wig", "closure wig", "headband wig",
    ],
    "Oils": [
        "hair growth oil", "rosemary oil hair", "castor oil hair",
        "argan oil hair", "jojoba oil skin", "coconut oil hair",
        "black seed oil hair", "peppermint oil hair growth",
        "scalp oil treatment",
    ],
    "Supplements beauté": [
        "hair skin nail supplements", "biotin hair growth",
        "collagen supplements skin", "vitamin d skin",
        "iron supplements hair loss", "zinc hair growth",
    ],
}

# Mots-clés généraux beauté ethnique
GENERAL_KEYWORDS = [
    "ethnic hair care", "afro hair products", "curly hair products",
    "natural hair growth", "protective hairstyle products",
    "melanin skincare", "dark skin care",
]

# ── SEUILS DE MATCHING ────────────────────────────────────────────────────────
MATCH_THRESHOLD_STRONG = 75    # >= 75% → Disponible
MATCH_THRESHOLD_MEDIUM = 40    # >= 40% → Proche
# < 40% → Absent

# ── SEUIL RECOMMANDATION ──────────────────────────────────────────────────────
RECOMMENDATION_SCORE_MIN = 45  # Score tendance minimum pour recommander

# ── PYTRENDS ─────────────────────────────────────────────────────────────────
PYTRENDS_GEO = ""          # "" = mondial, "FR" = France, "US" = USA
PYTRENDS_TIMEFRAME = "today 3-m"   # 3 derniers mois
PYTRENDS_LANGUAGE = "fr"

# ── STOPWORDS FR/EN pour normalisation ───────────────────────────────────────
STOPWORDS = {
    "le", "la", "les", "de", "du", "des", "un", "une", "et", "en", "pour",
    "avec", "sans", "sur", "the", "a", "an", "for", "and", "with", "of",
    "to", "in", "hair", "skin", "beauty", "soin", "cheveux", "peau",
}
