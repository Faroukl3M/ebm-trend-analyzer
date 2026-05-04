"""
trend_fetcher.py — Google Trends + Amazon RSS + TikTok (non-officiel) + fallbacks

Sources :
1. Google Trends — PyTrends (automatique)
2. Amazon Best Sellers + Movers & Shakers — RSS officiel (automatique, légal)
3. TikTok — TikTokApi non-officielle (semi-auto, peut planter) avec fallback
4. Manuel — saisie libre Amazon / TikTok

AVERTISSEMENT TikTok :
TikTokApi utilise Playwright pour simuler un navigateur.
Usage personnel/interne uniquement. Peut se casser à chaque MAJ TikTok.
"""

import time
import random
import re
import asyncio
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from typing import Optional, List
import config


# ── HASHTAGS BEAUTÉ ETHNIQUE À SCRAPER ───────────────────────────────────────
TIKTOK_HASHTAGS = [
    "hairtok",
    "naturalhair",
    "haircare",
    "skincare",
    "tiktokmademebuyit",
    "hairtreatment",
    "blackgirlhair",
    "curlyhair",
    "afrohairtok",
    "wiginstall",
    "lacefrontwig",
    "skincareblackgirl",
]

# Nombre de vidéos à analyser par hashtag
TIKTOK_VIDEOS_PER_HASHTAG = 15


# ── TIKTOK SCRAPER (TikTokApi non-officielle) ─────────────────────────────────

async def _scrape_tiktok_hashtag_async(hashtag: str, count: int = 15) -> List[dict]:
    """
    Scrape les vidéos trending d'un hashtag TikTok via TikTokApi.
    Extrait les noms de produits mentionnés dans les descriptions/titres.
    """
    results = []
    try:
        from TikTokApi import TikTokApi

        async with TikTokApi() as api:
            await api.create_sessions(
                ms_tokens=[None],   # sans token = mode anonyme
                num_sessions=1,
                sleep_after=3,
                headless=True,
            )

            tag = api.hashtag(name=hashtag)
            videos = []
            async for video in tag.videos(count=count):
                videos.append(video)

            for rank, video in enumerate(videos):
                try:
                    info = video.as_dict
                    desc = info.get("desc", "")
                    stats = info.get("stats", {})
                    plays    = int(stats.get("playCount", 0))
                    likes    = int(stats.get("diggCount", 0))
                    comments = int(stats.get("commentCount", 0))

                    # Score virabilité basé sur les vues et likes
                    viral_score = _compute_viral_score(plays, likes, comments, rank)

                    # Extraction produits depuis la description
                    products_found = _extract_products_from_text(desc)
                    for prod in products_found:
                        results.append({
                            "product":          prod,
                            "keyword":          prod.lower(),
                            "source":           "TikTok",
                            "raw_score_google": 0,
                            "raw_score_amazon": 0,
                            "raw_score_tiktok": viral_score,
                            "category":         _infer_category(prod),
                            "tiktok_plays":     plays,
                            "tiktok_hashtag":   hashtag,
                        })

                except Exception:
                    continue

    except ImportError:
        print(f"  ✗ TikTokApi non installée (pip install TikTokApi)")
    except Exception as e:
        print(f"  ✗ TikTok hashtag #{hashtag} : {e}")

    return results


def _compute_viral_score(plays: int, likes: int, comments: int, rank: int) -> float:
    """Calcule un score de viralité TikTok normalisé 0-100."""
    # Pondération : vues 50%, likes 35%, commentaires 15%
    play_score    = min(plays    / 1_000_000 * 50, 50)
    like_score    = min(likes    / 100_000   * 35, 35)
    comment_score = min(comments / 10_000    * 15, 15)
    base = play_score + like_score + comment_score
    # Bonus position dans le feed
    rank_bonus = max(10 - rank, 0)
    return round(min(base + rank_bonus, 100), 1)


# Produits beauté connus à détecter dans les textes TikTok
BEAUTY_PRODUCT_PATTERNS = [
    # Haircare
    r"rosemary\s*(hair\s*)?oil",
    r"rice\s*water",
    r"castor\s*oil",
    r"edge\s*control",
    r"leave[- ]in\s*(conditioner)?",
    r"deep\s*conditioner",
    r"protein\s*(treatment|mask)",
    r"scalp\s*(oil|serum|treatment|scrub)",
    r"curl\s*(cream|defin|activat)",
    r"twist\s*(out|cream)",
    r"bonnet\s*(cap)?",
    r"silk\s*pillowcase",
    r"hair\s*(mask|butter|growth|oil|serum|spray)",
    r"jamaican\s*black\s*castor",
    r"peppermint\s*oil",
    r"braiding\s*(cream|gel|spray)",
    # Skincare
    r"niacinamide\s*(serum)?",
    r"tranexamic\s*(acid)?",
    r"kojic\s*(acid|soap)?",
    r"snail\s*mucin",
    r"vitamin\s*[cC]\s*(serum)?",
    r"retinol\s*(cream|serum)?",
    r"hyaluronic\s*(acid)?",
    r"african\s*black\s*soap",
    r"shea\s*(butter|moisture)",
    r"turmeric\s*(mask|soap|serum)?",
    r"glycolic\s*acid",
    r"azelaic\s*acid",
    r"face\s*(oil|mask|serum|mist)",
    r"skin\s*(tint|barrier|gloss)",
    # Wigs
    r"(hd\s*)?lace\s*(front\s*)?(wig|closure|frontal)",
    r"glueless\s*wig",
    r"braided\s*wig",
    r"headband\s*wig",
    # Makeup
    r"skin\s*tint",
    r"tinted\s*moisturizer",
    r"(lip\s*)?(gloss|liner|oil)",
    r"blush\s*(draping)?",
    # Supplements
    r"hair\s*(vitamin|gummies|supplement)",
    r"biotin\s*(gummies|supplement)?",
    r"collagen\s*(peptides|supplement|powder)?",
]

COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in BEAUTY_PRODUCT_PATTERNS]

# Noms canoniques pour normaliser les extractions
PRODUCT_CANONICAL = {
    "rosemary hair oil":        "Rosemary Hair Growth Oil",
    "rosemary oil":             "Rosemary Hair Growth Oil",
    "rice water":               "Rice Water Hair Rinse",
    "castor oil":               "Jamaican Black Castor Oil",
    "jamaican black castor":    "Jamaican Black Castor Oil",
    "edge control":             "Edge Control Gel",
    "deep conditioner":         "Deep Conditioner Treatment",
    "protein treatment":        "Protein Hair Treatment",
    "protein mask":             "Protein Hair Mask",
    "scalp oil":                "Scalp Treatment Oil",
    "scalp serum":              "Scalp Serum",
    "scalp treatment":          "Scalp Treatment",
    "curl cream":               "Curl Defining Cream",
    "curl activat":             "Curl Activator Cream",
    "silk pillowcase":          "Silk Pillowcase for Hair",
    "bonnet":                   "Satin Hair Bonnet",
    "leave-in":                 "Leave-In Conditioner",
    "leave in conditioner":     "Leave-In Conditioner",
    "niacinamide":              "Niacinamide Serum 10%",
    "tranexamic":               "Tranexamic Acid Serum",
    "kojic acid":               "Kojic Acid Serum",
    "kojic soap":               "Kojic Acid Soap",
    "snail mucin":              "Snail Mucin Cream",
    "vitamin c serum":          "Vitamin C Brightening Serum",
    "retinol":                  "Retinol Serum",
    "african black soap":       "African Black Soap",
    "shea moisture":            "Shea Moisture Hair Product",
    "shea butter":              "Shea Butter Cream",
    "turmeric mask":            "Turmeric Face Mask",
    "glycolic acid":            "Glycolic Acid Toner",
    "azelaic acid":             "Azelaic Acid Serum",
    "hd lace wig":              "HD Lace Front Wig",
    "lace front wig":           "Lace Front Wig",
    "glueless wig":             "Glueless Wig",
    "braided wig":              "Braided Wig",
    "headband wig":             "Headband Wig",
    "hair vitamin":             "Hair Vitamins Supplement",
    "biotin":                   "Biotin Hair Supplement",
    "collagen":                 "Collagen Supplement",
    "lip gloss":                "Lip Gloss",
    "lip oil":                  "Nourishing Lip Oil",
    "skin tint":                "Skin Tint Foundation",
    "blush":                    "Blush Makeup",
    "peppermint oil":           "Peppermint Scalp Oil",
    "braiding cream":           "Braiding Cream",
    "braiding gel":             "Braiding Gel",
    "twist out":                "Twist Out Cream",
    "twist cream":              "Twist & Define Cream",
    "hair butter":              "Hair Butter",
    "hair growth":              "Hair Growth Treatment",
    "hair mask":                "Deep Hair Mask",
    "hair serum":               "Hair Serum",
    "hyaluronic":               "Hyaluronic Acid Serum",
    "face oil":                 "Face Oil",
    "face mask":                "Face Mask",
    "scalp scrub":              "Scalp Scrub Exfoliant",
}


def _extract_products_from_text(text: str) -> List[str]:
    """Extrait les noms de produits beauté d'un texte TikTok."""
    found = []
    for pattern in COMPILED_PATTERNS:
        match = pattern.search(text)
        if match:
            raw = match.group(0).lower().strip()
            # Cherche un nom canonique
            canonical = None
            for key, name in PRODUCT_CANONICAL.items():
                if key in raw:
                    canonical = name
                    break
            if canonical:
                found.append(canonical)
            else:
                found.append(raw.title())
    return list(set(found))  # dédoublonnage


def fetch_tiktok_trends(
    hashtags: Optional[List[str]] = None,
    videos_per_hashtag: int = TIKTOK_VIDEOS_PER_HASHTAG,
) -> List[dict]:
    """
    Point d'entrée synchrone pour le scraping TikTok.
    Gère automatiquement le fallback si TikTokApi échoue.
    """
    hashtags = hashtags or TIKTOK_HASHTAGS
    results = []

    print(f"[tiktok] Tentative scraping {len(hashtags)} hashtags...")

    try:
        # TikTokApi nécessite asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for hashtag in hashtags[:6]:  # max 6 hashtags pour éviter les bans
            print(f"  → #{hashtag}")
            try:
                batch = loop.run_until_complete(
                    _scrape_tiktok_hashtag_async(hashtag, videos_per_hashtag)
                )
                results.extend(batch)
                print(f"    ✓ {len(batch)} produits extraits")
                time.sleep(random.uniform(2.0, 4.0))  # pause entre hashtags
            except Exception as e:
                print(f"    ✗ #{hashtag} échoué : {e}")
                continue

        loop.close()

    except Exception as e:
        print(f"[tiktok] Erreur générale : {e}")

    if not results:
        print("[tiktok] Scraping échoué → fallback données TikTok statiques")
        results = _tiktok_fallback()
    else:
        print(f"[tiktok] {len(results)} tendances TikTok récupérées")

    return results


def _tiktok_fallback() -> List[dict]:
    """
    Données TikTok de fallback — tendances beauté ethnique virales 2024-2025.
    Utilisées si TikTokApi échoue.
    """
    data = [
        ("Rosemary Hair Growth Oil",  "rosemary oil",       92, "Oils"),
        ("Rice Water Hair Rinse",      "rice water hair",    88, "Haircare"),
        ("Scalp Scrub Exfoliant",      "scalp scrub",        84, "Haircare"),
        ("Snail Mucin Cream",          "snail mucin",        82, "Skincare"),
        ("Tranexamic Acid Serum",      "tranexamic acid",    80, "Skincare"),
        ("Niacinamide Serum 10%",      "niacinamide",        78, "Skincare"),
        ("Glueless Wig",               "glueless wig",       77, "Wigs"),
        ("HD Lace Front Wig",          "hd lace wig",        75, "Wigs"),
        ("Satin Hair Bonnet",          "bonnet hair",        73, "Haircare"),
        ("Silk Pillowcase for Hair",   "silk pillowcase",    70, "Haircare"),
        ("Leave-In Conditioner",       "leave in",           68, "Haircare"),
        ("Protein Hair Treatment",     "protein treatment",  65, "Haircare"),
        ("Edge Control Gel",           "edge control",       63, "Haircare"),
        ("Kojic Acid Soap",            "kojic soap",         62, "Skincare"),
        ("African Black Soap",         "black soap",         60, "Skincare"),
        ("Twist & Define Cream",       "twist out cream",    58, "Haircare"),
        ("Glycolic Acid Toner",        "glycolic acid",      57, "Skincare"),
        ("Lip Gloss",                  "lip gloss",          55, "Makeup"),
        ("Nourishing Lip Oil",         "lip oil",            54, "Makeup"),
        ("Braided Wig",                "braided wig",        52, "Wigs"),
        ("Hair Growth Serum",          "hair growth serum",  50, "Haircare"),
        ("Biotin Hair Supplement",     "biotin",             48, "Supplements beauté"),
        ("Collagen Supplement",        "collagen",           45, "Supplements beauté"),
        ("Skin Tint Foundation",       "skin tint",          44, "Makeup"),
        ("Peppermint Scalp Oil",       "peppermint scalp",   42, "Oils"),
    ]
    return [
        {
            "product":          p,
            "keyword":          k,
            "source":           "TikTok",
            "raw_score_google": 0,
            "raw_score_amazon": 0,
            "raw_score_tiktok": sc,
            "category":         cat,
        }
        for p, k, sc, cat in data
    ]


# ── AMAZON RSS ────────────────────────────────────────────────────────────────

AMAZON_RSS_FEEDS = {
    "fr": [
        ("Beauté Top général", "https://www.amazon.fr/gp/rss/bestsellers/beauty/ref=zg_bs_beauty_rsslink"),
        ("Soin cheveux",       "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975541031/ref=zg_bs_2975541031_rsslink"),
        ("Soin visage",        "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975524031/ref=zg_bs_2975524031_rsslink"),
        ("Corps & Bain",       "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975534031/ref=zg_bs_2975534031_rsslink"),
    ],
    "com": [
        ("Beauty Top General", "https://www.amazon.com/gp/rss/bestsellers/beauty/ref=zg_bs_beauty_rsslink"),
        ("Hair Care",          "https://www.amazon.com/gp/rss/bestsellers/beauty/11057241/ref=zg_bs_11057241_rsslink"),
        ("Skin Care",          "https://www.amazon.com/gp/rss/bestsellers/beauty/11058281/ref=zg_bs_11058281_rsslink"),
    ],
}
AMAZON_MOVERS_RSS = {
    "fr":  "https://www.amazon.fr/gp/rss/movers-and-shakers/beauty/ref=zg_msar_beauty_rsslink",
    "com": "https://www.amazon.com/gp/rss/movers-and-shakers/beauty/ref=zg_msar_beauty_rsslink",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def _extract_product_name(title: str) -> str:
    title = re.sub(r"^#?\d+\s*[-–—]?\s*", "", title).strip()
    title = re.sub(r"\s+\d+\s*(ml|g|oz|fl oz|pack|pcs|count|ct|x\d+).*$", "", title, flags=re.IGNORECASE)
    return title[:65].strip()


def fetch_amazon_rss(marketplace: str = "fr", max_per_feed: int = 10) -> List[dict]:
    results = []
    for feed_name, url in AMAZON_RSS_FEEDS.get(marketplace, AMAZON_RSS_FEEDS["fr"]):
        try:
            print(f"  → RSS Amazon : {feed_name}")
            resp = requests.get(url, headers=HEADERS, timeout=12)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for rank, item in enumerate(root.findall(".//item")[:max_per_feed]):
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                name = _extract_product_name(title_el.text.strip())
                if len(name) < 5:
                    continue
                results.append({
                    "product":          name,
                    "keyword":          name.lower(),
                    "source":           "Amazon",
                    "raw_score_google": 0,
                    "raw_score_amazon": max(100 - rank * 8, 30),
                    "raw_score_tiktok": 0,
                    "category":         _infer_category(name),
                })
            time.sleep(random.uniform(0.8, 1.5))
        except Exception as e:
            print(f"  ✗ {feed_name}: {e}")
    return results


def fetch_amazon_movers_rss(marketplace: str = "fr") -> List[dict]:
    results = []
    try:
        resp = requests.get(AMAZON_MOVERS_RSS.get(marketplace, AMAZON_MOVERS_RSS["fr"]), headers=HEADERS, timeout=12)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for rank, item in enumerate(root.findall(".//item")[:15]):
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                name = _extract_product_name(title_el.text.strip())
                if len(name) < 5:
                    continue
                results.append({
                    "product":          name,
                    "keyword":          name.lower(),
                    "source":           "Amazon",
                    "raw_score_google": 0,
                    "raw_score_amazon": max(95 - rank * 5, 40),
                    "raw_score_tiktok": 0,
                    "category":         _infer_category(name),
                })
        time.sleep(1)
    except Exception as e:
        print(f"  ✗ Movers & Shakers : {e}")
    return results


# ── GOOGLE TRENDS ─────────────────────────────────────────────────────────────

def fetch_google_trends(keywords: List[str], geo: str = "", timeframe: str = "today 3-m") -> List[dict]:
    results = []
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl="fr-FR", tz=60, timeout=(10, 25), retries=2, backoff_factor=0.5)
        for i in range(0, len(keywords), 5):
            batch = keywords[i:i + 5]
            try:
                pytrends.build_payload(batch, cat=0, timeframe=timeframe, geo=geo)
                time.sleep(random.uniform(1.5, 3.0))
                related = pytrends.related_queries()
                for kw in batch:
                    if kw in related and related[kw] and related[kw].get("rising") is not None:
                        rising_df = related[kw]["rising"]
                        if rising_df is not None and not rising_df.empty:
                            for _, row in rising_df.head(5).iterrows():
                                results.append({
                                    "product":          row["query"].title(),
                                    "keyword":          kw,
                                    "source":           "Google Trends",
                                    "raw_score_google": min(int(row.get("value", 50)), 100),
                                    "raw_score_amazon": 0,
                                    "raw_score_tiktok": 0,
                                    "category":         _infer_category(row["query"]),
                                })
                interest = pytrends.interest_over_time()
                if interest is not None and not interest.empty:
                    for kw in batch:
                        if kw in interest.columns:
                            recent = interest[kw].tail(4).mean()
                            older  = interest[kw].head(8).mean()
                            if older > 0 and ((recent - older) / older) * 100 > 20:
                                growth = ((recent - older) / older) * 100
                                results.append({
                                    "product":          kw.title(),
                                    "keyword":          kw,
                                    "source":           "Google Trends",
                                    "raw_score_google": min(int(50 + growth / 4), 100),
                                    "raw_score_amazon": 0,
                                    "raw_score_tiktok": 0,
                                    "category":         _infer_category(kw),
                                })
            except Exception as e:
                print(f"  ✗ Batch {batch}: {e}")
                for kw in batch:
                    results.append({
                        "product": kw.title(), "keyword": kw,
                        "source": "Google Trends", "raw_score_google": 40,
                        "raw_score_amazon": 0, "raw_score_tiktok": 0,
                        "category": _infer_category(kw),
                    })
                time.sleep(2)
    except ImportError:
        results = _fallback_google()
    return results


def _fallback_google() -> List[dict]:
    data = [
        ("Rosemary Hair Growth Oil",     "rosemary oil hair",          88, "Oils"),
        ("Rice Water Hair Rinse",         "rice water hair",            82, "Haircare"),
        ("Jamaican Black Castor Oil",     "castor oil hair",            78, "Oils"),
        ("Scalp Detox Shampoo",           "scalp detox shampoo",        72, "Haircare"),
        ("Curl Defining Cream",           "curl defining cream",        69, "Haircare"),
        ("Protein Hair Treatment",        "protein hair treatment",     65, "Haircare"),
        ("Tranexamic Acid Serum",         "tranexamic acid dark spots", 85, "Skincare"),
        ("Snail Mucin Cream",             "snail mucin cream",          80, "Skincare"),
        ("Kojic Acid Soap",               "kojic acid soap",            75, "Skincare"),
        ("Niacinamide Serum 10%",         "niacinamide dark spots",     73, "Skincare"),
        ("HD Lace Front Wig",             "HD lace wig",                84, "Wigs"),
        ("Glueless Wig",                  "glueless wig",               79, "Wigs"),
        ("Biotin Hair Growth Supplement", "biotin hair growth",         68, "Supplements beauté"),
    ]
    return [{"product": p, "keyword": k, "source": "Google Trends",
             "raw_score_google": sg, "raw_score_amazon": 0, "raw_score_tiktok": 0, "category": cat}
            for p, k, sg, cat in data]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _infer_category(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["wig", "lace", "frontal", "closure", "perruque"]):
        return "Wigs"
    if any(w in t for w in ["oil", "huile", "castor", "argan", "jojoba", "rosemary", "peppermint"]):
        return "Oils"
    if any(w in t for w in ["shampoo", "shampoing", "conditioner", "curl", "hair", "cheveux",
                             "scalp", "braid", "loc", "edge", "bonnet", "silk pillowcase", "twist"]):
        return "Haircare"
    if any(w in t for w in ["serum", "cream", "moisturizer", "skin", "peau", "soap", "savon",
                             "mask", "masque", "vitamin", "toner", "kojic", "niacinamide",
                             "tranexamic", "retinol", "hyaluronic", "snail", "spf", "glycolic",
                             "azelaic", "turmeric", "glow", "brightening"]):
        return "Skincare"
    if any(w in t for w in ["foundation", "concealer", "lipstick", "makeup", "maquillage",
                             "eyeshadow", "mascara", "blush", "highlighter", "lip gloss",
                             "lip oil", "skin tint"]):
        return "Makeup"
    if any(w in t for w in ["supplement", "biotin", "collagen", "iron", "zinc", "capsule", "gummies"]):
        return "Supplements beauté"
    return "Haircare"


def fetch_manual_source(products: List[str], source_name: str, base_score: int = 65) -> List[dict]:
    score_key = f"raw_score_{source_name.lower()}"
    results = []
    for i, p in enumerate(products):
        if not p.strip():
            continue
        r = {"product": p.strip().title(), "keyword": p.strip().lower(),
             "source": source_name, "raw_score_google": 0,
             "raw_score_amazon": 0, "raw_score_tiktok": 0,
             "category": _infer_category(p)}
        r[score_key] = max(base_score - i * 2, 30)
        results.append(r)
    return results


# ── AGGREGATOR ────────────────────────────────────────────────────────────────

def fetch_all_trends(
    category: Optional[str] = None,
    amazon_manual: Optional[List[str]] = None,
    tiktok_manual: Optional[List[str]] = None,
    amazon_marketplace: str = "fr",
    enable_tiktok_scraping: bool = False,
    tiktok_hashtags: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Agrège toutes les sources :
    1. Google Trends
    2. Amazon RSS (Best Sellers + Movers & Shakers)
    3. TikTok scraping (si enable_tiktok_scraping=True)
    4. Sources manuelles
    """
    all_results = []

    # 1. Google Trends
    keywords = (
        config.CATEGORY_KEYWORDS.get(category, [])
        if category and category in config.CATEGORY_KEYWORDS
        else config.GENERAL_KEYWORDS + [kw for kws in config.CATEGORY_KEYWORDS.values() for kw in kws[:3]]
    )
    print(f"[trend_fetcher] Google Trends ({len(keywords)} keywords)...")
    all_results.extend(fetch_google_trends(keywords, geo=config.PYTRENDS_GEO, timeframe=config.PYTRENDS_TIMEFRAME))

    # 2. Amazon RSS
    print(f"[trend_fetcher] Amazon RSS (amazon.{amazon_marketplace})...")
    all_results.extend(fetch_amazon_rss(marketplace=amazon_marketplace))
    all_results.extend(fetch_amazon_movers_rss(marketplace=amazon_marketplace))

    # 3. TikTok scraping
    if enable_tiktok_scraping:
        print(f"[trend_fetcher] TikTok scraping activé...")
        hashtags = tiktok_hashtags or TIKTOK_HASHTAGS
        tiktok_results = fetch_tiktok_trends(hashtags=hashtags)
        all_results.extend(tiktok_results)
    elif tiktok_manual:
        pass  # géré ci-dessous

    # 4. Manuels
    if amazon_manual:
        all_results.extend(fetch_manual_source(amazon_manual, "Amazon", base_score=75))
    if tiktok_manual:
        all_results.extend(fetch_manual_source(tiktok_manual, "TikTok", base_score=80))

    if not all_results:
        print("[trend_fetcher] Aucun résultat → fallback complet.")
        all_results = _fallback_google() + _tiktok_fallback()

    # Consolidation
    df = pd.DataFrame(all_results)
    for col in ["raw_score_google", "raw_score_amazon", "raw_score_tiktok"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "category" not in df.columns:
        df["category"] = "Haircare"

    df = df.groupby("product", as_index=False).agg({
        "keyword":          "first",
        "source":           lambda x: " + ".join(sorted(set(x))),
        "raw_score_google": "max",
        "raw_score_amazon": "max",
        "raw_score_tiktok": "max",
        "category":         "first",
    })

    if category and category not in ("Tous", None):
        df = df[df["category"] == category]

    df = df.reset_index(drop=True)
    print(f"[trend_fetcher] {len(df)} tendances uniques agrégées.")
    return df
