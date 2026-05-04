"""
trend_fetcher.py — Google Trends + Amazon RSS + TikTok (non-officiel)
Corrections v2.1 :
- Diagnostic Amazon RSS (affiche pourquoi ça échoue)
- Vérification Playwright installé avant de tenter TikTok
- Fallback TikTok avec scores plus réalistes (distingue vrai scraping vs statique)
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
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

TIKTOK_HASHTAGS = [
    "hairtok", "naturalhair", "haircare", "skincare",
    "tiktokmademebuyit", "hairtreatment", "blackgirlhair",
    "curlyhair", "afrohairtok", "wiginstall", "lacefrontwig",
]


# ── AMAZON RSS ────────────────────────────────────────────────────────────────

def _extract_product_name(title: str) -> str:
    title = re.sub(r"^#?\d+\s*[-–—]?\s*", "", title).strip()
    title = re.sub(r"\s+\d+\s*(ml|g|oz|fl oz|pack|pcs|count|ct|x\d+).*$", "", title, flags=re.IGNORECASE)
    return title[:65].strip()


def fetch_amazon_rss(marketplace: str = "fr", max_per_feed: int = 10) -> List[dict]:
    results = []
    feeds = AMAZON_RSS_FEEDS.get(marketplace, AMAZON_RSS_FEEDS["fr"])

    for feed_name, url in feeds:
        try:
            print(f"  → RSS Amazon : {feed_name}")
            resp = requests.get(url, headers=HEADERS, timeout=15)

            # Diagnostic explicite
            if resp.status_code == 403:
                print(f"  ✗ Amazon RSS bloqué (403 Forbidden) pour {feed_name} — Amazon bloque cette IP/région")
                continue
            elif resp.status_code == 503:
                print(f"  ✗ Amazon RSS indisponible (503) pour {feed_name}")
                continue
            elif resp.status_code != 200:
                print(f"  ✗ HTTP {resp.status_code} pour {feed_name}")
                continue

            # Vérification contenu RSS valide
            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type.lower():
                print(f"  ✗ {feed_name} : Amazon a retourné une page HTML (CAPTCHA ou redirection)")
                continue

            root = ET.fromstring(resp.content)
            items = root.findall(".//item")

            if not items:
                print(f"  ✗ {feed_name} : flux RSS vide")
                continue

            for rank, item in enumerate(items[:max_per_feed]):
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
                    "raw_score_amazon": max(100 - rank * 8, 40),
                    "raw_score_tiktok": 0,
                    "category":         _infer_category(name),
                })

            print(f"  ✓ {feed_name} : {min(len(items), max_per_feed)} produits")
            time.sleep(random.uniform(1.0, 2.0))

        except ET.ParseError as e:
            print(f"  ✗ XML invalide {feed_name}: {e}")
        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout {feed_name} (>15s)")
        except requests.exceptions.ConnectionError:
            print(f"  ✗ Connexion impossible {feed_name} — vérifiez votre réseau")
        except Exception as e:
            print(f"  ✗ Erreur {feed_name}: {type(e).__name__}: {e}")

    if not results:
        print("  ⚠️  Amazon RSS : 0 produits récupérés — utilisation fallback Amazon")
        results = _amazon_fallback()

    print(f"[amazon_rss] Total : {len(results)} produits")
    return results


def _amazon_fallback() -> List[dict]:
    """Fallback Amazon — Best Sellers beauté ethnique représentatifs."""
    data = [
        ("Cantu Shea Butter Leave In Conditioning Repair Cream", 92, "Haircare"),
        ("SheaMoisture Jamaican Black Castor Oil Strengthen Restore Shampoo", 88, "Haircare"),
        ("Mielle Organics Rosemary Mint Scalp & Hair Strengthening Oil", 86, "Oils"),
        ("Aveeno Oat Gel Moisturizer for Sensitive Skin", 82, "Skincare"),
        ("African Pride Moisture Miracle Honey & Coconut Oil Curl Milk", 80, "Haircare"),
        ("Dark and Lovely Fade Resist Rich Conditioning Color", 78, "Haircare"),
        ("Creme of Nature Pure Honey Hair Food Moisture Curl Defining", 76, "Haircare"),
        ("Garnier Fructis Hair Filler Protein-Treat Shampoo", 74, "Haircare"),
        ("ORS Olive Oil Incredibly Rich Oil Moisturizing Hair Lotion", 72, "Oils"),
        ("Palmer's Cocoa Butter Formula Moisturizing Body Lotion", 70, "Skincare"),
    ]
    return [{"product": p, "keyword": p.lower(), "source": "Amazon",
             "raw_score_google": 0, "raw_score_amazon": s, "raw_score_tiktok": 0,
             "category": c} for p, s, c in data]


def fetch_amazon_movers_rss(marketplace: str = "fr") -> List[dict]:
    results = []
    try:
        print(f"  → RSS Amazon Movers & Shakers")
        resp = requests.get(AMAZON_MOVERS_RSS.get(marketplace, AMAZON_MOVERS_RSS["fr"]),
                            headers=HEADERS, timeout=15)
        if resp.status_code == 200 and "html" not in resp.headers.get("Content-Type", "").lower():
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
            print(f"  ✓ Movers & Shakers : {len(results)} produits")
        else:
            print(f"  ✗ Movers & Shakers : HTTP {resp.status_code}")
        time.sleep(1)
    except Exception as e:
        print(f"  ✗ Movers & Shakers : {e}")
    return results


# ── TIKTOK ────────────────────────────────────────────────────────────────────

BEAUTY_PRODUCT_PATTERNS = [
    (r"rosemary\s*(hair\s*)?oil",             "Rosemary Hair Growth Oil"),
    (r"rice\s*water",                          "Rice Water Hair Rinse"),
    (r"jamaican\s*black\s*castor",             "Jamaican Black Castor Oil"),
    (r"castor\s*oil",                          "Castor Oil"),
    (r"edge\s*control",                        "Edge Control Gel"),
    (r"leave[- ]in\s*(conditioner)?",          "Leave-In Conditioner"),
    (r"deep\s*conditioner",                    "Deep Conditioner Treatment"),
    (r"protein\s*(treatment|mask)",            "Protein Hair Treatment"),
    (r"scalp\s*scrub",                         "Scalp Scrub Exfoliant"),
    (r"scalp\s*(oil|serum|treatment)",         "Scalp Treatment Oil"),
    (r"curl\s*(cream|defin|activat)",          "Curl Defining Cream"),
    (r"twist\s*(out|cream)",                   "Twist & Define Cream"),
    (r"bonnet\s*(cap)?",                       "Satin Hair Bonnet"),
    (r"silk\s*pillowcase",                     "Silk Pillowcase for Hair"),
    (r"hair\s*butter",                         "Hair Butter"),
    (r"hair\s*growth\s*(oil|serum|spray)",     "Hair Growth Serum"),
    (r"braiding\s*(cream|gel)",                "Braiding Gel"),
    (r"niacinamide",                           "Niacinamide Serum 10%"),
    (r"tranexamic\s*(acid)?",                  "Tranexamic Acid Serum"),
    (r"kojic\s*(acid|soap)?",                  "Kojic Acid Soap"),
    (r"snail\s*mucin",                         "Snail Mucin Cream"),
    (r"vitamin\s*[cC]\s*(serum|cream)?",       "Vitamin C Brightening Serum"),
    (r"retinol",                               "Retinol Serum"),
    (r"hyaluronic\s*(acid)?",                  "Hyaluronic Acid Serum"),
    (r"african\s*black\s*soap",               "African Black Soap"),
    (r"glycolic\s*acid",                       "Glycolic Acid Toner"),
    (r"azelaic\s*acid",                        "Azelaic Acid Serum"),
    (r"turmeric\s*(mask|soap|serum)?",         "Turmeric Face Mask"),
    (r"(hd\s*)?lace\s*front\s*wig",           "HD Lace Front Wig"),
    (r"glueless\s*wig",                        "Glueless Wig"),
    (r"braided\s*wig",                         "Braided Wig"),
    (r"headband\s*wig",                        "Headband Wig"),
    (r"skin\s*tint",                           "Skin Tint Foundation"),
    (r"lip\s*oil",                             "Nourishing Lip Oil"),
    (r"lip\s*gloss",                           "Lip Gloss"),
    (r"blush\s*(draping)?",                    "Blush Makeup"),
    (r"biotin\s*(gummies|supplement)?",        "Biotin Hair Supplement"),
    (r"collagen\s*(peptides|powder)?",         "Collagen Supplement"),
    (r"peppermint\s*(oil|scalp)",              "Peppermint Scalp Oil"),
]
COMPILED_PATTERNS = [(re.compile(p, re.IGNORECASE), name) for p, name in BEAUTY_PRODUCT_PATTERNS]


def _extract_products_from_text(text: str) -> List[str]:
    found = []
    for pattern, canonical_name in COMPILED_PATTERNS:
        if pattern.search(text):
            found.append(canonical_name)
    return list(set(found))


def _compute_viral_score(plays: int, likes: int, comments: int, rank: int) -> float:
    play_score    = min(plays    / 1_000_000 * 50, 50)
    like_score    = min(likes    / 100_000   * 35, 35)
    comment_score = min(comments / 10_000    * 15, 15)
    rank_bonus    = max(10 - rank, 0)
    return round(min(play_score + like_score + comment_score + rank_bonus, 100), 1)


def _check_playwright_installed() -> bool:
    """Vérifie si Playwright + Chromium sont bien installés."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True
    except Exception as e:
        print(f"  ✗ Playwright/Chromium non disponible : {e}")
        print("  → Solution : exécutez 'python -m playwright install chromium'")
        return False


async def _scrape_tiktok_hashtag_async(hashtag: str, count: int = 15) -> List[dict]:
    results = []
    try:
        from TikTokApi import TikTokApi
        async with TikTokApi() as api:
            await api.create_sessions(ms_tokens=[None], num_sessions=1,
                                      sleep_after=3, headless=True)
            tag = api.hashtag(name=hashtag)
            async for rank, video in enumerate(tag.videos(count=count)):
                try:
                    info     = video.as_dict
                    desc     = info.get("desc", "")
                    stats    = info.get("stats", {})
                    plays    = int(stats.get("playCount", 0))
                    likes    = int(stats.get("diggCount", 0))
                    comments = int(stats.get("commentCount", 0))
                    score    = _compute_viral_score(plays, likes, comments, rank)
                    for prod in _extract_products_from_text(desc):
                        results.append({
                            "product":          prod,
                            "keyword":          prod.lower(),
                            "source":           "TikTok",
                            "raw_score_google": 0,
                            "raw_score_amazon": 0,
                            "raw_score_tiktok": score,
                            "category":         _infer_category(prod),
                        })
                except Exception:
                    continue
    except Exception as e:
        print(f"  ✗ #{hashtag} : {e}")
    return results


def fetch_tiktok_trends(hashtags: Optional[List[str]] = None) -> List[dict]:
    hashtags = hashtags or TIKTOK_HASHTAGS

    # Vérification Playwright avant de tenter
    if not _check_playwright_installed():
        print("  → Fallback TikTok statique activé")
        return _tiktok_fallback()

    results = []
    print(f"[tiktok] Scraping {len(hashtags)} hashtags...")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for hashtag in hashtags[:6]:
            print(f"  → #{hashtag}")
            try:
                batch = loop.run_until_complete(_scrape_tiktok_hashtag_async(hashtag))
                results.extend(batch)
                print(f"    ✓ {len(batch)} produits extraits")
                time.sleep(random.uniform(2.0, 4.0))
            except Exception as e:
                print(f"    ✗ #{hashtag} : {e}")
        loop.close()
    except Exception as e:
        print(f"[tiktok] Erreur : {e}")

    if not results:
        print("[tiktok] Aucun résultat → fallback statique")
        return _tiktok_fallback()

    print(f"[tiktok] {len(results)} tendances récupérées (vrai scraping ✓)")
    return results


def _tiktok_fallback() -> List[dict]:
    """
    Données TikTok statiques — scores modérés (35-65) pour refléter
    qu'il s'agit de données de fallback et non du vrai scraping.
    """
    data = [
        ("Rosemary Hair Growth Oil",  "rosemary oil",      65, "Oils"),
        ("Rice Water Hair Rinse",      "rice water hair",   62, "Haircare"),
        ("Snail Mucin Cream",          "snail mucin",       60, "Skincare"),
        ("Tranexamic Acid Serum",      "tranexamic acid",   58, "Skincare"),
        ("Niacinamide Serum 10%",      "niacinamide",       56, "Skincare"),
        ("Scalp Scrub Exfoliant",      "scalp scrub",       54, "Haircare"),
        ("Glueless Wig",               "glueless wig",      53, "Wigs"),
        ("HD Lace Front Wig",          "hd lace wig",       52, "Wigs"),
        ("Satin Hair Bonnet",          "bonnet hair",       50, "Haircare"),
        ("Silk Pillowcase for Hair",   "silk pillowcase",   48, "Haircare"),
        ("Leave-In Conditioner",       "leave in",          47, "Haircare"),
        ("Protein Hair Treatment",     "protein treatment", 46, "Haircare"),
        ("Edge Control Gel",           "edge control",      44, "Haircare"),
        ("Kojic Acid Soap",            "kojic soap",        43, "Skincare"),
        ("African Black Soap",         "black soap",        42, "Skincare"),
        ("Twist & Define Cream",       "twist out cream",   41, "Haircare"),
        ("Glycolic Acid Toner",        "glycolic acid",     40, "Skincare"),
        ("Lip Gloss",                  "lip gloss",         38, "Makeup"),
        ("Nourishing Lip Oil",         "lip oil",           37, "Makeup"),
        ("Braided Wig",                "braided wig",       36, "Wigs"),
        ("Hair Growth Serum",          "hair growth serum", 35, "Haircare"),
        ("Biotin Hair Supplement",     "biotin",            35, "Supplements beauté"),
        ("Skin Tint Foundation",       "skin tint",         35, "Makeup"),
        ("Peppermint Scalp Oil",       "peppermint scalp",  35, "Oils"),
        ("Collagen Supplement",        "collagen",          35, "Supplements beauté"),
    ]
    return [{"product": p, "keyword": k, "source": "TikTok",
             "raw_score_google": 0, "raw_score_amazon": 0, "raw_score_tiktok": sc,
             "category": cat} for p, k, sc, cat in data]


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
                                    "raw_score_amazon": 0, "raw_score_tiktok": 0,
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
                                    "raw_score_amazon": 0, "raw_score_tiktok": 0,
                                    "category":         _infer_category(kw),
                                })
            except Exception as e:
                print(f"  ✗ Batch {batch}: {e}")
                for kw in batch:
                    results.append({"product": kw.title(), "keyword": kw,
                                    "source": "Google Trends", "raw_score_google": 42,
                                    "raw_score_amazon": 0, "raw_score_tiktok": 0,
                                    "category": _infer_category(kw)})
                time.sleep(2)
    except ImportError:
        print("[trend_fetcher] PyTrends non installé")
        results = _google_fallback()
    return results


def _google_fallback() -> List[dict]:
    data = [
        ("Rosemary Hair Growth Oil",     "rosemary oil hair",          88, "Oils"),
        ("Rice Water Hair Rinse",         "rice water hair",            82, "Haircare"),
        ("Jamaican Black Castor Oil",     "castor oil hair",            78, "Oils"),
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
    if any(w in t for w in ["wig", "lace front", "glueless", "braided wig", "headband wig", "perruque"]):
        return "Wigs"
    if any(w in t for w in [" oil", "huile", "castor", "argan", "jojoba", "rosemary", "peppermint"]):
        return "Oils"
    if any(w in t for w in ["shampoo", "shampoing", "conditioner", "curl", "hair", "cheveux",
                             "scalp", "braid", "edge", "bonnet", "twist", "silk pillowcase"]):
        return "Haircare"
    if any(w in t for w in ["serum", "cream", "moisturizer", "skin", "peau", "soap", "savon",
                             "mask", "masque", "vitamin", "toner", "kojic", "niacinamide",
                             "tranexamic", "retinol", "hyaluronic", "snail", "spf", "glycolic",
                             "azelaic", "turmeric", "glow", "brightening", "exfoliant", "scrub"]):
        return "Skincare"
    if any(w in t for w in ["foundation", "concealer", "lipstick", "makeup", "maquillage",
                             "eyeshadow", "mascara", "blush", "highlighter", "lip gloss",
                             "lip oil", "skin tint"]):
        return "Makeup"
    if any(w in t for w in ["supplement", "biotin", "collagen", "iron", "zinc", "capsule", "gummies", "vitamin"]):
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

    all_results = []

    # 1. Google Trends
    keywords = (config.CATEGORY_KEYWORDS.get(category, [])
                if category and category in config.CATEGORY_KEYWORDS
                else config.GENERAL_KEYWORDS + [kw for kws in config.CATEGORY_KEYWORDS.values() for kw in kws[:3]])
    print(f"[trend_fetcher] Google Trends ({len(keywords)} keywords)...")
    all_results.extend(fetch_google_trends(keywords, geo=config.PYTRENDS_GEO, timeframe=config.PYTRENDS_TIMEFRAME))

    # 2. Amazon RSS
    print(f"[trend_fetcher] Amazon RSS (amazon.{amazon_marketplace})...")
    all_results.extend(fetch_amazon_rss(marketplace=amazon_marketplace))
    all_results.extend(fetch_amazon_movers_rss(marketplace=amazon_marketplace))

    # 3. TikTok
    if enable_tiktok_scraping:
        all_results.extend(fetch_tiktok_trends(hashtags=tiktok_hashtags))
    elif tiktok_manual:
        pass  # géré ci-dessous

    # 4. Manuels
    if amazon_manual:
        all_results.extend(fetch_manual_source(amazon_manual, "Amazon", base_score=75))
    if tiktok_manual:
        all_results.extend(fetch_manual_source(tiktok_manual, "TikTok", base_score=80))

    if not all_results:
        all_results = _google_fallback() + _tiktok_fallback() + _amazon_fallback()

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
