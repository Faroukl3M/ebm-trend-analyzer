"""
trend_fetcher.py — Récupération des tendances depuis Google Trends + Amazon RSS + sources manuelles

Sources automatiques :
- Google Trends via PyTrends
- Amazon Best Sellers via flux RSS public (légal, gratuit, sans scraping)
- Amazon Movers & Shakers via flux RSS public
"""

import time
import random
import re
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from typing import Optional, List
import config


# ── AMAZON RSS FEEDS ──────────────────────────────────────────────────────────

AMAZON_RSS_FEEDS = {
    "fr": [
        ("Beauté Top général",     "https://www.amazon.fr/gp/rss/bestsellers/beauty/ref=zg_bs_beauty_rsslink"),
        ("Soin cheveux",           "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975541031/ref=zg_bs_2975541031_rsslink"),
        ("Soin visage",            "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975524031/ref=zg_bs_2975524031_rsslink"),
        ("Corps & Bain",           "https://www.amazon.fr/gp/rss/bestsellers/beauty/2975534031/ref=zg_bs_2975534031_rsslink"),
    ],
    "com": [
        ("Beauty Top General",     "https://www.amazon.com/gp/rss/bestsellers/beauty/ref=zg_bs_beauty_rsslink"),
        ("Hair Care",              "https://www.amazon.com/gp/rss/bestsellers/beauty/11057241/ref=zg_bs_11057241_rsslink"),
        ("Skin Care",              "https://www.amazon.com/gp/rss/bestsellers/beauty/11058281/ref=zg_bs_11058281_rsslink"),
    ],
}

AMAZON_MOVERS_RSS = {
    "fr":  "https://www.amazon.fr/gp/rss/movers-and-shakers/beauty/ref=zg_msar_beauty_rsslink",
    "com": "https://www.amazon.com/gp/rss/movers-and-shakers/beauty/ref=zg_msar_beauty_rsslink",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def _extract_product_name(title: str) -> str:
    """Nettoie un titre Amazon RSS : supprime rang et unités."""
    title = re.sub(r"^#?\d+\s*[-–—]?\s*", "", title).strip()
    title = re.sub(r"\s+\d+\s*(ml|g|oz|fl oz|pack|pcs|count|ct|x\d+).*$", "", title, flags=re.IGNORECASE)
    return title[:65].strip()


def fetch_amazon_rss(marketplace: str = "fr", max_per_feed: int = 10) -> List[dict]:
    """
    Récupère les Best Sellers Amazon Beauté via les flux RSS publics officiels.
    Aucun scraping — utilise uniquement les flux RSS fournis par Amazon.
    """
    results = []
    feeds = AMAZON_RSS_FEEDS.get(marketplace, AMAZON_RSS_FEEDS["fr"])

    for feed_name, url in feeds:
        try:
            print(f"  → RSS Amazon : {feed_name}")
            resp = requests.get(url, headers=HEADERS, timeout=12)
            if resp.status_code != 200:
                print(f"  ✗ HTTP {resp.status_code} pour {feed_name}")
                continue

            root = ET.fromstring(resp.content)
            items = root.findall(".//item")

            for rank, item in enumerate(items[:max_per_feed]):
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                product_name = _extract_product_name(title_el.text.strip())
                if len(product_name) < 5:
                    continue

                rank_score = max(100 - rank * 8, 30)
                results.append({
                    "product":          product_name,
                    "keyword":          product_name.lower(),
                    "source":           "Amazon",
                    "raw_score_google": 0,
                    "raw_score_amazon": rank_score,
                    "raw_score_tiktok": 0,
                    "category":         _infer_category(product_name),
                })

            time.sleep(random.uniform(0.8, 1.5))

        except ET.ParseError as e:
            print(f"  ✗ XML parse error {feed_name}: {e}")
        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout {feed_name}")
        except Exception as e:
            print(f"  ✗ Erreur {feed_name}: {e}")

    print(f"[amazon_rss] {len(results)} produits récupérés (Best Sellers).")
    return results


def fetch_amazon_movers_rss(marketplace: str = "fr") -> List[dict]:
    """Récupère les Movers & Shakers Amazon Beauté (produits en forte progression)."""
    url = AMAZON_MOVERS_RSS.get(marketplace, AMAZON_MOVERS_RSS["fr"])
    results = []
    try:
        print(f"  → RSS Amazon Movers & Shakers")
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for rank, item in enumerate(root.findall(".//item")[:15]):
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                product_name = _extract_product_name(title_el.text.strip())
                if len(product_name) < 5:
                    continue
                rank_score = max(95 - rank * 5, 40)
                results.append({
                    "product":          product_name,
                    "keyword":          product_name.lower(),
                    "source":           "Amazon",
                    "raw_score_google": 0,
                    "raw_score_amazon": rank_score,
                    "raw_score_tiktok": 0,
                    "category":         _infer_category(product_name),
                })
        time.sleep(1)
    except Exception as e:
        print(f"  ✗ Movers & Shakers non disponible : {e}")

    print(f"[amazon_rss] {len(results)} produits récupérés (Movers & Shakers).")
    return results


# ── GOOGLE TRENDS ─────────────────────────────────────────────────────────────

def fetch_google_trends(keywords: List[str], geo: str = "", timeframe: str = "today 3-m") -> List[dict]:
    """Récupère les requêtes en forte croissance via PyTrends."""
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
                        "product":          kw.title(),
                        "keyword":          kw,
                        "source":           "Google Trends",
                        "raw_score_google": 40,
                        "raw_score_amazon": 0,
                        "raw_score_tiktok": 0,
                        "category":         _infer_category(kw),
                    })
                time.sleep(2)

    except ImportError:
        print("[trend_fetcher] PyTrends non installé → fallback.")
        results = _fallback_trends()

    return results


# ── FALLBACK ──────────────────────────────────────────────────────────────────

def _fallback_trends() -> List[dict]:
    data = [
        ("Rosemary Hair Growth Oil",     "rosemary oil hair",          88, 0, 0, "Oils"),
        ("Rice Water Hair Rinse",         "rice water hair",            82, 0, 0, "Haircare"),
        ("Jamaican Black Castor Oil",     "castor oil hair",            78, 0, 0, "Oils"),
        ("Scalp Detox Shampoo",           "scalp detox shampoo",        72, 0, 0, "Haircare"),
        ("Curl Defining Cream",           "curl defining cream",        69, 0, 0, "Haircare"),
        ("Protein Hair Treatment",        "protein hair treatment",     65, 0, 0, "Haircare"),
        ("Edge Control Gel",              "edge control gel",           55, 0, 0, "Haircare"),
        ("Tranexamic Acid Serum",         "tranexamic acid dark spots", 85, 0, 0, "Skincare"),
        ("Snail Mucin Cream",             "snail mucin cream",          80, 0, 0, "Skincare"),
        ("Kojic Acid Soap",               "kojic acid soap",            75, 0, 0, "Skincare"),
        ("Niacinamide Serum 10%",         "niacinamide dark spots",     73, 0, 0, "Skincare"),
        ("African Black Soap",            "african black soap",         70, 0, 0, "Skincare"),
        ("HD Lace Front Wig",             "HD lace wig",                84, 0, 0, "Wigs"),
        ("Glueless Wig",                  "glueless wig",               79, 0, 0, "Wigs"),
        ("Braided Wig Box Braids",        "braided wig",                70, 0, 0, "Wigs"),
        ("Biotin Hair Growth Supplement", "biotin hair growth",         68, 0, 0, "Supplements beauté"),
    ]
    return [
        {"product": p, "keyword": k, "source": "Google Trends",
         "raw_score_google": sg, "raw_score_amazon": sa, "raw_score_tiktok": st, "category": cat}
        for p, k, sg, sa, st, cat in data
    ]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _infer_category(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["wig", "lace", "frontal", "closure", "perruque"]):
        return "Wigs"
    if any(w in t for w in ["oil", "huile", "castor", "argan", "jojoba", "rosemary"]):
        return "Oils"
    if any(w in t for w in ["shampoo", "shampoing", "conditioner", "curl", "hair", "cheveux",
                             "scalp", "braid", "loc", "edge", "bonnet"]):
        return "Haircare"
    if any(w in t for w in ["serum", "cream", "moisturizer", "skin", "peau", "soap", "savon",
                             "mask", "masque", "vitamin", "toner", "kojic", "niacinamide",
                             "tranexamic", "retinol", "hyaluronic", "snail", "spf"]):
        return "Skincare"
    if any(w in t for w in ["foundation", "concealer", "lipstick", "makeup", "maquillage",
                             "eyeshadow", "mascara", "blush", "highlighter"]):
        return "Makeup"
    if any(w in t for w in ["supplement", "biotin", "collagen", "iron", "zinc", "capsule"]):
        return "Supplements beauté"
    return "Haircare"


def fetch_manual_source(products: List[str], source_name: str, base_score: int = 65) -> List[dict]:
    score_key = f"raw_score_{source_name.lower().replace(' ', '_')}"
    results = []
    for i, p in enumerate(products):
        if not p.strip():
            continue
        r = {
            "product":          p.strip().title(),
            "keyword":          p.strip().lower(),
            "source":           source_name,
            "raw_score_google": 0,
            "raw_score_amazon": 0,
            "raw_score_tiktok": 0,
            "category":         _infer_category(p),
        }
        r[score_key] = max(base_score - i * 2, 30)
        results.append(r)
    return results


# ── AGGREGATOR ────────────────────────────────────────────────────────────────

def fetch_all_trends(
    category: Optional[str] = None,
    amazon_manual: Optional[List[str]] = None,
    tiktok_manual: Optional[List[str]] = None,
    amazon_marketplace: str = "fr",
) -> pd.DataFrame:
    """
    Agrège toutes les sources de tendances :
    1. Google Trends (automatique)
    2. Amazon Best Sellers RSS (automatique, légal)
    3. Amazon Movers & Shakers RSS (automatique, légal)
    4. Amazon / TikTok manuel (optionnel)
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

    # 2 & 3. Amazon RSS (automatique)
    print(f"[trend_fetcher] Amazon RSS (amazon.{amazon_marketplace})...")
    all_results.extend(fetch_amazon_rss(marketplace=amazon_marketplace))
    all_results.extend(fetch_amazon_movers_rss(marketplace=amazon_marketplace))

    # 4. Sources manuelles
    if amazon_manual:
        all_results.extend(fetch_manual_source(amazon_manual, "Amazon", base_score=75))
    if tiktok_manual:
        all_results.extend(fetch_manual_source(tiktok_manual, "TikTok", base_score=80))

    if not all_results:
        print("[trend_fetcher] Aucun résultat → fallback.")
        all_results = _fallback_trends()

    df = pd.DataFrame(all_results)

    for col in ["raw_score_google", "raw_score_amazon", "raw_score_tiktok"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "category" not in df.columns:
        df["category"] = "Haircare"

    # Fusion doublons — même produit → score max par source
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
