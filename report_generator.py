"""
report_generator.py — Génération du rapport final avec commentaires de justification
Intègre la notion de stock (Rupture / Stock faible / En stock)
"""

import pandas as pd
from typing import Optional
from scoring_engine import get_score_label, SATURATED_KEYWORDS, SEASONAL_KEYWORDS

SOURCE_DESCRIPTIONS = {
    "Google Trends":                    "croissance des recherches Google",
    "Amazon":                           "Best Seller Amazon Beauté",
    "TikTok":                           "viral sur TikTok/TikTokShop",
    "Amazon + Google Trends":           "Best Seller Amazon ET croissance Google",
    "Google Trends + TikTok":           "viral TikTok ET croissance Google",
    "Amazon + TikTok":                  "Best Seller Amazon ET viral TikTok",
    "Amazon + Google Trends + TikTok":  "tendance TRIFACE : Google + Amazon + TikTok",
}

CATEGORY_CONTEXT = {
    "Haircare":             "très recherché par les femmes aux cheveux afro, bouclés et texturés",
    "Skincare":             "demande forte chez les peaux noires et métissées pour l'éclat et l'uniformité du teint",
    "Makeup":               "marché en forte croissance pour les nuances adaptées aux peaux foncées",
    "Wigs":                 "segment en explosion avec l'essor des perruques HD lace et glueless",
    "Oils":                 "pilier de la routine cheveux natural hair, forte récurrence d'achat",
    "Supplements beauté":   "tendance croissante en beauté de l'intérieur",
}


def _build_comment(row: pd.Series) -> str:
    product    = row.get("product", "")
    statut     = row.get("statut", "")
    score      = float(row.get("trend_score", 0))
    source     = str(row.get("source_combined", row.get("source", "")))
    category   = str(row.get("category", ""))
    matched    = str(row.get("matched_title", ""))
    keyword    = str(row.get("keyword", product)).lower()
    stock_stat = str(row.get("matched_stock_status", ""))
    inventory  = float(row.get("matched_inventory_qty", 0))

    trend_label   = get_score_label(score)
    sources_desc  = SOURCE_DESCRIPTIONS.get(source, f"trending ({source})")
    category_desc = CATEGORY_CONTEXT.get(category, "catégorie beauté ethnique")

    potential = (
        "Fort — positionnement immédiat recommandé" if score >= 75 else
        "Moyen à Fort — à sourcer rapidement"       if score >= 55 else
        "Moyen — à surveiller"
    )

    risk_parts = []
    for sat in SATURATED_KEYWORDS:
        if sat in keyword:
            risk_parts.append("⚠️ Marché potentiellement saturé (nombreux concurrents)")
            break
    for sea in SEASONAL_KEYWORDS:
        if sea in keyword:
            risk_parts.append("⚠️ Produit potentiellement saisonnier")
            break
    if score < 45:
        risk_parts.append("⚠️ Tendance encore émergente — surveiller avant de commander")
    risk_note = " ".join(risk_parts) if risk_parts else "Risques limités identifiés."

    # ── Commentaires par statut ───────────────────────────────────────────────
    if statut == "Absent":
        comment = (
            f"{product} est {trend_label} sur le marché beauté ({sources_desc}). "
            f"Ce produit est absent de votre catalogue EBM — opportunité directe à saisir. "
            f"Pertinence EBM : {category_desc}. "
            f"Potentiel business : {potential}. {risk_note}"
        )

    elif statut == "Rupture (tendance)":
        stock_info = f"stock actuel = {int(inventory)} unité(s)" if inventory >= 0 else "stock = 0"
        comment = (
            f"{product} est {trend_label} ({sources_desc}) et vous avez un produit équivalent "
            f"(\"{matched}\") mais il est en {stock_stat.lower()} ({stock_info}). "
            f"Action prioritaire : **réassortir ce produit immédiatement** — la demande est là. "
            f"Potentiel business : {potential}."
        )

    elif statut == "Proche":
        comment = (
            f"{product} est en croissance ({sources_desc}). "
            f"Votre produit \"{matched}\" couvre partiellement ce besoin (match partiel). "
            f"Envisagez un produit plus ciblé ou optimisez le SEO de votre référencement existant. "
            f"Potentiel : {potential}."
        )

    else:  # Disponible
        comment = (
            f"{product} est déjà disponible dans votre catalogue sous \"{matched}\" (en stock). "
            f"Vérifiez le référencement SEO et la mise en avant sur votre boutique EBM."
        )

    return comment


def generate_report(
    scored_df: pd.DataFrame,
    category_filter: Optional[str] = None,
) -> pd.DataFrame:
    """
    Génère le rapport final structuré.

    Colonnes de sortie :
    Produit tendance | Source | Score tendance | Catégorie | Mot-clé |
    Niveau de match | Produit équivalent (catalogue) | Stock produit proche |
    Statut | Recommandation | Commentaire justification
    """
    if scored_df.empty:
        return pd.DataFrame()

    df = scored_df.copy()
    df["commentaire"] = df.apply(_build_comment, axis=1)

    # Statut stock lisible
    def stock_label(row):
        s = str(row.get("matched_stock_status", ""))
        qty = float(row.get("matched_inventory_qty", 0))
        if not s or row.get("statut") == "Absent":
            return ""
        return f"{s} ({int(qty)} unités)"

    report_df = pd.DataFrame({
        "Produit tendance":             df["product"],
        "Source":                       df.get("source_combined", df["source"]),
        "Score tendance":               df["trend_score"].round(1),
        "Catégorie":                    df.get("category", ""),
        "Mot-clé":                      df.get("keyword", ""),
        "Niveau de match":              df.get("match_score", 0).round(1),
        "Produit équivalent (catalogue)": df.get("matched_title", ""),
        "Stock produit proche":         df.apply(stock_label, axis=1),
        "Statut":                       df.get("statut", ""),
        "Recommandation":               df.get("recommendation", ""),
        "Commentaire justification":    df["commentaire"],
    })

    # Tri : Rupture en premier, puis Absent, puis Proche, puis Disponible ; par score desc
    priority = {"Rupture (tendance)": 0, "Absent": 1, "Proche": 2, "Disponible": 3}
    report_df["_sort"] = report_df.apply(
        lambda r: (
            0 if r["Recommandation"] == "OUI" else 1,
            priority.get(r["Statut"], 9),
            -r["Score tendance"]
        ),
        axis=1,
    )
    report_df = report_df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)

    if category_filter and category_filter not in ("Tous", None):
        report_df = report_df[report_df["Catégorie"] == category_filter]

    print(f"[report_generator] {len(report_df)} lignes générées.")
    return report_df
