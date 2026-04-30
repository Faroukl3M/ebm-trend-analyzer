"""
catalogue_parser.py — Lecture et normalisation du catalogue Shopify EBM (Matrixify)

Format réel détecté sur Products.csv EBM :
- 46 colonnes Matrixify
- Colonnes stock : Total Inventory Qty, Variant Inventory Qty
- Status : Active / Archived / Draft / Unlisted
- Types FR : Shampoing, Huile, Après-shampoing, Crème Capillaire, Gel, Masque, etc.
- Plusieurs lignes par produit (une par variante) → dédoublonnage sur Handle
"""

import pandas as pd
import re
from config import STOPWORDS

# Mapping colonnes Matrixify → noms internes
COLUMN_ALIASES = {
    "id":                       "ID",
    "handle":                   "Handle",
    "title":                    "Title",
    "body html":                "Description",
    "vendor":                   "Vendor",
    "type":                     "Product Type",
    "tags":                     "Tags",
    "status":                   "Status",
    "total inventory qty":      "Total Inventory Qty",
    "variant inventory qty":    "Variant Inventory Qty",
    "variant sku":              "Variant SKU",
    "variant price":            "Variant Price",
    "image src":                "Image Src",
    "category":                 "Category",
    # aliases alternatifs
    "product type":             "Product Type",
    "description":              "Description",
}

# Types FR → catégorie EBM normalisée
TYPE_TO_CATEGORY = {
    "shampoing":            "Haircare",
    "après-shampoing":      "Haircare",
    "apres-shampoing":      "Haircare",
    "conditionneur":        "Haircare",
    "masque capillaire":    "Haircare",
    "crème capillaire":     "Haircare",
    "creme capillaire":     "Haircare",
    "gel capillaire":       "Haircare",
    "spray capillaire":     "Haircare",
    "leave-in":             "Haircare",
    "soin capillaire":      "Haircare",
    "beurre capillaire":    "Haircare",
    "gel":                  "Haircare",
    "brosse":               "Haircare",
    "peigne":               "Haircare",
    "peigne à cheveux":     "Haircare",
    "accessoire cheveux":   "Haircare",
    "bonnet":               "Haircare",
    "huile capillaire":     "Oils",
    "huile essentielle":    "Oils",
    "huile":                "Oils",
    "sérum":                "Skincare",
    "serum":                "Skincare",
    "crème":                "Skincare",
    "creme":                "Skincare",
    "masque visage":        "Skincare",
    "masque":               "Skincare",
    "soin visage":          "Skincare",
    "savon":                "Skincare",
    "gommage corporel":     "Skincare",
    "lotion corporelle":    "Skincare",
    "soin":                 "Skincare",
    "maquillage":           "Makeup",
    "fond de teint":        "Makeup",
    "coloration":           "Haircare",
    "perruque":             "Wigs",
    "wig":                  "Wigs",
    "complément":           "Supplements beauté",
    "supplement":           "Supplements beauté",
}


def normalize_title(title: str) -> str:
    """Normalise un titre produit : minuscules, suppression stopwords et ponctuation."""
    if not isinstance(title, str):
        return ""
    text = title.lower()
    # Traduction FR→EN basique pour matching
    fr_en = {
        "masque": "mask", "capillaire": "hair", "cheveux": "hair",
        "huile": "oil", "shampoing": "shampoo", "après-shampoing": "conditioner",
        "crème": "cream", "creme": "cream", "sérum": "serum", "soin": "care",
        "gel": "gel", "spray": "spray", "beurre": "butter",
        "hydratant": "moisturizing", "nourrissant": "nourishing",
        "réparateur": "repairing", "fortifiant": "strengthening",
        "bouclés": "curly", "naturels": "natural", "secs": "dry",
        "croissance": "growth", "pousse": "growth", "brillance": "shine",
        "karité": "shea", "ricin": "castor", "romarin": "rosemary",
        "argan": "argan", "coco": "coconut", "avocat": "avocado",
        "perruque": "wig", "dentelle": "lace", "savon": "soap",
        "visage": "face", "corps": "body",
    }
    for fr, en in fr_en.items():
        text = re.sub(r"\b" + re.escape(fr) + r"\b", en, text)
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = [t for t in text.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(tokens)


def infer_category_from_type(product_type: str) -> str:
    """Infère la catégorie EBM depuis le type produit FR."""
    if not isinstance(product_type, str):
        return "Haircare"
    key = product_type.lower().strip()
    return TYPE_TO_CATEGORY.get(key, "Haircare")


def parse_shopify_catalogue(file_obj) -> pd.DataFrame:
    """
    Lit un CSV/Excel Matrixify EBM et retourne un DataFrame normalisé.

    Colonnes garanties en sortie :
    Handle | Title | Vendor | Product Type | Tags | Variant SKU | Variant Price |
    Description | Status | Total Inventory Qty | stock_status |
    title_normalized | tags_list | type_normalized | category
    """
    # Lecture
    name = getattr(file_obj, "name", "")
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_obj, dtype=str)
    else:
        try:
            df = pd.read_csv(file_obj, dtype=str, sep=",")
            if df.shape[1] == 1:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, dtype=str, sep=";")
        except Exception:
            df = pd.read_csv(file_obj, dtype=str, sep=";")

    # Normalisation noms colonnes
    df.columns = [
        COLUMN_ALIASES.get(c.strip().lower(), c.strip()) for c in df.columns
    ]

    # Colonnes obligatoires manquantes
    for col in ["Handle", "Title", "Vendor", "Product Type", "Tags",
                "Variant SKU", "Variant Price", "Description", "Status",
                "Total Inventory Qty", "Variant Inventory Qty"]:
        if col not in df.columns:
            df[col] = ""

    df = df.fillna("")

    # ── STOCK ────────────────────────────────────────────────────────────────
    df["Total Inventory Qty"] = pd.to_numeric(
        df["Total Inventory Qty"], errors="coerce"
    ).fillna(0)
    df["Variant Inventory Qty"] = pd.to_numeric(
        df["Variant Inventory Qty"], errors="coerce"
    ).fillna(0)

    # Dédoublonnage : une ligne par produit (Handle), garder la première variante
    # Pour le stock total, on prend la valeur de Total Inventory Qty (déjà agrégée par Shopify)
    df_products = df.drop_duplicates(subset=["Handle"], keep="first").copy()

    # Filtre : uniquement les produits ACTIFS (pas Archived, pas Draft)
    df_active = df_products[df_products["Status"].str.lower() == "active"].copy()

    # Statut stock
    def stock_status(qty):
        if qty <= 0:
            return "Rupture"
        elif qty <= 3:
            return "Stock faible"
        else:
            return "En stock"

    df_active["stock_status"] = df_active["Total Inventory Qty"].apply(stock_status)

    # Colonnes dérivées pour matching
    df_active["title_normalized"] = df_active["Title"].apply(normalize_title)
    df_active["tags_list"] = df_active["Tags"].apply(
        lambda x: [t.strip().lower() for t in str(x).split(",") if t.strip()]
    )
    df_active["type_normalized"] = df_active["Product Type"].apply(
        lambda x: str(x).lower().strip()
    )
    df_active["category"] = df_active["Product Type"].apply(infer_category_from_type)

    print(f"[catalogue_parser] {len(df_active)} produits actifs chargés.")
    print(f"  En stock     : {(df_active['stock_status'] == 'En stock').sum()}")
    print(f"  Stock faible : {(df_active['stock_status'] == 'Stock faible').sum()}")
    print(f"  Rupture      : {(df_active['stock_status'] == 'Rupture').sum()}")
    print(f"  Archivés ignorés : {(df_products['Status'].str.lower() == 'archived').sum()}")

    return df_active[[
        "Handle", "Title", "Vendor", "Product Type", "Tags",
        "Variant SKU", "Variant Price", "Description", "Status",
        "Total Inventory Qty", "Variant Inventory Qty", "stock_status",
        "title_normalized", "tags_list", "type_normalized", "category",
    ]]
