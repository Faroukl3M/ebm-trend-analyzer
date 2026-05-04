"""
EBM Trend Analyzer — Ethnic Beauty Market
Application Streamlit principale — v2.0
Compatible : Python 3.14 · Pandas 2.x · RapidFuzz 3.x
"""

import streamlit as st
import pandas as pd

from catalogue_parser import parse_shopify_catalogue
from trend_fetcher import fetch_all_trends
from matcher import match_trends_to_catalogue
from scoring_engine import compute_scores
from report_generator import generate_report
import config

st.set_page_config(
    page_title="EBM Trend Analyzer",
    page_icon="💄",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card{background:#f8f5ff;border-radius:10px;padding:1rem;text-align:center;}
.badge-absent{background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:12px;font-size:12px;}
.badge-rupture{background:#fef9c3;color:#713f12;padding:2px 8px;border-radius:12px;font-size:12px;}
.badge-proche{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:12px;}
.badge-present{background:#d1fae5;color:#065f46;padding:2px 8px;border-radius:12px;font-size:12px;}
.badge-oui{background:#ede9fe;color:#4c1d95;padding:2px 8px;border-radius:12px;font-weight:600;font-size:12px;}
</style>
""", unsafe_allow_html=True)

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("💄 EBM Trend Analyzer")
    st.caption("Ethnic Beauty Market · v2.0")
    st.divider()

    st.subheader("⚙️ Configuration")
    selected_category = st.selectbox(
        "Univers produit",
        ["Tous"] + config.CATEGORIES,
    )
    amazon_marketplace = st.radio(
        "Marketplace Amazon RSS",
        ["fr", "com"],
        horizontal=True,
        help="amazon.fr (France) ou amazon.com (USA)"
    )

    st.subheader("📊 Pondération scores")
    w_google = st.slider("Google Trends (%)", 10, 60, config.WEIGHT_GOOGLE, 5)
    w_tiktok = st.slider("TikTok (%)", 10, 60, config.WEIGHT_TIKTOK, 5)
    w_amazon = st.slider("Amazon (%)", 10, 60, config.WEIGHT_AMAZON, 5)
    total = w_google + w_tiktok + w_amazon
    if total != 100:
        st.warning(f"Total = {total}% (doit être 100%)")

    st.subheader("🎵 TikTok Scraping")
    enable_tiktok = st.toggle(
        "Activer le scraping TikTok",
        value=False,
        help="Utilise TikTokApi (non-officiel). Nécessite Playwright. Fallback automatique si ça échoue."
    )
    if enable_tiktok:
        st.warning("⚠️ Non-officiel — usage interne uniquement.")
        tiktok_hashtags_input = st.text_input(
            "Hashtags (séparés par virgule)",
            value="hairtok,naturalhair,skincare,wiginstall,tiktokmademebuyit",
        )
        custom_hashtags = [h.strip().lstrip("#") for h in tiktok_hashtags_input.split(",") if h.strip()]
    else:
        custom_hashtags = None

    st.subheader("🎯 Filtres résultats")
    min_score = st.slider("Score tendance minimum", 0, 100, 30)
    show_only_absent = st.checkbox("Absents uniquement", value=False)
    show_only_recommended = st.checkbox("Recommandés uniquement", value=False)
    show_rupture = st.checkbox("Inclure produits en rupture de stock", value=True,
                               help="Produits présents mais stock = 0 → opportunité de réassort")

    st.divider()
    st.caption("v2.1 · © EBM 2025")

# ── MAIN ──────────────────────────────────────────────────────────────────────
st.title("💄 EBM — Trend Gap Analyzer")
st.markdown("Identifiez les **produits manquants** dans votre catalogue face aux tendances du marché.")

# STEP 1 — Upload
st.header("1. Déposez votre catalogue Matrixify")
col1, col2 = st.columns([2, 1])
with col1:
    uploaded_file = st.file_uploader(
        "Fichier CSV ou Excel Matrixify (Products.csv)",
        type=["csv", "xlsx", "xls"],
    )
with col2:
    st.info("**Format détecté EBM**\n\nHandle · Title · Vendor · Type\nTags · Status\n**Total Inventory Qty** ✓\nVariant Inventory Qty ✓")

# STEP 2 — Sources manuelles
st.header("2. Sources manuelles (optionnel)")
with st.expander("📥 Compléter avec Amazon / TikTok manuellement", expanded=False):
    col_a, col_t = st.columns(2)
    with col_a:
        st.markdown("**Amazon Best Sellers** — 1 produit par ligne")
        amazon_manual = st.text_area(
            "Produits Amazon",
            placeholder="Rosemary Hair Growth Oil\nCastor Oil for Hair\n...",
            height=140,
        )
    with col_t:
        st.markdown("**TikTok Trending** — 1 produit par ligne")
        tiktok_manual = st.text_area(
            "Produits TikTok",
            placeholder="Rice Water Hair Rinse\nEdge Control Gel\n...",
            height=140,
        )

# STEP 3 — Lancer
st.header("3. Lancer l'analyse")
analyse_btn = st.button("🔍 Analyser les tendances", type="primary", disabled=(uploaded_file is None))

if uploaded_file is None:
    st.info("👆 Déposez votre fichier Products.csv Matrixify pour commencer.")
    st.stop()

# ── RUN ───────────────────────────────────────────────────────────────────────
if analyse_btn or "report_df" in st.session_state:
    if analyse_btn:
        weights = {"google": w_google, "tiktok": w_tiktok, "amazon": w_amazon}

        with st.spinner("📂 Lecture du catalogue Shopify EBM..."):
            catalogue_df = parse_shopify_catalogue(uploaded_file)
            st.session_state["catalogue_df"] = catalogue_df

        with st.spinner("🌐 Récupération des tendances (Google Trends + Amazon RSS)..."):
            amazon_list = [l.strip() for l in amazon_manual.strip().splitlines() if l.strip()] if amazon_manual else []
            tiktok_list = [l.strip() for l in tiktok_manual.strip().splitlines() if l.strip()] if tiktok_manual else []
            trends_df = fetch_all_trends(
                category=selected_category if selected_category != "Tous" else None,
                amazon_manual=amazon_list,
                tiktok_manual=tiktok_list,
                amazon_marketplace=amazon_marketplace,
                enable_tiktok_scraping=enable_tiktok,
                tiktok_hashtags=custom_hashtags,
            )
            st.session_state["trends_df"] = trends_df

        with st.spinner("🔗 Matching catalogue ↔ tendances..."):
            matched_df = match_trends_to_catalogue(trends_df, catalogue_df)

        with st.spinner("📊 Calcul scores & génération rapport..."):
            scored_df = compute_scores(matched_df, weights)
            report_df = generate_report(scored_df, selected_category)
            st.session_state["report_df"] = report_df

        st.success("✅ Analyse terminée !")

    report_df = st.session_state.get("report_df", pd.DataFrame())
    catalogue_df = st.session_state.get("catalogue_df", pd.DataFrame())

    if report_df.empty:
        st.warning("Aucun résultat généré.")
        st.stop()

    # ── CATALOGUE STATS ───────────────────────────────────────────────────────
    st.header("4. Résultats")

    with st.expander("📦 Aperçu catalogue EBM chargé", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Produits actifs", len(catalogue_df))
        c2.metric("En stock", int((catalogue_df["stock_status"] == "En stock").sum()))
        c3.metric("Stock faible (≤3)", int((catalogue_df["stock_status"] == "Stock faible").sum()))
        c4.metric("Rupture", int((catalogue_df["stock_status"] == "Rupture").sum()))

        # Stock par catégorie
        cat_stock = catalogue_df.groupby("category").agg(
            Produits=("Handle", "count"),
            Stock_total=("Total Inventory Qty", "sum"),
            En_rupture=("stock_status", lambda x: (x == "Rupture").sum())
        ).reset_index().rename(columns={"category": "Catégorie"})
        st.dataframe(cat_stock, use_container_width=True, hide_index=True)

    # ── MÉTRIQUES TENDANCES ────────────────────────────────────────────────────
    m1, m2, m3, m4, m5 = st.columns(5)
    absent = report_df[report_df["Statut"] == "Absent"]
    rupture_tendance = report_df[report_df["Statut"] == "Rupture (tendance)"] if "Rupture (tendance)" in report_df["Statut"].values else pd.DataFrame()
    recommended = report_df[report_df["Recommandation"] == "OUI"]

    m1.metric("Tendances détectées", len(report_df))
    m2.metric("Absents du catalogue", len(absent))
    m3.metric("En rupture & tendance", len(rupture_tendance))
    m4.metric("Opportunités recommandées", len(recommended))
    m5.metric("Score moyen", f"{report_df['Score tendance'].mean():.0f}/100")

    # ── FILTRES ────────────────────────────────────────────────────────────────
    filtered_df = report_df[report_df["Score tendance"] >= min_score].copy()
    if show_only_absent:
        filtered_df = filtered_df[filtered_df["Statut"].isin(["Absent", "Rupture (tendance)"])]
    if show_only_recommended:
        filtered_df = filtered_df[filtered_df["Recommandation"] == "OUI"]
    if not show_rupture:
        filtered_df = filtered_df[filtered_df["Statut"] != "Rupture (tendance)"]

    # ── TOP OPPORTUNITÉS ──────────────────────────────────────────────────────
    st.subheader(f"🏆 TOP Opportunités — {len(filtered_df)} produits")

    top = filtered_df[
        filtered_df["Statut"].isin(["Absent", "Rupture (tendance)"])
    ].sort_values("Score tendance", ascending=False).head(12)

    for _, row in top.iterrows():
        icon = "🔥" if row["Score tendance"] >= 70 else "📈"
        statut_badge = row["Statut"]
        with st.expander(f"{icon} {row['Produit tendance']} — Score {row['Score tendance']:.0f}/100  |  {statut_badge}"):
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"**Source :** {row['Source']}")
            c1.markdown(f"**Catégorie :** {row['Catégorie']}")
            c2.markdown(f"**Mot-clé :** `{row['Mot-clé']}`")
            c2.markdown(f"**Match catalogue :** {row['Niveau de match']}%")
            if row.get("Produit équivalent (catalogue)"):
                c2.markdown(f"**Produit proche :** {row['Produit équivalent (catalogue)']}")
            if row.get("Stock produit proche"):
                c3.markdown(f"**Stock produit proche :** {row['Stock produit proche']}")
            c3.markdown(f"**Statut :** {statut_badge}")
            c3.markdown(f"**Recommandation :** **{row['Recommandation']}**")
            st.markdown("---")
            st.markdown(f"📝 {row['Commentaire justification']}")

    # ── TABLEAU COMPLET ────────────────────────────────────────────────────────
    st.subheader("📋 Tableau complet")

    def color_statut(val):
        colors = {
            "Absent":              "background-color:#fee2e2;color:#991b1b",
            "Rupture (tendance)":  "background-color:#fef9c3;color:#713f12",
            "Proche":              "background-color:#fef3c7;color:#92400e",
            "Disponible":          "background-color:#d1fae5;color:#065f46",
        }
        return colors.get(val, "")

    def color_reco(val):
        return "background-color:#ede9fe;color:#4c1d95;font-weight:600" if val == "OUI" else ""

    try:
        styled = (
            filtered_df.style
            .map(color_statut, subset=["Statut"])
            .map(color_reco, subset=["Recommandation"])
        )
        st.dataframe(styled, use_container_width=True, height=500)
    except Exception:
        st.dataframe(filtered_df, use_container_width=True, height=500)

    # ── EXPORT ────────────────────────────────────────────────────────────────
    st.subheader("📥 Export")
    col_e1, col_e2 = st.columns(2)
    with col_e1:
        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Télécharger CSV", csv_data, "ebm_opportunites.csv", "text/csv")
    with col_e2:
        try:
            import io
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                filtered_df.to_excel(writer, index=False, sheet_name="Opportunités")
                ws = writer.sheets["Opportunités"]
                for col in ws.columns:
                    max_len = max(len(str(cell.value or "")) for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)
            st.download_button(
                "⬇️ Télécharger Excel",
                output.getvalue(),
                "ebm_opportunites.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.warning(f"Export Excel : {e}")
