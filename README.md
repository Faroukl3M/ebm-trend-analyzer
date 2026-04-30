# 💄 EBM Trend Analyzer

[![CI](https://github.com/VOTRE_USERNAME/ebm-trend-analyzer/actions/workflows/ci.yml/badge.svg)](https://github.com/VOTRE_USERNAME/ebm-trend-analyzer/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32+-red.svg)](https://streamlit.io)

> Outil semi-automatique d'analyse des tendances marché pour **Ethnic Beauty Market (EBM)**.
> Compare votre catalogue Shopify aux produits tendance (Google Trends + Amazon RSS)
> et identifie les opportunités manquantes.

---

## Deployer en 1 clic sur Streamlit Cloud

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/deploy?repository=VOTRE_USERNAME/ebm-trend-analyzer&branch=main&mainModule=app.py)

> Aucun terminal requis. L'app tourne dans le cloud, accessible depuis n'importe quel navigateur.

### Etapes de deploiement

1. **Forkez ce depot** : bouton `Fork` en haut a droite sur GitHub
2. Allez sur **[share.streamlit.io](https://share.streamlit.io)**
3. Connectez votre compte GitHub
4. Cliquez **"New app"** > selectionnez votre fork > branche `main` > fichier `app.py`
5. Cliquez **"Deploy"** : l'app est en ligne en 2 minutes

---

## Fonctionnalites

| Fonctionnalite | Detail |
|---|---|
| Import catalogue | CSV/Excel Matrixify — detection automatique des colonnes |
| Gestion du stock | En stock / Stock faible / Rupture |
| Google Trends | Requetes en forte croissance via PyTrends |
| Amazon RSS | Best Sellers + Movers & Shakers (flux officiels, legal) |
| Matching FR/EN | RapidFuzz + dictionnaire bilingue + tags Shopify |
| Scoring pondere | Google 40% · TikTok 35% · Amazon 25% (ajustable) |
| Justification | Commentaire automatique par opportunite |
| Export | CSV + Excel formate |

---

## Architecture

```
ebm-trend-analyzer/
├── app.py                  # Interface Streamlit
├── config.py               # Mots-cles, poids, seuils
├── catalogue_parser.py     # Lecture Matrixify + stock
├── trend_fetcher.py        # Google Trends + Amazon RSS
├── matcher.py              # Matching fuzzy FR/EN
├── scoring_engine.py       # Score 0-100
├── report_generator.py     # Rapport + commentaires
├── requirements.txt        # Dependances
├── sample_catalogue.csv    # Exemple de test
├── .streamlit/config.toml  # Theme Streamlit
└── .github/workflows/ci.yml
```

---

## Installation locale

```bash
git clone https://github.com/VOTRE_USERNAME/ebm-trend-analyzer.git
cd ebm-trend-analyzer
pip install -r requirements.txt
streamlit run app.py
```

---

## Statuts de matching

| Statut | Logique | Action |
|--------|---------|--------|
| Disponible | Match >= 75% + en stock | Optimiser SEO |
| Proche | Match 40-74% + en stock | Enrichir la gamme |
| Rupture (tendance) | Match trouve MAIS stock = 0 | Reassortir immediatement |
| Absent | Aucun match < 40% | Sourcer le produit |

---

## Configuration

Modifiez `config.py` pour personnaliser :
- Mots-cles par categorie (Haircare, Skincare, Wigs, Oils...)
- Pays Google Trends : `PYTRENDS_GEO = "FR"`
- Seuils de matching
- Poids des sources
