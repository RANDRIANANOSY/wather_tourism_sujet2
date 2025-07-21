
# 🌤️ Projet Climat et Tourisme - Documentation Complète

## 📋 Vue d'ensemble du projet

### Problématique
Peut-on recommander les meilleures périodes pour visiter une ville selon des critères météorologiques ?

### Objectif
Développer un système automatisé d'analyse climatique pour fournir des recommandations touristiques basées sur des données météorologiques historiques et temps réel.

### Critères d'analyse
- **Température idéale** : Entre 22°C et 28°C
- **Précipitations** : Moins de 50mm par mois
- **Vent** : Moins de 20 km/h
- **Ensoleillement** : Plus de 20 jours par mois

---

## 🏗️ Architecture du projet

### 1. Pipeline ETL avec Apache Airflow

```
[API OpenWeather] → [Extract] → [Clean] → [Merge] → [Analyze] → [Dashboard]
                      ↓         ↓         ↓          ↓
                   [Logs]   [Validate] [Historic] [Reports]
```

#### Tâches du DAG :
1. **extract_weather_data** : Extraction quotidienne des données météo
2. **clean_and_validate_data** : Nettoyage et validation des données
3. **merge_with_historical_data** : Fusion avec l'historique
4. **generate_tourism_recommendations** : Génération des recommandations
5. **send_quality_report** : Rapport de qualité
6. **cleanup_temp_files** : Nettoyage des fichiers temporaires

### 2. Modèle de données (Étoile)

```
                    [Fait_Météo]
                         |
              ┌──────────┼──────────┐
              |          |          |
        [Dim_Ville] [Dim_Temps] [Dim_Météo]
```

#### Tables principales :
- **Fait_Météo** : Mesures quotidiennes par ville
- **Dim_Ville** : Informations sur les villes analysées
- **Dim_Temps** : Dimensions temporelles (jour, mois, saison)
- **Dim_Météo** : Catégories et seuils météorologiques

---

## 📊 Analyse exploratoire des données (EDA)

### Résultats principaux

#### Classement des villes par score météorologique moyen :
1. **Madrid** : 73.0/100 - Climat méditerranéen idéal
2. **Barcelona** : 72.3/100 - Températures agréables toute l'année
3. **Rome** : 70.7/100 - Étés chauds, hivers doux
4. **London** : 67.2/100 - Climat tempéré avec pluies fréquentes
5. **Vienna** : 66.8/100 - Continental avec saisons marquées
6. **Berlin** : 66.2/100 - Continental modéré
7. **Paris** : 63.9/100 - Océanique avec variations saisonnières
8. **Amsterdam** : 63.1/100 - Maritime avec humidité élevée

#### Meilleures périodes générales :
- **Juin à septembre** : Scores les plus élevés (> 70/100)
- **Mai et octobre** : Périodes de transition favorables
- **Décembre à mars** : Moins favorables pour le tourisme

#### Insights météorologiques :
- **Température optimale** atteinte principalement entre juin et août
- **Précipitations** généralement plus faibles en été
- **Variabilité saisonnière** plus marquée pour les villes continentales

---

## 🛠️ Installation et configuration

### Prérequis
```bash
# Versions requises
Python >= 3.8
Apache Airflow >= 2.5.0
Pandas >= 1.5.0
```

### Installation d'Apache Airflow
```bash
# Installation avec pip
pip install apache-airflow[postgres,google,pandas]==2.5.0

# Initialisation de la base de données
airflow db init

# Création d'un utilisateur admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Configuration du DAG
1. **Placer le fichier** `climate_tourism_dag.py` dans le dossier `dags/`
2. **Configurer l'API OpenWeather** :
   ```python
   OPENWEATHER_API_KEY = "votre_clé_api_openweather"
   ```
3. **Créer les répertoires** :
   ```bash
   mkdir -p /opt/airflow/data
   chmod 755 /opt/airflow/data
   ```

### Lancement d'Airflow
```bash
# Démarrage du scheduler
airflow scheduler

# Démarrage du webserver
airflow webserver --port 8080
```

---

## 📈 Utilisation du système

### 1. Monitoring du DAG
- **Interface web** : http://localhost:8080
- **Logs** : Consultables dans l'interface Airflow
- **Alertes** : Configurées pour les échecs de tâches

### 2. Consultation des résultats
- **Données historiques** : `/opt/airflow/data/climate_tourism_historical.csv`
- **Recommandations** : `/opt/airflow/data/tourism_recommendations_YYYYMMDD.json`
- **Rapports qualité** : `/opt/airflow/data/quality_report_YYYYMMDD_HHMMSS.json`

### 3. Dashboard interactif
Fichiers générés compatibles avec :
- **Power BI** : Import direct des fichiers CSV
- **Tableau Public** : Connexion aux données
- **Looker Studio** : Upload des datasets
- **Python/Streamlit** : Dashboard personnalisé

---

## 🔧 Maintenance et troubleshooting

### Problèmes courants

#### DAG ne s'exécute pas
```bash
# Vérifier le statut d'Airflow
airflow dags list

# Tester une tâche manuellement
airflow tasks test climate_tourism_etl extract_weather_data 2024-01-01
```

#### Erreurs d'API
- Vérifier la clé API OpenWeather
- Contrôler les limites de taux d'appel
- Valider la connectivité réseau

#### Problèmes de qualité des données
- Consulter le rapport de qualité quotidien
- Vérifier les logs de validation
- Analyser les métriques de nettoyage

### Optimisations possibles
1. **Cache des données** : Mise en cache des appels API fréquents
2. **Parallélisation** : Extraction simultanée pour plusieurs villes
3. **Compression** : Archivage des données historiques anciennes
4. **Monitoring avancé** : Intégration avec Grafana/Prometheus

---

## 📊 Modèle de données détaillé

### Schema Star (Étoile)

```sql
-- Table de faits principale
CREATE TABLE fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(id),
    date_id INTEGER REFERENCES dim_date(id),
    temperature_avg DECIMAL(5,2),
    precipitation_mm DECIMAL(6,2),
    wind_speed_kmh DECIMAL(5,2),
    humidity_percent DECIMAL(5,2),
    sunny_days INTEGER,
    tourism_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Ville
CREATE TABLE dim_city (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    timezone VARCHAR(50),
    climate_zone VARCHAR(50)
);

-- Dimension Temps
CREATE TABLE dim_date (
    id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    season VARCHAR(20),
    is_weekend BOOLEAN,
    day_of_week INTEGER
);

-- Dimension Météo (catégories)
CREATE TABLE dim_weather_category (
    id SERIAL PRIMARY KEY,
    score_range VARCHAR(20),
    category_name VARCHAR(50),
    description TEXT,
    color_code VARCHAR(10),
    recommendation TEXT
);
```

---

## 📱 Dashboard et visualisations

### Indicateurs clés (KPI)
1. **Score moyen mensuel** par ville
2. **Tendances saisonnières** sur 5 ans
3. **Comparaisons inter-villes** par période
4. **Alertes météorologiques** temps réel

### Types de visualisations
- **Heatmaps** : Scores par ville/mois
- **Cartes géographiques** : Conditions actuelles
- **Séries temporelles** : Évolutions historiques
- **Radar charts** : Comparaisons multi-critères

### Filtres interactifs
- Sélection de villes multiples
- Plages de dates personnalisées
- Critères météorologiques spécifiques
- Niveaux de score minimum

---

## 🔒 Sécurité et bonnes pratiques

### Sécurité des données
- **Chiffrement** : Données sensibles chiffrées au repos
- **API Keys** : Stockées dans des variables d'environnement
- **Logs** : Pas d'informations sensibles dans les logs
- **Accès** : Contrôle d'accès basé sur les rôles Airflow

### Bonnes pratiques
- **Idempotence** : Tâches réexécutables sans effet de bord
- **Monitoring** : Supervision continue des pipelines
- **Tests** : Tests unitaires pour les fonctions critiques
- **Documentation** : Code commenté et documenté

---

## 📈 Évolutions futures

### Fonctionnalités à développer
1. **Machine Learning** : Prédictions météorologiques à 7-14 jours
2. **Intégration événements** : Festivals, événements locaux
3. **Recommandations personnalisées** : Basées sur les préférences utilisateur
4. **API REST** : Accès programmatique aux recommandations
5. **Notifications** : Alertes sur les meilleures opportunités

### Améliorations techniques
1. **Kubernetes** : Déploiement containerisé
2. **Data Lake** : Stockage big data avec Delta Lake
3. **Real-time** : Streaming avec Apache Kafka
4. **Multi-cloud** : Déploiement sur AWS/GCP/Azure

---

## 🤝 Contribution

### Structure du projet
```
climate-tourism/
├── dags/
│   └── climate_tourism_dag.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── outputs/
├── notebooks/
│   └── climate_tourism_eda.ipynb
├── src/
│   ├── extractors/
│   ├── transformers/
│   └── analyzers/
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
│   └── README.md
└── requirements.txt
```

### Comment contribuer
1. **Fork** du repository
2. **Création** d'une branche feature
3. **Tests** des modifications
4. **Pull request** avec description détaillée

---

## 📞 Support et contact

### Ressources
- **Documentation Airflow** : https://airflow.apache.org/docs/
- **API OpenWeather** : https://openweathermap.org/api
- **Pandas Documentation** : https://pandas.pydata.org/docs/

### Équipe projet
- **Data Engineering** : Configuration pipeline Airflow
- **Data Science** : Développement des modèles d'analyse
- **DevOps** : Déploiement et monitoring
- **Product** : Définition des indicateurs métier

---

*Documentation mise à jour le : 2024*
*Version : 1.0*
