#!/usr/bin/env python3
"""
Script de chargement des données météo transformées dans une base de données
"""
import os
import sqlite3
import pandas as pd
from typing import Optional
import logging
from datetime import datetime
from transform import WeatherTransformer  # Import du transformateur

# Configuration
DATA_DIR = "data"
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
DB_PATH = os.path.join(DATA_DIR, "weather_db.sqlite")
TABLE_NAMES = {
    'clean_data': 'weather_clean',
    'scores': 'weather_monthly_scores'
}

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherLoader:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.transformer = WeatherTransformer()  # Réutilisation du transformateur
        
    def connect_db(self) -> bool:
        """Établit une connexion à la base de données"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connexion à la base de données établie: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Erreur de connexion à la base: {str(e)}")
            return False
    
    def init_db(self) -> bool:
        """Initialise la structure de la base de données"""
        if not self.conn:
            return False
            
        try:
            cursor = self.conn.cursor()
            
            # Table pour les données nettoyées
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES['clean_data']} (
                ville TEXT,
                pays TEXT,
                date DATE,
                temperature REAL,
                humidite REAL,
                pression REAL,
                vent_vitesse REAL,
                mois INTEGER,
                annee INTEGER,
                PRIMARY KEY (ville, date)
            )""")
            
            # Table pour les scores mensuels
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES['scores']} (
                ville TEXT,
                mois INTEGER,
                temp_moyenne REAL,
                humidite_moyenne REAL,
                vent_moyen REAL,
                score_moyen REAL,
                nb_jours INTEGER,
                PRIMARY KEY (ville, mois))
            """)
            
            self.conn.commit()
            logger.info("Structure de la base initialisée")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Erreur d'initialisation: {str(e)}")
            return False
    
    def load_from_files(self, date_str: str = None) -> bool:
        """
        Charge les données depuis les fichiers transformés
        Si date_str est None, utilise la date actuelle
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        
        try:
            # Charger les données transformées
            clean_path = os.path.join(PROCESSED_DIR, "weather_cleaned.csv")
            scores_path = os.path.join(PROCESSED_DIR, "weather_scores.csv")
            
            df_clean = pd.read_csv(clean_path)
            df_scores = pd.read_csv(scores_path)
            
            return self._load_dataframes(df_clean, df_scores)
            
        except FileNotFoundError as e:
            logger.error(f"Fichier non trouvé: {str(e)}")
            return False
        except pd.errors.EmptyDataError:
            logger.error("Fichier de données vide")
            return False
    
    def load_from_transformer(self) -> bool:
        """
        Charge les données directement depuis le transformateur
        (en mémoire sans passer par les fichiers)
        """
        _, monthly_scores = self.transformer.transform()
        return self._load_dataframes(None, monthly_scores)
    
    def _load_dataframes(self, 
                       df_clean: Optional[pd.DataFrame], 
                       df_scores: pd.DataFrame) -> bool:
        """
        Méthode interne pour charger des DataFrames dans la base
        """
        if not self.connect_db():
            return False
            
        try:
            cursor = self.conn.cursor()
            
            # Chargement des données nettoyées
            if df_clean is not None:
                df_clean.to_sql(
                    TABLE_NAMES['clean_data'],
                    self.conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                logger.info(f"{len(df_clean)} enregistrements chargés dans {TABLE_NAMES['clean_data']}")
            
            # Chargement des scores mensuels (toujours mis à jour)
            cursor.execute(f"DELETE FROM {TABLE_NAMES['scores']}")
            
            df_scores.to_sql(
                TABLE_NAMES['scores'],
                self.conn,
                if_exists='append',
                index=False
            )
            logger.info(f"{len(df_scores)} scores mensuels chargés dans {TABLE_NAMES['scores']}")
            
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Erreur de chargement: {str(e)}")
            self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None
    
    def get_best_months(self, city: str, limit: int = 3) -> Optional[pd.DataFrame]:
        """
        Récupère les meilleurs mois pour visiter une ville
        selon le score météo moyen
        """
        if not self.connect_db():
            return None
            
        try:
            query = f"""
            SELECT ville, mois, score_moyen 
            FROM {TABLE_NAMES['scores']}
            WHERE ville = ?
            ORDER BY score_moyen DESC
            LIMIT ?
            """
            
            df = pd.read_sql(query, self.conn, params=(city, limit))
            return df
            
        except sqlite3.Error as e:
            logger.error(f"Erreur de requête: {str(e)}")
            return None
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

def main():
    """Point d'entrée principal"""
    loader = WeatherLoader()
    
    # 1. Initialisation de la base
    if not loader.connect_db() or not loader.init_db():
        return
    
    # 2. Chargement des données (depuis les fichiers)
    success = loader.load_from_files()
    
    # Alternative: Chargement direct depuis le transformateur
    # success = loader.load_from_transformer()
    
    if success:
        # 3. Exemple de requête
        print("\nMeilleurs mois pour visiter Paris:")
        paris_months = loader.get_best_months("Paris")
        if paris_months is not None:
            print(paris_months.to_string(index=False))
    
    logger.info("Chargement terminé")

if __name__ == "__main__":
    main()