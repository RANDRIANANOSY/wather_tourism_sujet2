#!/usr/bin/env python3
"""
Script de transformation des données météo brutes en données analysables
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging
import numpy as np

# Configuration
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
CITIES = ["Paris", "London", "Tokyo", "New York", "Berlin"]

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherTransformer:
    def __init__(self):
        self.weather_data = None
        self.weather_scores = None
        
    def load_raw_data(self, data_type: str, date_str: str = None) -> Optional[Dict]:
        """Charge les données brutes depuis les fichiers JSON"""
        try:
            if date_str is None:
                date_str = datetime.now().strftime("%Y%m%d")
                
            file_path = os.path.join(RAW_DIR, data_type, f"{data_type}_weather_{date_str}.json")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            logger.info(f"Données {data_type} chargées: {file_path}")
            return data
            
        except FileNotFoundError:
            logger.error(f"Fichier non trouvé: {file_path}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Erreur de lecture du JSON: {file_path}")
            return None
    
    def merge_datasets(self, historical: List[Dict], realtime: List[Dict]) -> pd.DataFrame:
        """Fusionne les données historiques et temps réel"""
        # Conversion en DataFrames
        df_hist = pd.DataFrame(historical)
        df_realtime = pd.DataFrame(realtime)
        
        # Standardisation des colonnes
        common_cols = ['ville', 'pays', 'date', 'temperature', 'humidite', 'pression', 'vent_vitesse']
        
        # Filtrage des colonnes disponibles
        df_hist = df_hist[[col for col in common_cols if col in df_hist.columns]]
        df_realtime = df_realtime[[col for col in common_cols if col in df_realtime.columns]]
        
        # Fusion
        merged_df = pd.concat([df_hist, df_realtime], ignore_index=True)
        
        # Conversion des types
        numeric_cols = ['temperature', 'humidite', 'pression', 'vent_vitesse']
        for col in numeric_cols:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
        
        # Conversion de la date
        if 'date' in merged_df.columns:
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            merged_df['mois'] = merged_df['date'].dt.month
            merged_df['annee'] = merged_df['date'].dt.year
        
        logger.info(f"Données fusionnées: {len(merged_df)} enregistrements")
        return merged_df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoyage des données météo"""
        # Suppression des doublons
        df = df.drop_duplicates(subset=['ville', 'date'], keep='last')
        
        # Gestion des valeurs manquantes
        numeric_cols = ['temperature', 'humidite', 'pression', 'vent_vitesse']
        for col in numeric_cols:
            if col in df.columns:
                # Remplacement par la moyenne par ville/mois
                df[col] = df.groupby(['ville', 'mois'])[col].transform(
                    lambda x: x.fillna(x.mean())
                )
        
        # Filtrage des villes cibles
        if 'ville' in df.columns:
            df = df[df['ville'].isin(CITIES)]
        
        logger.info("Données nettoyées avec succès")
        return df
    
    def calculate_weather_score(self, row: Dict) -> float:
        """Calcule un score météo composite (0-100)"""
        # Poids des différents facteurs
        weights = {
            'temperature': 0.4,
            'humidite': 0.2,
            'vent_vitesse': 0.2,
            'pression': 0.1,
            'precipitation': 0.1  # Ajouté plus tard si disponible
        }
        
        # Score de température (idéal entre 20 et 25°C)
        temp = row.get('temperature', 0)
        temp_score = 100 - 4 * abs(temp - 22.5)  # Meilleur score à 22.5°C
        
        # Score d'humidité (idéal entre 40% et 60%)
        humidity = row.get('humidite', 0)
        humidity_score = 100 - abs(humidity - 50) / 0.5
        
        # Score de vent (moins c'est mieux)
        wind = row.get('vent_vitesse', 0)
        wind_score = max(0, 100 - wind * 10)
        
        # Score composite
        total_score = (
            weights['temperature'] * temp_score +
            weights['humidite'] * humidity_score +
            weights['vent_vitesse'] * wind_score
        )
        
        # Normalisation entre 0 et 100
        return max(0, min(100, total_score))
    
    def calculate_monthly_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les scores météo moyens par ville et par mois"""
        if df.empty:
            return pd.DataFrame()
        
        # Application du score à chaque ligne
        df['score_meteo'] = df.apply(self.calculate_weather_score, axis=1)
        
        # Agrégation par ville et mois
        monthly_scores = df.groupby(['ville', 'mois']).agg({
            'temperature': 'mean',
            'humidite': 'mean',
            'vent_vitesse': 'mean',
            'score_meteo': ['mean', 'count']
        }).reset_index()
        
        # Aplatissement des colonnes multi-index
        monthly_scores.columns = [
            'ville', 'mois', 'temp_moyenne', 'humidite_moyenne',
            'vent_moyen', 'score_moyen', 'nb_jours'
        ]
        
        # Arrondissement des valeurs
        for col in ['temp_moyenne', 'humidite_moyenne', 'vent_moyen', 'score_moyen']:
            monthly_scores[col] = monthly_scores[col].round(1)
        
        logger.info("Scores mensuels calculés")
        return monthly_scores
    
    def save_processed_data(self, df: pd.DataFrame, filename: str):
        """Sauvegarde les données transformées"""
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        
        # Sauvegarde en CSV
        csv_path = os.path.join(PROCESSED_DIR, f"{filename}.csv")
        df.to_csv(csv_path, index=False)
        
        # Sauvegarde en JSON
        json_path = os.path.join(PROCESSED_DIR, f"{filename}.json")
        df.to_json(json_path, orient='records', indent=2)
        
        logger.info(f"Données sauvegardées dans {PROCESSED_DIR}/{filename}")
    
    def transform(self, date_str: str = None):
        """Pipeline complet de transformation"""
        # 1. Chargement des données
        historical = self.load_raw_data("historical", date_str) or []
        realtime = self.load_raw_data("realtime", date_str) or []
        
        # 2. Fusion et nettoyage
        merged_df = self.merge_datasets(historical, realtime)
        cleaned_df = self.clean_data(merged_df)
        
        # 3. Calcul des scores
        monthly_scores = self.calculate_monthly_scores(cleaned_df)
        
        # 4. Sauvegarde
        if not cleaned_df.empty:
            self.save_processed_data(cleaned_df, "weather_cleaned")
        if not monthly_scores.empty:
            self.save_processed_data(monthly_scores, "weather_scores")
        
        # Stockage pour utilisation ultérieure
        self.weather_data = cleaned_df
        self.weather_scores = monthly_scores
        
        return cleaned_df, monthly_scores

def main():
    """Point d'entrée principal"""
    transformer = WeatherTransformer()
    weather_data, weather_scores = transformer.transform()
    
    if weather_scores is not None:
        print("\nScores météo mensuels moyens:")
        print(weather_scores.to_string(index=False))
    
    logger.info("Transformation terminée avec succès!")

if __name__ == "__main__":
    main()