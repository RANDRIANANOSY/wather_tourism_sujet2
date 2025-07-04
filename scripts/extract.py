import os
import pandas as pd
from datetime import datetime
import glob
import logging
from typing import List, Dict, Optional

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_transformation.log'),
        logging.StreamHandler()
    ]
)

# Configuration des chemins
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
OUTPUT_FILENAME = "donnees_meteo_consolidees.csv"

def load_raw_data() -> Optional[pd.DataFrame]:
    """Charge et combine tous les fichiers CSV bruts"""
    try:
        # Vérifie l'existence du dossier
        if not os.path.exists(RAW_DATA_DIR):
            logging.error(f"Le dossier {RAW_DATA_DIR} n'existe pas")
            return None
        
        # Liste tous les fichiers CSV dans le dossier raw
        csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "donnees_meteo_*.csv"))
        
        if not csv_files:
            logging.warning(f"Aucun fichier trouvé dans {RAW_DATA_DIR}")
            return None
        
        # Charge et combine tous les fichiers
        dfs = []
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                dfs.append(df)
                logging.info(f"Chargement réussi: {os.path.basename(file)} ({len(df)} enregistrements)")
            except Exception as e:
                logging.warning(f"Erreur lors du chargement de {file}: {str(e)}")
                continue
        
        if not dfs:
            logging.error("Aucun fichier valide chargé")
            return None
            
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données: {str(e)}")
        return None

def clean_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Nettoie et transforme les données"""
    try:
        # Conversion des dates/heures
        if 'date' in df.columns and 'heure' in df.columns:
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['heure'])
            df = df.drop(['date', 'heure'], axis=1)
        
        # Vérification des valeurs manquantes
        missing_values = df.isnull().sum()
        if missing_values.any():
            logging.warning(f"Valeurs manquantes détectées:\n{missing_values[missing_values > 0]}")
        
        # Conversion des types de données
        numeric_cols = ['temperature_c', 'temperature_ressentie_c', 'humidite_pourcent', 
                       'pression_hpa', 'vent_vitesse_kmh', 'couverture_nuageuse_pourcent']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Gestion de la visibilité
        if 'visibilite_km' in df.columns:
            df['visibilite_km'] = pd.to_numeric(df['visibilite_km'], errors='coerce')
            df['visibilite_km'].fillna('N/A', inplace=True)
        
        # Suppression des doublons
        initial_count = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_count:
            logging.info(f"Suppression de {initial_count - len(df)} doublons")
        
        # Tri par date
        if 'datetime' in df.columns:
            df = df.sort_values('datetime')
        
        return df
    
    except Exception as e:
        logging.error(f"Erreur lors du nettoyage des données: {str(e)}")
        return None

def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule des métriques supplémentaires"""
    try:
        if 'datetime' in df.columns:
            # Extraction des composantes temporelles
            df['annee'] = df['datetime'].dt.year
            df['mois'] = df['datetime'].dt.month
            df['jour'] = df['datetime'].dt.day
            df['heure_num'] = df['datetime'].dt.hour
        
        # Calcul de l'indice de confort (simplifié)
        if all(col in df.columns for col in ['temperature_c', 'humidite_pourcent']):
            df['indice_confort'] = df.apply(
                lambda x: 'Confortable' if (18 <= x['temperature_c'] <= 24 and 30 <= x['humidite_pourcent'] <= 70) 
                else 'Inconfortable', axis=1)
        
        return df
    except Exception as e:
        logging.warning(f"Erreur lors du calcul des métriques: {str(e)}")
        return df

def save_processed_data(df: pd.DataFrame) -> bool:
    """Sauvegarde les données transformées"""
    try:
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        output_path = os.path.join(PROCESSED_DATA_DIR, OUTPUT_FILENAME)
        
        # Sauvegarde en CSV
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # Sauvegarde en Parquet (optionnel)
        parquet_path = os.path.join(PROCESSED_DATA_DIR, "donnees_meteo_consolidees.parquet")
        df.to_parquet(parquet_path, index=False)
        
        logging.info(f"Données sauvegardées avec succès dans:\n- {output_path}\n- {parquet_path}")
        logging.info(f"Nombre total d'enregistrements: {len(df)}")
        logging.info(f"Période couverte: {df['datetime'].min()} à {df['datetime'].max()}")
        
        return True
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde: {str(e)}")
        return False

def main():
    """Point d'entrée principal"""
    logging.info("Début du traitement des données météo")
    
    # 1. Chargement des données brutes
    raw_df = load_raw_data()
    if raw_df is None:
        logging.error("Arrêt du traitement - données brutes non disponibles")
        return False
    
    # 2. Nettoyage des données
    cleaned_df = clean_data(raw_df)
    if cleaned_df is None:
        logging.error("Arrêt du traitement - échec du nettoyage")
        return False
    
    # 3. Calcul des métriques
    enriched_df = calculate_metrics(cleaned_df)
    
    # 4. Sauvegarde des résultats
    success = save_processed_data(enriched_df)
    
    if success:
        logging.info("Traitement terminé avec succès!")
    else:
        logging.error("Erreur lors du traitement")
    
    return success

if __name__ == "__main__":
    main()