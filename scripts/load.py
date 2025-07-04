import pandas as pd
import logging
from typing import Optional
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

class WeatherDataLoader:
    """
    Chargeur de données météo avec capacités d'analyse et de visualisation
    """
    
    def __init__(self):
        self.data = None
        self.loaded = False
        self.processed_dir = Path('data') / 'processed'
        self.output_dir = Path('outputs')
        
        # Configuration du logging
        self._configure_logging()
        
        # Création des dossiers nécessaires
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _configure_logging(self):
        """Configure le système de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('weather_loader.log'),
                logging.StreamHandler()
            ]
        )
        logging.getLogger('matplotlib').setLevel(logging.WARNING)

    def _validate_data(self) -> bool:
        """Valide la structure et le contenu des données chargées"""
        required_columns = {
            'ville', 'datetime', 'temperature_c', 
            'humidite_pourcent', 'conditions'
        }
        
        if not isinstance(self.data, pd.DataFrame):
            logging.error("Les données ne sont pas un DataFrame valide")
            return False
            
        if self.data.empty:
            logging.error("Le DataFrame chargé est vide")
            return False
            
        missing_cols = [col for col in required_columns if col not in self.data.columns]
        if missing_cols:
            logging.error(f"Colonnes requises manquantes : {missing_cols}")
            return False
            
        # Vérification des valeurs aberrantes
        if 'temperature_c' in self.data.columns:
            if (self.data['temperature_c'] < -50).any() or (self.data['temperature_c'] > 60).any():
                logging.warning("Valeurs de température suspectes détectées")
                
        return True

    def load_data(self, source_format: str = 'auto') -> bool:
        """
        Charge les données depuis le dossier processed
        Formats supportés : 'parquet', 'csv', 'auto'
        """
        try:
            # Détection automatique du format
            if source_format == 'auto':
                parquet_path = self.processed_dir / 'donnees_meteo_consolidees.parquet'
                csv_path = self.processed_dir / 'donnees_meteo_consolidees.csv'
                
                if parquet_path.exists():
                    source_format = 'parquet'
                elif csv_path.exists():
                    source_format = 'csv'
                else:
                    logging.error("Aucun fichier de données trouvé dans data/processed")
                    return False

            # Chargement selon le format
            filepath = self.processed_dir / f"donnees_meteo_consolidees.{'parquet' if source_format == 'parquet' else 'csv'}"
            
            if source_format == 'parquet':
                self.data = pd.read_parquet(filepath)
            else:  # CSV
                self.data = pd.read_csv(filepath, parse_dates=['datetime'])

            # Validation et post-traitement
            if self._validate_data():
                self._enhance_data()
                self.loaded = True
                logging.info(f"Données chargées avec succès. {len(self.data)} enregistrements.")
                return True
            return False

        except Exception as e:
            logging.error(f"Erreur de chargement : {str(e)}", exc_info=True)
            return False

    def _enhance_data(self):
        """Enrichit les données avec des métriques supplémentaires"""
        try:
            # Extraction des composantes temporelles
            self.data['annee'] = self.data['datetime'].dt.year
            self.data['mois'] = self.data['datetime'].dt.month
            self.data['jour'] = self.data['datetime'].dt.day
            self.data['heure'] = self.data['datetime'].dt.hour
            
            # Calcul de l'indice de chaleur
            if all(col in self.data.columns for col in ['temperature_c', 'vent_vitesse_kmh']):
                self.data['indice_chaleur'] = self.data.apply(
                    lambda x: 13.12 + 0.6215*x['temperature_c'] - 11.37*(x['vent_vitesse_kmh']**0.16) + 
                    0.3965*x['temperature_c']*(x['vent_vitesse_kmh']**0.16),
                    axis=1
                )
        except Exception as e:
            logging.warning(f"Erreur lors de l'enrichissement des données : {str(e)}")

    def get_city_data(self, city_name: str, save_csv: bool = False) -> Optional[pd.DataFrame]:
        """Récupère les données pour une ville spécifique"""
        if not self.loaded:
            logging.warning("Données non chargées")
            return None
        
        try:
            city_data = self.data[self.data['ville'] == city_name].copy()
            if city_data.empty:
                logging.warning(f"Aucune donnée pour {city_name}")
                return None
                
            if save_csv:
                city_dir = self.output_dir / 'villes'
                city_dir.mkdir(exist_ok=True)
                output_path = city_dir / f"{city_name.lower()}_data.csv"
                city_data.to_csv(output_path, index=False)
                logging.info(f"Données pour {city_name} sauvegardées dans {output_path}")
            
            return city_data
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données pour {city_name}: {str(e)}")
            return None

    def get_time_period_data(self, start_date: str, end_date: str, save_csv: bool = False) -> Optional[pd.DataFrame]:
        """Filtre les données par période temporelle avec validation"""
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            if start_dt > end_dt:
                logging.error("La date de début doit être antérieure à la date de fin")
                return None
                
            mask = (self.data['datetime'] >= start_dt) & (self.data['datetime'] <= end_dt)
            period_data = self.data.loc[mask].copy()
            
            if period_data.empty:
                logging.warning(f"Aucune donnée disponible entre {start_date} et {end_date}")
                return None
                
            if save_csv:
                period_dir = self.output_dir / 'periodes'
                period_dir.mkdir(exist_ok=True)
                filename = f"donnees_{start_dt:%Y%m%d}_to_{end_dt:%Y%m%d}.csv"
                output_path = period_dir / filename
                period_data.to_csv(output_path, index=False)
                logging.info(f"Données sauvegardées dans {output_path}")
            
            return period_data
            
        except ValueError as e:
            logging.error(f"Format de date invalide : {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Erreur de filtrage temporel : {str(e)}", exc_info=True)
            return None

    def generate_analysis_report(self, output_file: str = 'rapport_meteo.png') -> bool:
        """Génère un rapport d'analyse complet avec gestion de style robuste"""
        if not self.loaded:
            logging.error("Données non chargées")
            return False
            
        try:
            # Configuration du style avec fallback
            available_styles = plt.style.available
            preferred_styles = ['seaborn-v0_8', 'seaborn', 'ggplot', 'default']
            
            for style in preferred_styles:
                if style in available_styles:
                    plt.style.use(style)
                    sns.set_palette("husl")
                    break
            else:
                logging.warning("Aucun style préféré disponible, utilisation du style par défaut")
                sns.reset_defaults()
            
            # Création des graphiques
            fig, axes = plt.subplots(3, 1, figsize=(15, 18))
            
            # Graphique 1: Distribution des températures
            sns.boxplot(data=self.data, x='ville', y='temperature_c', ax=axes[0])
            axes[0].set_title('Distribution des Températures par Ville', pad=20)
            axes[0].set_xlabel('')
            axes[0].set_ylabel('Température (°C)')
            
            # Graphique 2: Tendances temporelles
            sample_data = self.data.sample(min(1000, len(self.data)))
            sns.lineplot(
                data=sample_data, 
                x='datetime', 
                y='temperature_c', 
                hue='ville',
                estimator='mean', 
                errorbar=None,
                ax=axes[1]
            )
            axes[1].set_title('Évolution des Températures Moyennes', pad=20)
            axes[1].set_xlabel('Date')
            axes[1].set_ylabel('Température (°C)')
            axes[1].legend(title='Ville')
            
            # Graphique 3: Corrélations
            numeric_cols = ['temperature_c', 'humidite_pourcent', 'vent_vitesse_kmh', 'pression_hpa']
            numeric_cols = [col for col in numeric_cols if col in self.data.columns]
            
            if len(numeric_cols) >= 2:  # Nécessite au moins 2 colonnes pour une corrélation
                sns.heatmap(
                    self.data[numeric_cols].corr(), 
                    annot=True, 
                    cmap='coolwarm', 
                    center=0,
                    ax=axes[2]
                )
                axes[2].set_title('Matrice de Corrélation', pad=20)
            else:
                axes[2].axis('off')
                axes[2].text(0.5, 0.5, 'Pas assez de données numériques\npour la matrice de corrélation',
                            ha='center', va='center')
            
            # Sauvegarde du rapport
            plt.tight_layout()
            output_path = self.output_dir / output_file
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logging.info(f"Rapport d'analyse généré avec succès : {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"Erreur lors de la génération du rapport : {str(e)}", exc_info=True)
            return False

if __name__ == "__main__":
    try:
        loader = WeatherDataLoader()
        
        if loader.load_data(source_format='auto'):
            # Exemple d'utilisation
            paris_data = loader.get_city_data('Paris', save_csv=True)
            if paris_data is not None:
                print("\nStatistiques pour Paris:")
                print(paris_data[['datetime', 'temperature_c', 'conditions']].tail(3))
            
            # Génération du rapport complet
            loader.generate_analysis_report()
            
            # Export des données récentes
            loader.get_time_period_data(
                start_date="2023-06-01",
                end_date="2023-06-30",
                save_csv=True
            )
    except Exception as e:
        logging.critical(f"Erreur critique dans le main : {str(e)}", exc_info=True)