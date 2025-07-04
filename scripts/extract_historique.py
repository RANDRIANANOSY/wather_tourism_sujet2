import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
import calendar
import sys

# Configuration des styles CORRIGÉE
sns.set_theme(style="whitegrid")  # Remplace plt.style.use('seaborn')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

class WeatherAnalyzer:
    def __init__(self, hourly_path=None, daily_path=None):
        """
        Initialise l'analyseur de données météo.
        
        Args:
            hourly_path (str): Chemin vers le fichier des données horaires
            daily_path (str): Chemin vers le fichier des données quotidiennes
        """
        self.hourly_df = None
        self.daily_df = None
        
        if hourly_path and daily_path:
            self.load_data(hourly_path, daily_path)
    
    def load_data(self, hourly_path, daily_path):
        """Charge les données depuis les fichiers CSV avec meilleure gestion des erreurs"""
        try:
            self.hourly_df = pd.read_csv(
                hourly_path, 
                parse_dates=['date'],
                infer_datetime_format=True
            )
            self.daily_df = pd.read_csv(
                daily_path,
                parse_dates=['date'],
                infer_datetime_format=True
            )
            print("Données chargées avec succès!")
            return True
        except FileNotFoundError as e:
            print(f"Erreur: Fichier non trouvé - {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"Erreur lors du chargement: {str(e)}")
            sys.exit(1)
    
    def clean_data(self):
        """Nettoie et prépare les données pour l'analyse"""
        if self.daily_df is None or self.hourly_df is None:
            raise ValueError("Veuillez d'abord charger les données")
        
        try:
            # Conversion des timestamps
            if 'sunrise' in self.daily_df.columns:
                self.daily_df['sunrise'] = pd.to_datetime(
                    self.daily_df['sunrise'], 
                    unit='s',
                    errors='coerce'
                )
            if 'sunset' in self.daily_df.columns:
                self.daily_df['sunset'] = pd.to_datetime(
                    self.daily_df['sunset'], 
                    unit='s',
                    errors='coerce'
                )
            
            # Extraction des caractéristiques temporelles
            for df in [self.hourly_df, self.daily_df]:
                df['year'] = df['date'].dt.year
                df['month'] = df['date'].dt.month
                df['month_name'] = df['month'].apply(lambda x: calendar.month_abbr[x])
                df['day_of_week'] = df['date'].dt.dayofweek
                df['day_name'] = df['date'].dt.day_name()
                df['season'] = df['month'].apply(self._get_season)
            
            # Ajout de la durée du jour (si les colonnes existent)
            if all(col in self.daily_df.columns for col in ['sunrise', 'sunset']):
                self.daily_df['day_length'] = (
                    self.daily_df['sunset'] - self.daily_df['sunrise']
                ).dt.total_seconds() / 3600
            
            # Calcul de l'amplitude thermique (si les colonnes existent)
            if all(col in self.daily_df.columns for col in ['temperature_2m_max', 'temperature_2m_min']):
                self.daily_df['temp_amplitude'] = (
                    self.daily_df['temperature_2m_max'] - self.daily_df['temperature_2m_min']
                )
            
            return self.hourly_df, self.daily_df
            
        except Exception as e:
            print(f"Erreur lors du nettoyage des données: {str(e)}")
            sys.exit(1)
    
    def _get_season(self, month):
        """Retourne la saison correspondante au mois"""
        if 3 <= month <= 5:
            return 'Printemps'
        elif 6 <= month <= 8:
            return 'Été'
        elif 9 <= month <= 11:
            return 'Automne'
        else:
            return 'Hiver'
    
    def analyze(self):
        """Effectue une analyse complète des données avec gestion des erreurs"""
        if self.hourly_df is None or self.daily_df is None:
            raise ValueError("Données non chargées")
        
        try:
            results = {}
            
            # Statistiques de base
            results['period'] = (self.hourly_df['date'].min(), self.hourly_df['date'].max())
            results['hourly_count'] = len(self.hourly_df)
            
            # Vérifie si la colonne temperature_2m existe
            if 'temperature_2m' in self.hourly_df.columns:
                temp_stats = self.hourly_df['temperature_2m'].agg(['mean', 'min', 'max', 'std'])
                results['temp_stats'] = temp_stats.to_dict()
            else:
                results['temp_stats'] = None
            
            # Analyse temporelle
            if 'temperature_2m' in self.hourly_df.columns and 'hour' in self.hourly_df.columns:
                results['monthly_stats'] = self.hourly_df.groupby(
                    ['year', 'month', 'month_name']
                )['temperature_2m'].agg(['mean', 'min', 'max', 'std']).reset_index()
                
                results['diurnal_pattern'] = self.hourly_df.groupby('hour')['temperature_2m'].agg(
                    ['mean', 'std']).reset_index()
            else:
                results['monthly_stats'] = None
                results['diurnal_pattern'] = None
            
            # Analyse saisonnière
            if 'season' in self.daily_df.columns:
                seasonal_cols = {}
                if 'temperature_2m_mean' in self.daily_df.columns:
                    seasonal_cols['temperature_2m_mean'] = 'mean'
                if 'temp_amplitude' in self.daily_df.columns:
                    seasonal_cols['temp_amplitude'] = 'mean'
                if 'day_length' in self.daily_df.columns:
                    seasonal_cols['day_length'] = 'mean'
                
                if seasonal_cols:
                    results['seasonal_stats'] = self.daily_df.groupby('season').agg(seasonal_cols)
                else:
                    results['seasonal_stats'] = None
            else:
                results['seasonal_stats'] = None
            
            return results
            
        except Exception as e:
            print(f"Erreur lors de l'analyse: {str(e)}")
            sys.exit(1)
    
    def visualize(self, results):
        """Génère des visualisations à partir des résultats"""
        try:
            # 1. Graphique d'évolution temporelle
            plt.figure(figsize=(18, 12))
            
            if 'temperature_2m' in self.hourly_df.columns:
                # 1. Evolution temporelle (7 jours pour la lisibilité)
                plt.subplot(2, 2, 1)
                sample = self.hourly_df[self.hourly_df['date'] <= self.hourly_df['date'].min() + pd.Timedelta(days=7)]
                plt.plot(sample['date'], sample['temperature_2m'], marker='o', markersize=3)
                plt.title('Evolution des températures sur une semaine')
                plt.xlabel('Date')
                plt.ylabel('Température (°C)')
                plt.grid(True)
            
            # 2. Profil diurne
            if results.get('diurnal_pattern') is not None:
                plt.subplot(2, 2, 2)
                sns.lineplot(data=results['diurnal_pattern'], x='hour', y='mean')
                plt.fill_between(
                    results['diurnal_pattern']['hour'],
                    results['diurnal_pattern']['mean'] - results['diurnal_pattern']['std'],
                    results['diurnal_pattern']['mean'] + results['diurnal_pattern']['std'],
                    alpha=0.2
                )
                plt.title('Profil diurne des températures')
                plt.xlabel('Heure de la journée')
                plt.ylabel('Température moyenne (°C)')
                plt.xticks(range(0, 24))
                plt.grid(True)
            
            # 3. Variations mensuelles
            if results.get('monthly_stats') is not None:
                plt.subplot(2, 2, 3)
                sns.lineplot(
                    data=results['monthly_stats'],
                    x='month_name',
                    y='mean',
                    hue='year',
                    marker='o'
                )
                plt.title('Variations mensuelles des températures')
                plt.xlabel('Mois')
                plt.ylabel('Température moyenne (°C)')
                plt.grid(True)
                plt.legend(title='Année')
            
            # 4. Relation durée du jour / amplitude thermique
            if all(col in self.daily_df.columns for col in ['day_length', 'temp_amplitude', 'season']):
                plt.subplot(2, 2, 4)
                sns.scatterplot(
                    data=self.daily_df,
                    x='day_length',
                    y='temp_amplitude',
                    hue='season',
                    palette='viridis',
                    alpha=0.6
                )
                plt.title('Amplitude thermique vs Durée du jour')
                plt.xlabel('Durée du jour (heures)')
                plt.ylabel('Amplitude thermique (°C)')
                plt.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Graphique supplémentaire: Boxplot saisonnier
            if 'temperature_2m' in self.hourly_df.columns and 'season' in self.hourly_df.columns:
                plt.figure(figsize=(10, 6))
                sns.boxplot(
                    data=self.hourly_df,
                    x='season',
                    y='temperature_2m',
                    order=['Printemps', 'Été', 'Automne', 'Hiver']
                )
                plt.title('Distribution des températures par saison')
                plt.xlabel('Saison')
                plt.ylabel('Température (°C)')
                plt.grid(True)
                plt.show()
                
        except Exception as e:
            print(f"Erreur lors de la visualisation: {str(e)}")
            sys.exit(1)

# Exemple d'utilisation
if __name__ == "__main__":
    # Création de données de test (remplacer par vos fichiers réels)
    try:
        hourly_data = pd.DataFrame({
            'date': pd.date_range(start='2022-02-16 21:00:00', periods=1000, freq='H'),
            'temperature_2m': np.sin(np.linspace(0, 10*np.pi, 1000)) * 10 + 15 + np.random.normal(0, 2, 1000)
        })
        hourly_data['hour'] = hourly_data['date'].dt.hour
        
        daily_data = pd.DataFrame({
            'date': pd.date_range(start='2022-02-16', periods=100, freq='D'),
            'temperature_2m_min': np.random.normal(5, 3, 100),
            'temperature_2m_max': np.random.normal(20, 3, 100),
            'temperature_2m_mean': np.random.normal(12, 2, 100),
            'sunrise': np.random.randint(1645065862, 1645065862+86400*100, 100),
            'sunset': np.random.randint(1645065862+43200, 1645065862+86400*100, 100)
        })
        
        # Sauvegarde et rechargement pour simuler des fichiers
        hourly_data.to_csv('hourly_test.csv', index=False)
        daily_data.to_csv('daily_test.csv', index=False)
        
        # Initialisation de l'analyseur
        analyzer = WeatherAnalyzer('hourly_test.csv', 'daily_test.csv')
        
        # Nettoyage des données
        hourly_clean, daily_clean = analyzer.clean_data()
        
        # Analyse
        analysis_results = analyzer.analyze()
        
        # Affichage des résultats
        print("\n=== Statistiques de base ===")
        print(f"Période: {analysis_results['period'][0]} à {analysis_results['period'][1]}")
        print(f"Nombre de relevés: {analysis_results['hourly_count']}")
        
        if analysis_results['temp_stats']:
            print(f"Température moyenne: {analysis_results['temp_stats']['mean']:.1f}°C")
        
        print("\n=== Statistiques mensuelles ===")
        if analysis_results['monthly_stats'] is not None:
            print(analysis_results['monthly_stats'].head())
        else:
            print("Données mensuelles non disponibles")
        
        # Visualisation
        analyzer.visualize(analysis_results)
        
    except Exception as e:
        print(f"Erreur dans l'exemple d'utilisation: {str(e)}")
        sys.exit(1)