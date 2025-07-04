import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

## 1. Nettoyage et préparation des données
def clean_weather_data(hourly_df, daily_df):
    # Conversion des timestamps Unix en datetime
    daily_df['sunrise'] = pd.to_datetime(daily_df['sunrise'], unit='s')
    daily_df['sunset'] = pd.to_datetime(daily_df['sunset'], unit='s')
    
    # Conversion des colonnes de date
    hourly_df['date'] = pd.to_datetime(hourly_df['date'])
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    
    # Extraction des composantes temporelles
    hourly_df['hour'] = hourly_df['date'].dt.hour
    hourly_df['month'] = hourly_df['date'].dt.month
    hourly_df['year'] = hourly_df['date'].dt.year
    
    return hourly_df, daily_df

## 2. Analyse des données
def analyze_weather(hourly_df, daily_df):
    print("\n=== Statistiques Clés ===")
    print(f"Période couverte: {hourly_df['date'].min()} à {hourly_df['date'].max()}")
    print(f"Nombre de relevés horaires: {len(hourly_df)}")
    print(f"Température moyenne: {hourly_df['temperature_2m'].mean():.1f}°C")
    print(f"Température minimale: {hourly_df['temperature_2m'].min():.1f}°C")
    print(f"Température maximale: {hourly_df['temperature_2m'].max():.1f}°C")
    
    # Températures par mois
    monthly_stats = hourly_df.groupby('month')['temperature_2m'].agg(['mean', 'min', 'max'])
    print("\nTempératures par mois:")
    print(monthly_stats)
    
    # Variation diurne
    diurnal_pattern = hourly_df.groupby('hour')['temperature_2m'].mean()
    
    return monthly_stats, diurnal_pattern

## 3. Visualisation des données
def plot_weather_data(hourly_df, daily_df, monthly_stats, diurnal_pattern):
    plt.figure(figsize=(15, 10))
    
    # Graphique 1: Evolution temporelle
    plt.subplot(2, 2, 1)
    sample_data = hourly_df[hourly_df['date'] < '2022-03-01']  # Premier mois pour visibilité
    plt.plot(sample_data['date'], sample_data['temperature_2m'])
    plt.title('Evolution des températures (Février 2022)')
    plt.xlabel('Date')
    plt.ylabel('Température (°C)')
    plt.grid(True)
    
    # Graphique 2: Profil diurne
    plt.subplot(2, 2, 2)
    diurnal_pattern.plot(kind='bar')
    plt.title('Profil diurne moyen des températures')
    plt.xlabel('Heure de la journée')
    plt.ylabel('Température moyenne (°C)')
    plt.xticks(rotation=0)
    plt.grid(True)
    
    # Graphique 3: Variations mensuelles
    plt.subplot(2, 2, 3)
    monthly_stats.plot(kind='line', marker='o')
    plt.title('Variations mensuelles des températures')
    plt.xlabel('Mois')
    plt.ylabel('Température (°C)')
    plt.grid(True)
    plt.legend(['Moyenne', 'Minimum', 'Maximum'])
    
    # Graphique 4: Comparaison jour/nuit
    plt.subplot(2, 2, 4)
    daily_df['day_length'] = (daily_df['sunset'] - daily_df['sunrise']).dt.total_seconds() / 3600
    plt.scatter(daily_df['day_length'], daily_df['temperature_2m_max'] - daily_df['temperature_2m_min'])
    plt.title('Amplitude thermique vs Durée du jour')
    plt.xlabel('Durée du jour (heures)')
    plt.ylabel('Amplitude thermique (°C)')
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()

## Chargement des données (à adapter selon votre source)
# hourly_data = pd.read_csv('hourly_weather.csv')
# daily_data = pd.read_csv('daily_weather.csv')

# Pour cet exemple, je crée des DataFrames simulés basés sur votre sortie
hourly_data = pd.DataFrame({
    'date': pd.date_range(start='2022-02-16 21:00:00', end='2025-07-01 20:00:00', freq='H'),
    'temperature_2m': [19.316, 19.216, 19.116, 19.266, 18.316] + 
                     [13.116, 12.516, 12.566, 12.716, 12.966]  # Données simplifiées
})

daily_data = pd.DataFrame({
    'date': pd.date_range(start='2022-02-16', end='2025-06-30', freq='D'),
    'temperature_2m_min': [16.316, 14.016, 12.616, 15.116, 12.466] +
                         [10.666, 13.166, 11.966, 7.816, 12.016],  # Données simplifiées
    'sunrise': [1645065862, 1645152290, 1645238717, 1645325144, 1645411570] +
              [1750994819, 1751081227, 1751167635, 1751254041, 1751340446],
    'sunset': [1645111620, 1645197983, 1645284344, 1645370704, 1645457064] +
             [1751034160, 1751120578, 1751206996, 1751293415, 1751379834]
})

# Nettoyage des données
hourly_clean, daily_clean = clean_weather_data(hourly_data, daily_data)

# Analyse
monthly_stats, diurnal_pattern = analyze_weather(hourly_clean, daily_clean)

# Visualisation
plot_weather_data(hourly_clean, daily_clean, monthly_stats, diurnal_pattern)