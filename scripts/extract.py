#!/usr/bin/env python3
"""
Script d'extraction des données météo (historiques et temps réel)
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from typing import List, Dict, Any

# Configuration de base
API_KEY = "625b9c2fc2a42cd66d6e7e258bb443a3"  # Remplacez par votre clé valide
BASE_URL = "https://api.openweathermap.org/data/2.5"
HISTORICAL_URL = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
DATA_DIR = "data/raw"
CITIES = ["Paris", "London", "Tokyo", "New York", "Berlin"]  # Villes à suivre
DAYS_BACK = 30  # Nombre de jours d'historique à récupérer

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherExtractor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "WeatherDataCollector/1.0"})

    def _make_request(self, url: str, params: dict) -> Dict[str, Any] | None:
        """Effectue une requête API générique avec gestion des erreurs"""
        try:
            response = self.session.get(url, params=params, timeout=15)
            
            # Gestion spéciale pour les données historiques payantes
            if "timemachine" in url and response.status_code == 401:
                error_data = response.json()
                if "subscription" in error_data.get("message", "").lower():
                    logger.warning("Abonnement payant requis pour les données historiques")
                    return None
            
            response.raise_for_status()
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Erreur API: {str(e)}")
            return None

    def get_city_coords(self, city_name: str) -> Dict[str, float] | None:
        """Récupère les coordonnées GPS d'une ville"""
        params = {
            "q": city_name,
            "appid": self.api_key,
            "units": "metric"
        }
        data = self._make_request(f"{BASE_URL}/weather", params)
        if data and "coord" in data:
            return {
                "lat": data["coord"]["lat"],
                "lon": data["coord"]["lon"],
                "city": data["name"],
                "country": data["sys"]["country"]
            }
        return None

    def get_historical_weather(self, lat: float, lon: float, target_date: datetime) -> Dict[str, Any] | None:
        """Récupère les données météo historiques pour une date spécifique"""
        params = {
            "lat": lat,
            "lon": lon,
            "dt": int(target_date.timestamp()),
            "appid": self.api_key,
            "units": "metric",
            "lang": "fr"
        }
        data = self._make_request(HISTORICAL_URL, params)
        
        if data and "data" in data and len(data["data"]) > 0:
            point = data["data"][0]  # Premier point de données
            return {
                "date": target_date.strftime("%Y-%m-%d"),
                "heure": datetime.fromtimestamp(point["dt"]).strftime("%H:%M:%S"),
                "temperature": point["temp"],
                "humidite": point["humidity"],
                "pression": point["pressure"],
                "vent_vitesse": point["wind_speed"],
                "description": point["weather"][0]["description"],
                "ville": data.get("name", ""),
                "pays": data.get("country", "")
            }
        return None

    def get_current_weather(self, city_name: str) -> Dict[str, Any] | None:
        """Récupère les données météo actuelles"""
        params = {
            "q": city_name,
            "appid": self.api_key,
            "units": "metric",
            "lang": "fr"
        }
        data = self._make_request(f"{BASE_URL}/weather", params)
        
        if data:
            return {
                "ville": data["name"],
                "pays": data["sys"]["country"],
                "date": datetime.now().strftime("%Y-%m-%d"),
                "heure": datetime.now().strftime("%H:%M:%S"),
                "temperature": data["main"]["temp"],
                "humidite": data["main"]["humidity"],
                "pression": data["main"]["pressure"],
                "vent_vitesse": data["wind"]["speed"],
                "description": data["weather"][0]["description"]
            }
        return None

def setup_data_directory():
    """Crée l'arborescence des dossiers de données"""
    os.makedirs(f"{DATA_DIR}/historical", exist_ok=True)
    os.makedirs(f"{DATA_DIR}/realtime", exist_ok=True)
    logger.info("Arborescence des dossiers créée")

def save_data(data: List[Dict[str, Any]], data_type: str, filename: str):
    """Sauvegarde les données dans le format approprié"""
    filepath = f"{DATA_DIR}/{data_type}/{filename}"
    
    # Sauvegarde en JSON
    with open(f"{filepath}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    # Sauvegarde en CSV (si des données existent)
    if data:
        pd.DataFrame(data).to_csv(f"{filepath}.csv", index=False)
    
    logger.info(f"Données sauvegardées: {filepath} ({len(data)} enregistrements)")

def collect_historical_data(extractor: WeatherExtractor, cities: List[str], days_back: int) -> List[Dict[str, Any]]:
    """Collecte les données historiques pour toutes les villes"""
    historical_data = []
    
    for city in cities:
        logger.info(f"Traitement des données historiques pour {city}...")
        coords = extractor.get_city_coords(city)
        if not coords:
            continue
            
        for day in range(1, days_back + 1):
            target_date = datetime.now() - timedelta(days=day)
            weather = extractor.get_historical_weather(coords["lat"], coords["lon"], target_date)
            
            if weather:
                weather.update({
                    "ville": coords["city"],
                    "pays": coords["country"],
                    "lat": coords["lat"],
                    "lon": coords["lon"]
                })
                historical_data.append(weather)
            
            # Pause pour éviter de surcharger l'API
            time.sleep(0.2)
    
    return historical_data

def collect_realtime_data(extractor: WeatherExtractor, cities: List[str]) -> List[Dict[str, Any]]:
    """Collecte les données en temps réel pour toutes les villes"""
    realtime_data = []
    
    for city in cities:
        logger.info(f"Traitement des données temps réel pour {city}...")
        weather = extractor.get_current_weather(city)
        if weather:
            realtime_data.append(weather)
        time.sleep(0.1)
    
    return realtime_data

def main():
    """Point d'entrée principal du script"""
    setup_data_directory()
    extractor = WeatherExtractor(API_KEY)
    
    # 1. Extraction des données historiques
    historical_data = collect_historical_data(extractor, CITIES, DAYS_BACK)
    save_data(
        historical_data,
        "historical",
        f"historical_weather_{datetime.now().strftime('%Y%m%d')}"
    )
    
    # 2. Extraction des données en temps réel
    realtime_data = collect_realtime_data(extractor, CITIES)
    save_data(
        realtime_data,
        "realtime",
        f"realtime_weather_{datetime.now().strftime('%Y%m%d')}"
    )
    
    logger.info("Extraction terminée avec succès!")

if __name__ == "__main__":
    main()