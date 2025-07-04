# scripts/transform.py
import os
import pandas as pd
import sys

def check_data_files():
    """Vérifie l'existence des fichiers requis et retourne les chemins complets"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    required_files = {
        'current': 'current_weather.csv',
        'historical': 'historical_weather.csv'
    }
    
    missing_files = []
    file_paths = {}
    
    for key, filename in required_files.items():
        path = os.path.join(data_dir, filename)
        if not os.path.exists(path):
            missing_files.append(filename)
        file_paths[key] = path
    
    if missing_files:
        print("\nERREUR: Fichiers manquants dans data/:")
        for file in missing_files:
            print(f"- {file}")
        print("\nSolution possible:")
        print("1. Exécutez le script extract.py pour générer les données")
        print("2. Ou exécutez generate_test_data.py pour créer des données de test")
        return None
    
    return file_paths

def transform_data():
    """Fonction principale de transformation"""
    print("\n=== Début de la transformation ===")
    
    # 1. Vérification des fichiers
    file_paths = check_data_files()
    if not file_paths:
        return False
    
    # 2. Chargement des données
    print("Chargement des fichiers...")
    try:
        current_df = pd.read_csv(file_paths['current'])
        historical_df = pd.read_csv(file_paths['historical'])
        
        if current_df.empty or historical_df.empty:
            print("ERREUR: Un ou plusieurs fichiers sont vides")
            return False
    except Exception as e:
        print(f"ERREUR lors du chargement: {str(e)}")
        return False
    
    # [Le reste de votre code de transformation...]
    
    return True

if __name__ == "__main__":
    if transform_data():
        print("Transformation terminée avec succès")
        sys.exit(0)
    else:
        print("Échec de la transformation")
        sys.exit(1)