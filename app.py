# app.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Vérification des imports
try:
    import plotly.graph_objects as go
    import plotly.express as px
    print("✅ Plotly importé avec succès")
except ImportError as e:
    st.error("❌ Erreur : Plotly n'est pas installé. Exécutez: pip install plotly")
    st.stop()

# Configuration de la page
st.set_page_config(
    page_title="🌤️ Météo Tourisme - Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# Données de démonstration
@st.cache_data
def load_demo_data():
    """Génère des données de démonstration pour le projet météo tourisme"""
    np.random.seed(42)
    
    cities = ['Paris', 'Londres', 'Rome', 'Madrid', 'Berlin', 'Barcelone', 'Amsterdam', 'Prague']
    months = list(range(1, 13))
    month_names = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
                   'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']
    
    data = []
    for city in cities:
        for month in months:
            # Simulation de données météo réalistes selon la saison
            if month in [12, 1, 2]:  # Hiver
                temp_base = 5 if city != 'Rome' else 12
                temp = np.random.normal(temp_base, 3)
                rain = np.random.normal(15, 5)
            elif month in [6, 7, 8]:  # Été
                temp_base = 25 if city != 'Londres' else 20
                temp = np.random.normal(temp_base, 4)
                rain = np.random.normal(5, 2)
            else:  # Printemps/Automne
                temp_base = 15
                temp = np.random.normal(temp_base, 5)
                rain = np.random.normal(10, 3)
            
            wind = np.random.normal(12, 4)
            humidity = np.random.normal(65, 15)
            
            # Calcul du score touristique
            temp_score = 100 if 22 <= temp <= 28 else max(0, 100 - abs(temp - 25) * 5)
            rain_score = max(0, 100 - rain * 10)
            wind_score = max(0, 100 - wind * 3)
            tourism_score = (temp_score * 0.4 + rain_score * 0.35 + wind_score * 0.25)
            
            data.append({
                'city_name': city,
                'month': month,
                'month_name': month_names[month-1],
                'temperature': max(temp, -10),
                'precipitation': max(rain, 0),
                'wind_speed': max(wind, 0),
                'humidity': max(min(humidity, 100), 0),
                'tourism_score': max(tourism_score, 0)
            })
    
    return pd.DataFrame(data)

def main():
    # Titre principal
    st.title("🌤️ Climat et Tourisme - Quand Voyager ?")
    st.markdown("""
    **Découvrez les meilleures périodes pour visiter vos destinations préférées**
    
    Ce dashboard analyse les conditions météorologiques pour vous recommander 
    les périodes optimales de voyage selon vos préférences.
    """)
    
    # Chargement des données
    data = load_demo_data()
    
    # Sidebar pour filtres
    with st.sidebar:
        st.header("🎯 Paramètres")
        
        # Sélection ville
        cities = sorted(data['city_name'].unique())
        selected_city = st.selectbox("🏙️ Choisir une ville", cities)
        
        st.divider()
        
        # Critères de pondération
        st.subheader("⚖️ Pondération des critères")
        st.caption("Ajustez l'importance de chaque critère selon vos préférences")
        
        temp_weight = st.slider("🌡️ Température", 0.0, 1.0, 0.4, 0.05)
        rain_weight = st.slider("🌧️ Précipitations", 0.0, 1.0, 0.35, 0.05)
        wind_weight = st.slider("💨 Vent", 0.0, 1.0, 0.25, 0.05)
        
        # Normalisation des poids
        total_weight = temp_weight + rain_weight + wind_weight
        if total_weight > 0:
            temp_weight /= total_weight
            rain_weight /= total_weight
            wind_weight /= total_weight
        
        st.divider()
        
        # Paramètres de température idéale
        st.subheader("🌡️ Température Idéale")
        temp_range = st.slider(
            "Plage de température préférée (°C)",
            min_value=10,
            max_value=35,
            value=(22, 28),
            step=1
        )
    
    # Filtrage des données
    city_data = data[data['city_name'] == selected_city].copy()
    
    # Recalcul du score avec les nouveaux poids
    city_data['temp_score'] = city_data['temperature'].apply(
        lambda x: 100 if temp_range[0] <= x <= temp_range[1] 
        else max(0, 100 - abs(x - sum(temp_range)/2) * 5)
    )
    city_data['rain_score'] = city_data['precipitation'].apply(
        lambda x: max(0, 100 - x * 10)
    )
    city_data['wind_score'] = city_data['wind_speed'].apply(
        lambda x: max(0, 100 - x * 3)
    )
    city_data['tourism_score'] = (
        city_data['temp_score'] * temp_weight +
        city_data['rain_score'] * rain_weight +
        city_data['wind_score'] * wind_weight
    )
    
    # Métriques principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_temp = city_data['temperature'].mean()
        st.metric("🌡️ Température Moyenne", f"{avg_temp:.1f}°C")
    
    with col2:
        avg_rain = city_data['precipitation'].mean()
        st.metric("🌧️ Précipitations Moyennes", f"{avg_rain:.1f}mm")
    
    with col3:
        avg_wind = city_data['wind_speed'].mean()
        st.metric("💨 Vent Moyen", f"{avg_wind:.1f}km/h")
    
    with col4:
        avg_score = city_data['tourism_score'].mean()
        st.metric("⭐ Score Tourisme", f"{avg_score:.0f}/100")
    
    # Graphiques principaux
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Score Météo par Mois")
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=city_data['month_name'],
            y=city_data['tourism_score'],
            marker_color=city_data['tourism_score'],
            marker_colorscale='RdYlGn',
            name='Score Tourisme',
            text=[f"{score:.0f}" for score in city_data['tourism_score']],
            textposition='auto',
        ))
        fig.update_layout(
            title=f"Score de Recommandation Mensuel - {selected_city}",
            xaxis_title="Mois",
            yaxis_title="Score (0-100)",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🌡️ Évolution des Températures")
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=city_data['month_name'],
            y=city_data['temperature'],
            mode='lines+markers',
            line=dict(color='orange', width=3),
            marker=dict(size=8),
            name='Température'
        ))
        fig.add_hline(y=temp_range[0], line_dash="dash", line_color="green", 
                      annotation_text="Température idéale min")
        fig.add_hline(y=temp_range[1], line_dash="dash", line_color="red", 
                      annotation_text="Température idéale max")
        fig.update_layout(
            title=f"Températures Moyennes Mensuelles - {selected_city}",
            xaxis_title="Mois",
            yaxis_title="Température (°C)",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Détails météorologiques
    st.subheader("🌦️ Détails Météorologiques par Mois")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Précipitations**")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=city_data['month_name'],
            y=city_data['precipitation'],
            marker_color='lightblue',
            name='Précipitations'
        ))
        fig.update_layout(
            title="Précipitations Mensuelles (mm)",
            xaxis_title="Mois",
            yaxis_title="Précipitations (mm)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.write("**Vitesse du Vent**")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=city_data['month_name'],
            y=city_data['wind_speed'],
            marker_color='lightgreen',
            name='Vent'
        ))
        fig.update_layout(
            title="Vitesse du Vent (km/h)",
            xaxis_title="Mois",
            yaxis_title="Vitesse (km/h)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Recommandations personnalisées
    st.subheader("🎯 Recommandations Personnalisées")
    
    # Calcul des meilleures périodes
    best_months = city_data.nlargest(3, 'tourism_score')
    worst_months = city_data.nsmallest(3, 'tourism_score')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.success("✅ **Meilleures Périodes**")
        for i, (_, month) in enumerate(best_months.iterrows(), 1):
            st.write(f"**{i}. {month['month_name']}** - Score: {month['tourism_score']:.0f}/100")
            with st.expander(f"Détails {month['month_name']}"):
                st.write(f"🌡️ Température: {month['temperature']:.1f}°C")
                st.write(f"🌧️ Précipitations: {month['precipitation']:.1f}mm")
                st.write(f"💨 Vent: {month['wind_speed']:.1f}km/h")
    
    with col2:
        st.warning("⚠️ **Périodes à Éviter**")
        for i, (_, month) in enumerate(worst_months.iterrows(), 1):
            st.write(f"**{i}. {month['month_name']}** - Score: {month['tourism_score']:.0f}/100")
            with st.expander(f"Détails {month['month_name']}"):
                st.write(f"🌡️ Température: {month['temperature']:.1f}°C")
                st.write(f"🌧️ Précipitations: {month['precipitation']:.1f}mm")
                st.write(f"💨 Vent: {month['wind_speed']:.1f}km/h")
    
    # Comparaison multi-villes
    st.subheader("🌍 Comparaison Multi-Destinations")
    
    selected_cities = st.multiselect(
        "Sélectionnez des villes à comparer",
        cities,
        default=[selected_city],
        max_selections=5
    )
    
    if len(selected_cities) > 1:
        comparison_data = data[data['city_name'].isin(selected_cities)]
        
        fig = px.line(
            comparison_data,
            x='month',
            y='tourism_score',
            color='city_name',
            title="Comparaison des Scores Météo",
            labels={'month': 'Mois', 'tourism_score': 'Score Tourisme', 'city_name': 'Ville'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Tableau de comparaison
        st.subheader("📋 Tableau de Comparaison")
        comparison_summary = comparison_data.groupby(['city_name']).agg({
            'tourism_score': 'mean',
            'temperature': 'mean',
            'precipitation': 'mean',
            'wind_speed': 'mean'
        }).round(1)
        comparison_summary.columns = ['Score Moyen', 'Temp. Moy. (°C)', 'Précip. Moy. (mm)', 'Vent Moy. (km/h)']
        st.dataframe(comparison_summary, use_container_width=True)
    
    # Informations techniques
    with st.expander("🔧 Informations Techniques"):
        st.markdown("""
        **Méthode de Calcul du Score :**
        - **Température** : Score optimal entre la plage définie par l'utilisateur
        - **Précipitations** : Moins de précipitations = score plus élevé
        - **Vent** : Vitesse modérée préférable pour le confort
        
        **Formule du Score :**
        ```
        Score = (Score_Temp × Poids_Temp) + (Score_Pluie × Poids_Pluie) + (Score_Vent × Poids_Vent)
        ```
        
        **Sources de Données :**
        - Données simulées pour démonstration
        - Dans un projet réel : API OpenWeather, données historiques Kaggle
        - Fréquence de mise à jour : Quotidienne via pipeline Airflow
        """)

if __name__ == "__main__":
    main()
