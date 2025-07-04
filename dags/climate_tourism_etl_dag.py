"""
Apache Airflow DAG for Climate Tourism Analysis ETL Pipeline
Complete ETL pipeline with extraction, cleaning, modeling, and reporting
"""

from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import json
import numpy as np

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Configuration
DAG_ID = 'climate_tourism_etl_pipeline'
PROJECT_ROOT = '/home/user/output/climate_tourism_project'
DATA_RAW_DIR = f'{PROJECT_ROOT}/data/raw'
DATA_PROCESSED_DIR = f'{PROJECT_ROOT}/data/processed'
REPORTS_DIR = f'{PROJECT_ROOT}/reports'

default_args = {
    'owner': 'climate_tourism_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Cities configuration
CITIES_CONFIG = [
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "London", "country": "United Kingdom", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "New York", "country": "United States", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Tokyo", "country": "Japan", "latitude": 35.6762, "longitude": 139.6503},
    {"city": "Sydney", "country": "Australia", "latitude": -33.8688, "longitude": 151.2093},
    {"city": "Berlin", "country": "Germany", "latitude": 52.5200, "longitude": 13.4050},
    {"city": "Rome", "country": "Italy", "latitude": 41.9028, "longitude": 12.4964},
    {"city": "Madrid", "country": "Spain", "latitude": 40.4168, "longitude": -3.7038},
    {"city": "Amsterdam", "country": "Netherlands", "latitude": 52.3676, "longitude": 4.9041},
    {"city": "Vienna", "country": "Austria", "latitude": 48.2082, "longitude": 16.3738}
]

# Utility functions
def ensure_directories():
    """Ensure all required directories exist"""
    for directory in [DATA_RAW_DIR, DATA_PROCESSED_DIR, REPORTS_DIR]:
        os.makedirs(directory, exist_ok=True)

def calculate_comfort_score(temperature, humidity, wind_speed, precipitation):
    """Calculate weather comfort score for tourism (0-100 scale)"""
    try:
        # Temperature score (optimal: 22-28°C)
        if 22 <= temperature <= 28:
            temp_score = 100
        elif 18 <= temperature < 22 or 28 < temperature <= 32:
            temp_score = 80
        elif 15 <= temperature < 18 or 32 < temperature <= 35:
            temp_score = 60
        else:
            temp_score = 40

        # Humidity score (optimal: 40-60%)
        if 40 <= humidity <= 60:
            humidity_score = 100
        elif 30 <= humidity < 40 or 60 < humidity <= 70:
            humidity_score = 80
        else:
            humidity_score = 60

        # Wind score (optimal: 5-15 km/h)
        if 5 <= wind_speed <= 15:
            wind_score = 100
        elif 0 <= wind_speed < 5 or 15 < wind_speed <= 25:
            wind_score = 80
        else:
            wind_score = 60

        # Precipitation score (optimal: 0-2mm)
        if precipitation <= 2:
            precip_score = 100
        elif precipitation <= 5:
            precip_score = 80
        else:
            precip_score = 60

        return round((temp_score * 0.4 + humidity_score * 0.2 + wind_score * 0.2 + precip_score * 0.2), 1)
    except:
        return 50.0

def get_season_from_month(month):
    """Get season from month"""
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Autumn"

# Task Functions
def extract_historical_data(**context):
    """Extract historical weather data for all cities"""
    ensure_directories()

    print("🔄 Starting historical data extraction...")

    # Generate synthetic historical data
    historical_data = []

    for city_config in CITIES_CONFIG:
        city_name = city_config['city']
        country = city_config['country']
        lat = city_config['latitude']
        lon = city_config['longitude']

        # Generate data for 2020-2023
        for year in range(2020, 2024):
            for month in range(1, 13):
                # Generate 5 data points per month for demonstration
                for day in [1, 8, 15, 22, 28]:
                    try:
                        date = datetime(year, month, day)

                        # Generate realistic weather data based on location and season
                        base_temp = 15 if abs(lat) < 40 else 10 if abs(lat) < 60 else 0
                        seasonal_factor = np.sin(2 * np.pi * (month - 3) / 12)
                        temp = base_temp + (15 * seasonal_factor) + np.random.normal(0, 3)

                        humidity = max(20, min(100, 60 + np.random.normal(0, 15)))
                        pressure = 1013 + np.random.normal(0, 20)
                        wind_speed = max(0, np.random.exponential(5))
                        precipitation = max(0, np.random.exponential(2) if np.random.random() < 0.3 else 0)

                        historical_data.append({
                            'city': city_name,
                            'country': country,
                            'datetime': date,
                            'date': date.date(),
                            'temperature': round(temp, 1),
                            'humidity': round(humidity, 1),
                            'pressure': round(pressure, 1),
                            'wind_speed': round(wind_speed, 1),
                            'precipitation': round(precipitation, 2),
                            'latitude': lat,
                            'longitude': lon,
                            'year': year,
                            'month': month,
                            'season': get_season_from_month(month)
                        })
                    except ValueError:
                        continue

    df = pd.DataFrame(historical_data)

    # Save data
    execution_date = context['ds']
    filepath = os.path.join(DATA_RAW_DIR, f"historical_weather_data_{execution_date}.csv")
    df.to_csv(filepath, index=False)

    print(f"✅ Historical data extracted: {len(df)} records")

    # Store metadata
    metadata = {
        'records_count': len(df),
        'cities_count': df['city'].nunique(),
        'filepath': filepath
    }

    context['task_instance'].xcom_push(key='historical_data_metadata', value=metadata)
    return filepath

def clean_and_validate_data(**context):
    """Clean and validate weather data"""
    print("🧹 Starting data cleaning and validation...")

    # Get historical data
    historical_metadata = context['task_instance'].xcom_pull(
        task_ids='extract_historical_data', key='historical_data_metadata'
    )

    if not historical_metadata:
        raise AirflowException("No historical data found")

    # Load and clean data
    df = pd.read_csv(historical_metadata['filepath'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    initial_count = len(df)

    # Basic cleaning
    # Remove extreme outliers
    numeric_columns = ['temperature', 'humidity', 'pressure', 'wind_speed', 'precipitation']
    for col in numeric_columns:
        if col in df.columns:
            Q1 = df[col].quantile(0.05)
            Q3 = df[col].quantile(0.95)
            df = df[(df[col] >= Q1) & (df[col] <= Q3)]

    # Remove duplicates
    df = df.drop_duplicates(subset=['city', 'datetime'])

    # Fill missing values
    df = df.fillna(method='ffill').fillna(method='bfill')

    # Add comfort score
    df['comfort_score'] = df.apply(
        lambda row: calculate_comfort_score(
            row['temperature'], row['humidity'], 
            row['wind_speed'], row['precipitation']
        ), axis=1
    )

    # Add weather categories
    df['weather_category'] = df['comfort_score'].apply(
        lambda x: 'Excellent' if x >= 80 else 'Good' if x >= 60 else 'Fair' if x >= 40 else 'Poor'
    )

    print(f"✅ Data cleaned: {initial_count} -> {len(df)} records")

    # Save cleaned data
    execution_date = context['ds']
    filepath = os.path.join(DATA_PROCESSED_DIR, f"cleaned_weather_data_{execution_date}.csv")
    df.to_csv(filepath, index=False)

    # Quality report
    quality_report = {
        'original_records': initial_count,
        'cleaned_records': len(df),
        'cities_count': df['city'].nunique(),
        'avg_comfort_score': round(df['comfort_score'].mean(), 1),
        'cleaning_timestamp': datetime.now().isoformat()
    }

    print(f"📊 Average comfort score: {quality_report['avg_comfort_score']}")

    # Store metadata
    metadata = {
        'records_count': len(df),
        'cities_count': df['city'].nunique(),
        'filepath': filepath,
        'quality_report': quality_report
    }

    context['task_instance'].xcom_push(key='cleaned_data_metadata', value=metadata)
    return filepath

def calculate_monthly_comfort_scores(**context):
    """Calculate monthly comfort scores for all cities"""
    print("📊 Starting monthly comfort score calculations...")

    # Get cleaned data
    cleaned_metadata = context['task_instance'].xcom_pull(
        task_ids='clean_and_validate_data', key='cleaned_data_metadata'
    )

    if not cleaned_metadata:
        raise AirflowException("No cleaned data found")

    # Load data
    df = pd.read_csv(cleaned_metadata['filepath'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Calculate monthly aggregations
    monthly_scores = df.groupby(['city', 'country', 'year', 'month', 'season']).agg({
        'temperature': ['mean', 'min', 'max'],
        'humidity': 'mean',
        'wind_speed': 'mean',
        'precipitation': ['mean', 'sum'],
        'comfort_score': ['mean', 'min', 'max'],
        'datetime': 'count'
    }).round(2)

    # Flatten column names
    monthly_scores.columns = ['_'.join(col).strip() for col in monthly_scores.columns]
    monthly_scores = monthly_scores.reset_index()

    # Rename columns
    column_mapping = {
        'temperature_mean': 'avg_temperature',
        'temperature_min': 'min_temperature',
        'temperature_max': 'max_temperature',
        'humidity_mean': 'avg_humidity',
        'wind_speed_mean': 'avg_wind_speed',
        'precipitation_mean': 'avg_precipitation',
        'precipitation_sum': 'total_precipitation',
        'comfort_score_mean': 'avg_comfort_score',
        'comfort_score_min': 'min_comfort_score',
        'comfort_score_max': 'max_comfort_score',
        'datetime_count': 'measurement_count'
    }
    monthly_scores.rename(columns=column_mapping, inplace=True)

    # Add month names
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                  5: 'May', 6: 'June', 7: 'July', 8: 'August',
                  9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    monthly_scores['month_name'] = monthly_scores['month'].map(month_names)

    # Calculate tourism score
    monthly_scores['tourism_score'] = (
        monthly_scores['avg_comfort_score'] * 0.7 +
        (100 - monthly_scores['avg_precipitation'].clip(0, 100)) * 0.3
    ).round(1)

    print(f"✅ Monthly scores calculated for {len(monthly_scores)} city-month combinations")

    # Save results
    execution_date = context['ds']
    filepath = os.path.join(DATA_PROCESSED_DIR, f"monthly_comfort_scores_{execution_date}.csv")
    monthly_scores.to_csv(filepath, index=False)

    # Store metadata
    metadata = {
        'records_count': len(monthly_scores),
        'cities_count': monthly_scores['city'].nunique(),
        'filepath': filepath,
        'avg_comfort_score': round(monthly_scores['avg_comfort_score'].mean(), 1)
    }

    context['task_instance'].xcom_push(key='monthly_scores_metadata', value=metadata)
    return filepath

def generate_travel_recommendations(**context):
    """Generate travel recommendations based on comfort scores"""
    print("🎯 Starting travel recommendations generation...")

    # Get monthly scores
    monthly_metadata = context['task_instance'].xcom_pull(
        task_ids='calculate_monthly_comfort_scores', key='monthly_scores_metadata'
    )

    if not monthly_metadata:
        raise AirflowException("No monthly scores found")

    # Load data
    monthly_scores = pd.read_csv(monthly_metadata['filepath'])

    # Find best travel periods for each city (top 6 months)
    best_periods = []

    for city in monthly_scores['city'].unique():
        city_data = monthly_scores[monthly_scores['city'] == city]
        top_months = city_data.nlargest(6, 'tourism_score')

        for rank, (_, row) in enumerate(top_months.iterrows(), 1):
            best_periods.append({
                'city': row['city'],
                'country': row['country'],
                'month': row['month'],
                'month_name': row['month_name'],
                'season': row['season'],
                'avg_temperature': row['avg_temperature'],
                'avg_comfort_score': row['avg_comfort_score'],
                'tourism_score': row['tourism_score'],
                'avg_precipitation': row['avg_precipitation'],
                'recommendation_rank': rank
            })

    best_periods_df = pd.DataFrame(best_periods)

    # Generate city summaries
    city_summaries = monthly_scores.groupby(['city', 'country']).agg({
        'avg_temperature': 'mean',
        'avg_comfort_score': 'mean',
        'tourism_score': 'mean',
        'avg_precipitation': 'mean'
    }).round(2).reset_index()

    city_summaries['climate_rating'] = city_summaries['avg_comfort_score'].apply(
        lambda x: 'Excellent' if x >= 80 else 'Good' if x >= 60 else 'Fair' if x >= 40 else 'Poor'
    )

    print(f"✅ Travel recommendations generated for {len(city_summaries)} cities")

    # Save results
    execution_date = context['ds']

    best_periods_file = os.path.join(DATA_PROCESSED_DIR, f"best_travel_periods_{execution_date}.csv")
    best_periods_df.to_csv(best_periods_file, index=False)

    city_summaries_file = os.path.join(DATA_PROCESSED_DIR, f"city_climate_summaries_{execution_date}.csv")
    city_summaries.to_csv(city_summaries_file, index=False)

    # Store metadata
    metadata = {
        'best_periods_count': len(best_periods_df),
        'cities_analyzed': len(city_summaries),
        'best_periods_file': best_periods_file,
        'city_summaries_file': city_summaries_file
    }

    context['task_instance'].xcom_push(key='recommendations_metadata', value=metadata)
    return metadata

def generate_reports(**context):
    """Generate comprehensive reports and analytics"""
    print("📋 Starting report generation...")

    # Get all metadata
    cleaned_metadata = context['task_instance'].xcom_pull(
        task_ids='clean_and_validate_data', key='cleaned_data_metadata'
    )
    monthly_metadata = context['task_instance'].xcom_pull(
        task_ids='calculate_monthly_comfort_scores', key='monthly_scores_metadata'
    )
    recommendations_metadata = context['task_instance'].xcom_pull(
        task_ids='generate_travel_recommendations', key='recommendations_metadata'
    )

    # Create reports directory
    execution_date = context['ds']
    report_dir = os.path.join(REPORTS_DIR, execution_date)
    os.makedirs(report_dir, exist_ok=True)

    # Load data
    monthly_scores = pd.read_csv(monthly_metadata['filepath'])
    best_periods = pd.read_csv(recommendations_metadata['best_periods_file'])
    city_summaries = pd.read_csv(recommendations_metadata['city_summaries_file'])

    # Generate summary statistics
    summary_stats = {
        'execution_date': execution_date,
        'total_cities_analyzed': len(city_summaries),
        'total_monthly_scores': len(monthly_scores),
        'total_recommendations': len(best_periods),
        'average_comfort_score': round(monthly_scores['avg_comfort_score'].mean(), 1),
        'data_quality': cleaned_metadata['quality_report'],
        'top_destinations': city_summaries.nlargest(10, 'avg_comfort_score')[
            ['city', 'country', 'avg_comfort_score', 'climate_rating']
        ].to_dict('records'),
        'seasonal_analysis': monthly_scores.groupby('season')['avg_comfort_score'].agg(['mean', 'count']).to_dict()
    }

    # Generate seasonal recommendations
    seasonal_recommendations = {}
    for season in ['Spring', 'Summer', 'Autumn', 'Winter']:
        season_data = best_periods[best_periods['season'] == season]
        if not season_data.empty:
            top_season = season_data.nlargest(10, 'tourism_score')
            seasonal_recommendations[season] = top_season[
                ['city', 'country', 'month_name', 'avg_temperature', 'tourism_score']
            ].to_dict('records')

    # Create comprehensive report
    comprehensive_report = {
        'summary_statistics': summary_stats,
        'seasonal_recommendations': seasonal_recommendations,
        'city_rankings': city_summaries.sort_values('avg_comfort_score', ascending=False).to_dict('records'),
        'data_processing_info': {
            'records_processed': cleaned_metadata['records_count'],
            'cities_covered': cleaned_metadata['cities_count'],
            'processing_timestamp': datetime.now().isoformat()
        }
    }

    # Save comprehensive report
    report_file = os.path.join(report_dir, f"comprehensive_report_{execution_date}.json")
    with open(report_file, 'w') as f:
        json.dump(comprehensive_report, f, indent=2, default=str)

    # Generate executive summary
    executive_summary = f"""# Climate Tourism Analysis Report - {execution_date}

## 📊 Executive Summary
- **Cities Analyzed**: {len(city_summaries)}
- **Data Points Processed**: {cleaned_metadata['records_count']:,}
- **Travel Recommendations**: {len(best_periods)}
- **Average Comfort Score**: {summary_stats['average_comfort_score']}/100

## 🏆 Top 5 Destinations
{chr(10).join([f"{i+1}. **{dest['city']}, {dest['country']}**: {dest['avg_comfort_score']:.1f}/100 ({dest['climate_rating']})" 
               for i, dest in enumerate(summary_stats['top_destinations'][:5])])}

## 🌍 Best Travel Seasons
{chr(10).join([f"- **{season}**: {data['mean']:.1f} avg comfort score" 
               for season, data in summary_stats['seasonal_analysis'].items()])}

## 📈 Data Quality
- **Records Processed**: {cleaned_metadata['quality_report']['cleaned_records']:,}
- **Cities Covered**: {cleaned_metadata['quality_report']['cities_count']}
- **Processing Success Rate**: {(cleaned_metadata['quality_report']['cleaned_records'] / cleaned_metadata['quality_report']['original_records'] * 100):.1f}%

## 🎯 Key Insights
1. **Most Comfortable Season**: {max(summary_stats['seasonal_analysis'], key=lambda x: summary_stats['seasonal_analysis'][x]['mean'])}
2. **Best Overall Destination**: {summary_stats['top_destinations'][0]['city']}, {summary_stats['top_destinations'][0]['country']}
3. **Climate Diversity**: {len([d for d in summary_stats['top_destinations'] if d['climate_rating'] == 'Excellent'])} cities with excellent climate ratings

---
*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""

    # Save executive summary
    summary_file = os.path.join(report_dir, f"executive_summary_{execution_date}.md")
    with open(summary_file, 'w') as f:
        f.write(executive_summary)

    print(f"✅ Reports generated successfully in: {report_dir}")

    # Store metadata
    metadata = {
        'report_directory': report_dir,
        'comprehensive_report': report_file,
        'executive_summary': summary_file,
        'generation_timestamp': datetime.now().isoformat()
    }

    context['task_instance'].xcom_push(key='reports_metadata', value=metadata)
    return report_dir

def pipeline_success_notification(**context):
    """Send pipeline success notification"""
    print("🎉 Climate Tourism ETL Pipeline completed successfully!")

    # Get final statistics
    reports_metadata = context['task_instance'].xcom_pull(
        task_ids='generate_reports', key='reports_metadata'
    )

    if reports_metadata:
        print(f"📋 Reports available at: {reports_metadata['report_directory']}")
        print(f"📊 Executive summary: {reports_metadata['executive_summary']}")

    return "Pipeline completed successfully"

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Climate Tourism Analysis ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['climate', 'tourism', 'etl', 'weather']
)

# Define tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_historical_data',
    python_callable=extract_historical_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_and_validate_data',
    python_callable=clean_and_validate_data,
    dag=dag
)

calculate_task = PythonOperator(
    task_id='calculate_monthly_comfort_scores',
    python_callable=calculate_monthly_comfort_scores,
    dag=dag
)

recommend_task = PythonOperator(
    task_id='generate_travel_recommendations',
    python_callable=generate_travel_recommendations,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag
)

success_task = PythonOperator(
    task_id='pipeline_success_notification',
    python_callable=pipeline_success_notification,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> extract_task >> clean_task >> calculate_task >> recommend_task >> report_task >> success_task >> end_task

# Task groups for better organization
with dag:
    # Data Processing Group
    data_processing_group = [extract_task, clean_task]

    # Analytics Group  
    analytics_group = [calculate_task, recommend_task]

    # Reporting Group
    reporting_group = [report_task, success_task]

if __name__ == "__main__":
    dag.test()
