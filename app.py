import streamlit as st
import pymysql
import pandas as pd
import time

# MySQL connection function
def get_mysql_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="wony123",
        database="weather_db"
    )

# Function to get the most recent data (aggregatedWeather와 metricsWeather)
def get_latest_weather_data(city):
    conn = get_mysql_connection()
    
    query_aggregated = f"""
    SELECT * FROM aggregatedWeather
    WHERE city = '{city}'
    ORDER BY window_end DESC
    LIMIT 1;
    """
    aggregated_data = pd.read_sql(query_aggregated, conn)
    
    query_metrics = f"""
    SELECT * FROM metricsWeather
    WHERE city = '{city}'
    ORDER BY timestamp DESC
    LIMIT 1;
    """
    metrics_data = pd.read_sql(query_metrics, conn)
    
    conn.close()
    return aggregated_data, metrics_data

# Function to get the most recent data (weatherDetails)
def get_latest_weather_details(city):
    conn = get_mysql_connection()
    
    query_weather_details = f"""
    SELECT * FROM weatherDetails
    WHERE city = '{city}'
    ORDER BY timestamp DESC
    LIMIT 1;
    """
    weather_details_data = pd.read_sql(query_weather_details, conn)
    
    conn.close()
    return weather_details_data

# Streamlit UI configuration
st.set_page_config(page_title="Weather Data Viewer", page_icon="☀️", layout="wide")
st.title("🌤️ Real-Time Weather")

# Custom CSS for font change, layout adjustment, and centering content
st.markdown("""
    <style>
        /* Apply Roboto font for the entire app */
        body {
            font-family: 'Roboto', sans-serif;
        }
        .stMetric {
            font-size: 2em;  /* Larger font size for the numbers */
            font-weight: bold;
            color: #333;  /* Dark color for better visibility */
        }
        .css-18e3th9 {
            max-width: 80%;  /* Adjusts the container width for better centering */
            margin: auto;
        }
        .stExpanderHeader {
            font-size: 1.2em;
            font-weight: bold;
        }
        .stExpander {
            width: 100%;
        }
        .stSelectbox, .stButton {
            width: 50%;  /* Center the select box and button */
            margin-left: auto;
            margin-right: auto;
        }
    </style>
""", unsafe_allow_html=True)

# Select city
city_list = [
    'Bolzano', 'Seoul', 'London', 'New York', 'Paris',
    'Mumbai', 'Sydney', 'Beijing', 'Berlin', 'Moscow',
    'Rio de Janeiro', 'Cape Town', 'Toronto', 'Mexico City', 'Dubai',
    'Singapore', 'Bangkok', 'Istanbul', 'Tokyo', 'Los Angeles'
]
city = st.selectbox("Select a city", city_list, index=0)

if city:
    # Display city name only once at the top
    st.info(f"Fetching the latest weather for **{city}**...")

    # Fetch data
    aggregated_data, metrics_data = get_latest_weather_data(city)
    weather_details_data = get_latest_weather_details(city)

    if not aggregated_data.empty and not metrics_data.empty and not weather_details_data.empty:
        st.subheader(f"Latest Weather in **{city}**")
        st.markdown("---")

        # Display aggregated data (in grid format)
        with st.expander("Aggregated Weather Data", expanded=True):
            st.markdown("#### Key Weather Metrics")
            cols_to_display = [col for col in aggregated_data.columns if col != "id" and col != "city"]
            col1, col2 = st.columns(2)
            
            # Display aggregated data in two columns
            with col1:
                for col in cols_to_display[:len(cols_to_display)//2]:
                    value = aggregated_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest value of {col.replace('_', ' ').capitalize()}",
                    )

            with col2:
                for col in cols_to_display[len(cols_to_display)//2:]:
                    value = aggregated_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest value of {col.replace('_', ' ').capitalize()}",
                    )

        st.markdown("---")

        # Display metrics data (in grid format)
        with st.expander("Metrics Weather Data", expanded=True):
            st.markdown("#### Key Weather Metrics")
            cols_to_display_metrics = [col for col in metrics_data.columns if col != "id" and col != "city"]
            col1, col2 = st.columns(2)
            
            # Display metrics data in two columns
            with col1:
                for col in cols_to_display_metrics[:len(cols_to_display_metrics)//2]:
                    value = metrics_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest metric of {col.replace('_', ' ').capitalize()}",
                    )

            with col2:
                for col in cols_to_display_metrics[len(cols_to_display_metrics)//2:]:
                    value = metrics_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest metric of {col.replace('_', ' ').capitalize()}",
                    )

        st.markdown("---")

        # Display weather details data (in grid format)
        with st.expander("Weather Details", expanded=True):
            st.markdown("#### Latest Weather Details")
            cols_to_display_details = [col for col in weather_details_data.columns if col != "id" and col != "city"]
            col1, col2 = st.columns(2)
            
            # Display weather details in two columns
            with col1:
                for col in cols_to_display_details[:len(cols_to_display_details)//2]:
                    value = weather_details_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest detail of {col.replace('_', ' ').capitalize()}",
                    )

            with col2:
                for col in cols_to_display_details[len(cols_to_display_details)//2:]:
                    value = weather_details_data[col][0]
                    st.metric(
                        label=col.replace('_', ' ').capitalize(),
                        value=f"{value}",
                        delta=None,  # Optional: can show delta change if applicable
                        help=f"Latest detail of {col.replace('_', ' ').capitalize()}",
                    )

    else:
        st.error(f"No data found for {city}.")
    
    # Refresh the page every 3 minutes (180 seconds)
    time.sleep(180)
    st.rerun()
