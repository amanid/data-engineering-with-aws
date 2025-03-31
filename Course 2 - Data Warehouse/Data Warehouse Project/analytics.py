import configparser
import psycopg2
import pandas as pd
import streamlit as st
import time
import plotly.express as px

# Read configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Streamlit settings
st.set_page_config(page_title="Sparkify Analytics Dashboard", layout="wide")

# Login credentials (default: admin/admin)
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "admin"


def authenticate_user():
    """
    Authenticate user using Streamlit text inputs.
    """
    st.sidebar.title("🔒 Login")
    username = st.sidebar.text_input("Username", value="", placeholder="Enter username")
    password = st.sidebar.text_input("Password", value="", type="password", placeholder="Enter password")

    if st.sidebar.button("Login"):
        if username == DEFAULT_USERNAME and password == DEFAULT_PASSWORD:
            st.session_state["authenticated"] = True
            st.sidebar.success("Login successful!")
        else:
            st.sidebar.error("Invalid credentials.")

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    return st.session_state["authenticated"]


# Helper function to connect to Redshift
def connect_to_redshift():
    """
    Connect to the Redshift database using configuration from dwh.cfg.
    Returns:
        conn: psycopg2 connection object.
    """
    try:
        conn = psycopg2.connect(
            f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} "
            f"user={config['CLUSTER']['DB_USER']} password={config['CLUSTER']['DB_PASSWORD']} "
            f"port={config['CLUSTER']['DB_PORT']}"
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Redshift: {e}")
        raise


# Fetch query results as a Pandas DataFrame
def run_query(query):
    """
    Execute a SQL query and return the results as a Pandas DataFrame.
    Args:
        query (str): SQL query to execute.
    Returns:
        pd.DataFrame: Query results.
    """
    conn = connect_to_redshift()
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# Define advanced analytics queries
analytics_queries = {
    "Top 10 Most Played Songs": """
        SELECT s.title AS song_title, COUNT(sp.songplay_id) AS play_count
        FROM songplays sp
        JOIN songs s ON sp.song_id = s.song_id
        GROUP BY s.title
        ORDER BY play_count DESC
        LIMIT 10;
    """,
    "User Activity by Subscription Level": """
        SELECT u.level, COUNT(sp.songplay_id) AS activity_count
        FROM songplays sp
        JOIN users u ON sp.user_id = u.user_id
        GROUP BY u.level
        ORDER BY activity_count DESC;
    """,
    "User Activity by Day of the Week": """
        SELECT t.weekday, COUNT(sp.songplay_id) AS activity_count
        FROM songplays sp
        JOIN time t ON sp.start_time = t.start_time
        GROUP BY t.weekday
        ORDER BY activity_count DESC;
    """,
    "Top 10 Most Popular Artists": """
        SELECT a.name AS artist_name, COUNT(sp.songplay_id) AS play_count
        FROM songplays sp
        JOIN artists a ON sp.artist_id = a.artist_id
        GROUP BY a.name
        ORDER BY play_count DESC
        LIMIT 10;
    """,
    "User Activity Trends by Year": """
        SELECT t.year, COUNT(sp.songplay_id) AS activity_count
        FROM songplays sp
        JOIN time t ON sp.start_time = t.start_time
        GROUP BY t.year
        ORDER BY t.year;
    """
}


# Display queries with charts and tables
def display_analytics():
    """
    Execute and display analytics queries with tables and advanced visualizations.
    """
    for title, query in analytics_queries.items():
        st.subheader(title)

        # Fetch and display the data
        data = run_query(query)

        if not data.empty:
            # Render data table
            with st.expander(f"View raw data for {title}"):
                st.dataframe(data)

            # Advanced visualization with Plotly
            if "play_count" in data.columns:
                fig = px.bar(
                    data,
                    x=data.columns[0],
                    y="play_count",
                    title=title,
                    labels={"play_count": "Play Count", data.columns[0]: ""},
                    text_auto=True
                )
                st.plotly_chart(fig, use_container_width=True)

            elif "activity_count" in data.columns:
                fig = px.line(
                    data,
                    x=data.columns[0],
                    y="activity_count",
                    title=title,
                    labels={"activity_count": "Activity Count", data.columns[0]: ""},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning(f"No data available for {title}.")


# Auto-refresh the dashboard
def main():
    """
    Main function to render the Streamlit dashboard with advanced features.
    """
    if authenticate_user():
        st.sidebar.title("Dashboard Settings")
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=10)
        selected_query = st.sidebar.selectbox(
            "Select Query to Display", options=["All"] + list(analytics_queries.keys()), index=0
        )

        # Display the selected query or all
        if selected_query == "All":
            display_analytics()
        else:
            st.subheader(selected_query)
            data = run_query(analytics_queries[selected_query])
            if not data.empty:
                st.dataframe(data)
                if "play_count" in data.columns:
                    st.bar_chart(data.set_index(data.columns[0])["play_count"])
                elif "activity_count" in data.columns:
                    st.line_chart(data.set_index(data.columns[0])["activity_count"])
            else:
                st.warning(f"No data available for {selected_query}.")

        # Auto-refresh functionality
        st.sidebar.markdown("---")
        st.sidebar.info("The dashboard refreshes every interval set above.")
        time.sleep(refresh_interval)
        st.experimental_rerun()


if __name__ == "__main__":
    main()
