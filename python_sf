import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(page_title="Snowflake View Explorer", layout="wide")
st.title("🔍 Snowflake View Explorer")

# Read credentials from Streamlit secrets
sf_user = st.secrets["SNOWFLAKE_USER"]
sf_password = st.secrets["SNOWFLAKE_PASSWORD"]
sf_account = st.secrets["SNOWFLAKE_ACCOUNT"]
sf_warehouse = st.secrets["SNOWFLAKE_WAREHOUSE"]
sf_database = st.secrets["SNOWFLAKE_DATABASE"]
sf_schema = st.secrets["SNOWFLAKE_SCHEMA"]

@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

conn = get_conn()
cur = conn.cursor()

def get_views():
    cur.execute(f"""
        SELECT table_name 
        FROM {sf_database}.information_schema.views 
        WHERE table_schema = '{sf_schema}'
        ORDER BY table_name
    """)
    return [row[0] for row in cur.fetchall()]

views = get_views()
selected_view = st.selectbox("📄 Select a View", views)

if selected_view:
    col1, col2 = st.columns([1, 2])

    cur.execute(f"""
        SELECT column_name, data_type 
        FROM {sf_database}.information_schema.columns 
        WHERE table_schema = '{sf_schema}' AND table_name = '{selected_view}'
    """)
    cols = pd.DataFrame(cur.fetchall(), columns=["Column Name", "Data Type"])

    with col1:
        st.subheader("🧱 Columns")
        st.dataframe(cols, use_container_width=True)

    cur.execute(f"SELECT * FROM {sf_schema}.{selected_view} LIMIT 5")
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    sample_json = [dict(zip(headers, row)) for row in rows]

    with col2:
        st.subheader("📦 Sample Data")
        st.json(sample_json)
