import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Job Market Tracker",
    page_icon="📊",
    layout="wide",
)

# ─── CONNEXION SNOWFLAKE ──────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        database=st.secrets["snowflake"]["database"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        role=st.secrets["snowflake"]["role"],
        schema="PUBLIC",
    )

@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)

# ─── DATA ─────────────────────────────────────────────────────────────────────

df_trend = query("""
    SELECT
        year || '-' || LPAD(month::VARCHAR, 2, '0') AS date,
        job_count,
        rolling_avg_3m
    FROM JOBMARKET.PUBLIC_MARTS.JOB_TREND
    ORDER BY year, month
""")

df_skills_current = query("""
    SELECT skill, SUM(mention_count) AS total
    FROM JOBMARKET.PUBLIC_MARTS.SKILL_TRENDS
    WHERE year  = YEAR(CURRENT_DATE())
      AND month = MONTH(CURRENT_DATE())
    GROUP BY skill
    ORDER BY total DESC
    LIMIT 15
""")

df_skills_time = query("""
    SELECT
        year || '-' || LPAD(month::VARCHAR, 2, '0') AS date,
        skill,
        SUM(mention_count) AS mention_count
    FROM JOBMARKET.PUBLIC_MARTS.SKILL_TRENDS
    WHERE skill IN (
        SELECT skill FROM (
            SELECT skill, SUM(mention_count) AS total
            FROM JOBMARKET.PUBLIC_MARTS.SKILL_TRENDS
            GROUP BY skill
            ORDER BY total DESC
            LIMIT 5
        )
    )
    GROUP BY date, skill
    ORDER BY date
""")

df_companies = query("""
    SELECT company, COUNT(*) AS offers
    FROM JOBMARKET.PUBLIC_STAGING.STG_JOBS
    WHERE company IS NOT NULL AND company != ''
    GROUP BY company
    ORDER BY offers DESC
    LIMIT 10
""")

df_cities = query("""
    SELECT location, COUNT(*) AS offers
    FROM JOBMARKET.PUBLIC_STAGING.STG_JOBS
    WHERE location IS NOT NULL AND location != ''
    GROUP BY location
    ORDER BY offers DESC
    LIMIT 10
""")

df_contracts = query("""
    SELECT
        COALESCE(NULLIF(contract_type, ''), 'Non précisé') AS contract_type,
        COUNT(*) AS total
    FROM JOBMARKET.PUBLIC_STAGING.STG_JOBS
    GROUP BY contract_type
    ORDER BY total DESC
""")

df_total = query("""
    SELECT COUNT(*) AS total FROM JOBMARKET.PUBLIC_STAGING.STG_JOBS
""")

# ─── PLOTLY THEME ─────────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor="white",
    plot_bgcolor="#f8f9fa",
    font=dict(color="#212529", family="sans-serif", size=13),
    margin=dict(t=40, b=40, l=10, r=10),
    xaxis=dict(gridcolor="#dee2e6", linecolor="#adb5bd"),
    yaxis=dict(gridcolor="#dee2e6", linecolor="#adb5bd"),
)

COLORS = ["#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd", "#d62728"]

# ─── HEADER ───────────────────────────────────────────────────────────────────

st.title("📊 Job Market Tracker")
st.caption(f"Data Engineering jobs in France — last updated {datetime.now().strftime('%d %b %Y')}")
st.divider()

# ─── KPI CARDS ────────────────────────────────────────────────────────────────

total_offers = int(df_total["TOTAL"].iloc[0])
top_skill    = df_skills_current["SKILL"].iloc[0] if not df_skills_current.empty else "N/A"
top_city     = df_cities["LOCATION"].iloc[0].split(",")[0] if not df_cities.empty else "N/A"
top_company  = df_companies["COMPANY"].iloc[0] if not df_companies.empty else "N/A"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Offers", f"{total_offers:,}")
col2.metric("Top Skill This Month", top_skill)
col3.metric("Top City", top_city)
col4.metric("Top Company", top_company)

st.divider()

# ─── SECTION 1 : VUE GLOBALE ─────────────────────────────────────────────────

st.subheader("Market Overview")

col_left, col_right = st.columns([2, 1])

with col_left:
    if not df_trend.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_trend["DATE"],
            y=df_trend["JOB_COUNT"],
            name="Monthly offers",
            marker_color="#1f77b4",
            opacity=0.7,
        ))
        fig.add_trace(go.Scatter(
            x=df_trend["DATE"],
            y=df_trend["ROLLING_AVG_3M"],
            name="3-month rolling avg",
            line=dict(color="#d62728", width=2.5),
            mode="lines",
        ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Job Offers per Month",
            legend=dict(bgcolor="white", bordercolor="#dee2e6", borderwidth=1),
        )
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    if not df_contracts.empty:
        fig2 = px.pie(
            df_contracts,
            values="TOTAL",
            names="CONTRACT_TYPE",
            color_discrete_sequence=COLORS,
            hole=0.4,
            title="Contract Types",
        )
        fig2.update_layout(
            paper_bgcolor="white",
            font=dict(color="#212529", size=13),
            margin=dict(t=40, b=20, l=10, r=10),
            legend=dict(bgcolor="white"),
        )
        fig2.update_traces(textinfo="percent+label", textfont_size=12)
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─── SECTION 2 : COMPÉTENCES ─────────────────────────────────────────────────

st.subheader(" Skills Analysis")

col_s1, col_s2 = st.columns([1, 2])

with col_s1:
    if not df_skills_current.empty:
        fig3 = px.bar(
            df_skills_current.sort_values("TOTAL"),
            x="TOTAL",
            y="SKILL",
            orientation="h",
            title="Top 15 Skills This Month",
        )
        fig3.update_traces(marker_color="#1f77b4")
        fig3.update_layout(**PLOTLY_LAYOUT)
        fig3.update_yaxes(tickfont=dict(size=11))
        st.plotly_chart(fig3, use_container_width=True)

with col_s2:
    if not df_skills_time.empty:
        fig4 = px.line(
            df_skills_time,
            x="DATE",
            y="MENTION_COUNT",
            color="SKILL",
            title="Top 5 Skills Over Time",
            color_discrete_sequence=COLORS,
            markers=True,
        )
        fig4.update_layout(
            **PLOTLY_LAYOUT,
            legend=dict(bgcolor="white", bordercolor="#dee2e6", borderwidth=1),
        )
        st.plotly_chart(fig4, use_container_width=True)

st.divider()

# ─── SECTION 3 : MARCHÉ ───────────────────────────────────────────────────────

st.subheader("Market")

col_m1, col_m2 = st.columns(2)

with col_m1:
    if not df_companies.empty:
        fig5 = px.bar(
            df_companies.sort_values("OFFERS"),
            x="OFFERS",
            y="COMPANY",
            orientation="h",
            title="Top 10 Companies Hiring",
        )
        fig5.update_traces(marker_color="#9467bd")
        fig5.update_layout(**PLOTLY_LAYOUT)
        fig5.update_yaxes(tickfont=dict(size=11))
        st.plotly_chart(fig5, use_container_width=True)

with col_m2:
    if not df_cities.empty:
        fig6 = px.bar(
            df_cities.sort_values("OFFERS"),
            x="OFFERS",
            y="LOCATION",
            orientation="h",
            title="Top 10 Cities Hiring",
        )
        fig6.update_traces(marker_color="#2ca02c")
        fig6.update_layout(**PLOTLY_LAYOUT)
        fig6.update_yaxes(tickfont=dict(size=11))
        st.plotly_chart(fig6, use_container_width=True)

st.divider()
st.caption("Built by Oumayma Mhamdi")