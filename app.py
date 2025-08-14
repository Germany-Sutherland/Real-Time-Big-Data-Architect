import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# ---------------- PAGE SETUP ----------------
st.set_page_config(page_title="🤖 Agentic ELT – Human Brain Style", layout="wide")
st.title("🤖 Agentic ELT – Human Brain Style")
st.caption("Free & Real-Time Big Data Pipeline with Human-Like AI Commentary")

# Auto refresh every 60 seconds (safe for free Streamlit)
st_autorefresh(interval=60000, key="data_refresh")

# ---------------- EXTRACT ----------------
st.subheader("📡 Extract – Getting Real-Time Big Data")
api_url = "https://api.openaq.org/v2/latest?country=IN&limit=10"

try:
    response = requests.get(api_url, timeout=10)
    data_json = response.json()
except Exception as e:
    st.error(f"❌ Error fetching data: {e}")
    st.stop()

# ---------------- TRANSFORM ----------------
st.subheader("🛠 Transform – Cleaning & Structuring Data")
records = []
for r in data_json.get("results", []):
    for m in r.get("measurements", []):
        records.append({
            "City": r.get("city"),
            "Location": r.get("location"),
            "Parameter": m.get("parameter"),
            "Value": m.get("value"),
            "Unit": m.get("unit"),
            "Last Updated": m.get("lastUpdated")
        })

df = pd.DataFrame(records)

if df.empty:
    st.warning("⚠ No data available at the moment. API might be down.")
    st.stop()

df["Last Updated"] = pd.to_datetime(df["Last Updated"])
df.sort_values(by="Last Updated", ascending=False, inplace=True)

# ---------------- LOAD ----------------
st.subheader("📊 Load – Visualizing Real-Time Data")
st.dataframe(df, use_container_width=True)

# ---------------- AGENTIC AI COMMENTARY ----------------
def agentic_reasoning(dataframe):
    """Simulates human-like AI commentary without paid APIs."""
    avg_val = dataframe["Value"].mean()
    most_common = dataframe["Parameter"].mode()[0] if not dataframe["Parameter"].empty else "Unknown"
    latest_time = dataframe["Last Updated"].max().strftime("%Y-%m-%d %H:%M:%S")
    
    if avg_val > 100:
        risk_level = "🚨 Poor – High risk to health"
    elif avg_val > 50:
        risk_level = "⚠ Moderate – Sensitive groups at risk"
    else:
        risk_level = "✅ Good – Safe for most people"
    
    commentary = f"""
    🧠 **Agentic Brain Analysis**:
    - Most common measurement: **{most_common}**
    - Average reading: **{avg_val:.2f}**
    - Data freshness: **{latest_time}**
    - Overall air quality: {risk_level}
    """
    return commentary

st.markdown(agentic_reasoning(df))

# ---------------- HOW TO USE ----------------
st.markdown("---")
st.markdown("### 📖 How to Use This App")
st.markdown("""
1. The app fetches **real-time air quality data** every 60 seconds.  
2. Data comes from the **free, open-source OpenAQ API**.  
3. **ETL Process**:
   - **Extract:** Get live readings from monitoring stations.  
   - **Transform:** Clean & format data for clarity.  
   - **Load:** Display in a clear, scrollable table.  
4. The **Agentic AI Brain** explains the data in human language.  
5. No manual refresh is needed – it's automatic.  
6. Works 100% free on GitHub + Streamlit.  
7. You can edit `api_url` to track a different country or city.  
8. No paid keys or APIs are used – totally open-source.  
9. Great for learning ELT pipelines with AI-like reasoning.  
10. Fully optimized for free-tier Streamlit Cloud usage.  
""")

st.success("✅ Running smoothly on free GitHub + Streamlit without experimental_rerun")
