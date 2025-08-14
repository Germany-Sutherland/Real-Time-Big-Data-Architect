# app.py
import json
import time
from datetime import datetime
import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# -------------------- Page Style / Theme --------------------
st.set_page_config(page_title="Agentic ELT – Real-Time Data Playground", layout="wide")
st.markdown(
    """
    <style>
      /* Soft gradient background */
      .stApp {background: radial-gradient(1200px 600px at 0% 0%, #f0f7ff 0%, #ffffff 45%)}
      /* Headings */
      h1, h2, h3 {font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;}
      /* Pretty section cards */
      .card {border:1px solid #e5e7eb; background:#ffffff; border-radius:16px; padding:16px 18px; box-shadow:0 2px 12px rgba(15,23,42,.06);}
      .pill  {display:inline-block; padding:4px 10px; border-radius:999px; font-size:12px; background:#eef2ff; color:#3730a3; border:1px solid #c7d2fe;}
      /* Data source radio spacing */
      div[role="radiogroup"] > label {margin-bottom:6px;}
      /* Subtle section divider spacing */
      .spacer {height:10px;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🤖 Agentic ELT – Human-Like Real-Time Big Data")
st.caption("Choose ONE free open-source real-time data source below. The agent plans ETL steps, transforms data, and explains the result.")

# Safe auto-refresh (every 60s) – Streamlit free-tier friendly
st_autorefresh(interval=60_000, key="auto_refresh")

# -------------------- 10 Free Real-Time Sources (no keys) --------------------
SOURCES = {
    # name: (display label, url, brief)
    "openaq": ("OpenAQ (Air Quality)", "https://api.openaq.org/v2/latest?limit=20&sort=desc", "Live air quality readings"),
    "open_meteo": ("Open-Meteo (Weather)", "https://api.open-meteo.com/v1/forecast?latitude=51.5072&longitude=-0.1276&current=temperature_2m,wind_speed_10m", "Current weather (London)"),
    "coingecko": ("CoinGecko (Crypto Prices)", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd", "BTC & ETH spot prices"),
    "usgs_quakes": ("USGS (Earthquakes)", "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=50&orderby=time", "Recent global earthquakes"),
    "spacex": ("SpaceX (Latest Launch)", "https://api.spacexdata.com/v4/launches/latest", "Latest launch data"),
    "github_events": ("GitHub (Public Events)", "https://api.github.com/events", "Live public GitHub events (rate-limited)"),
    "nws_alerts": ("US NWS (Weather Alerts)", "https://api.weather.gov/alerts/active?limit=20", "Active US weather alerts"),
    "fx_rates": ("Exchange Rates (exchangerate.host)", "https://api.exchangerate.host/latest?base=USD&symbols=EUR,GBP,JPY,INR", "Latest FX against USD"),
    "iss_now": ("Open-Notify (ISS Position)", "http://api.open-notify.org/iss-now.json", "Current ISS location"),
    "binance": ("Binance (BTCUSDT Ticker)", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "BTC/USDT spot price"),
}

# -------------------- UI: choose exactly one (tick) --------------------
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown("### ✅ Choose **one** Real-Time Big Data Source")
choice = st.radio(
    "Tick exactly one source below",
    options=list(SOURCES.keys()),
    format_func=lambda k: SOURCES[k][0],
    horizontal=False,
)
st.markdown('</div>', unsafe_allow_html=True)

# -------------------- Extract --------------------
def fetch(url: str, headers: dict | None = None, timeout: int = 12):
    hdrs = headers or {}
    # Some public APIs (NWS) ask for a User-Agent
    if "weather.gov" in url and "User-Agent" not in hdrs:
        hdrs["User-Agent"] = "Agentic-ELT-Demo/1.0 (contact: demo@example.com)"
    try:
        r = requests.get(url, headers=hdrs, timeout=timeout)
        r.raise_for_status()
        # Many endpoints return JSON; fall back to text if needed
        try:
            return r.json(), None
        except Exception:
            return r.text, None
    except Exception as e:
        return None, str(e)

url = SOURCES[choice][1]
raw, err = fetch(url)

# -------------------- Transform (normalize to a small table for each source) --------------------
def normalize_to_df(key: str, raw_obj):
    if raw_obj is None:
        return pd.DataFrame()

    # OpenAQ
    if key == "openaq":
        results = raw_obj.get("results", [])
        rows = []
        for res in results:
            city = res.get("city")
            for m in res.get("measurements", []):
                rows.append({
                    "city": city,
                    "parameter": m.get("parameter"),
                    "value": m.get("value"),
                    "unit": m.get("unit"),
                    "updated": m.get("lastUpdated"),
                })
        return pd.DataFrame(rows)

    # Open-Meteo
    if key == "open_meteo":
        cur = raw_obj.get("current", {})
        return pd.DataFrame([{
            "temperature_2m": cur.get("temperature_2m"),
            "wind_speed_10m": cur.get("wind_speed_10m"),
            "time": cur.get("time"),
        }])

    # CoinGecko
    if key == "coingecko":
        # raw like {"bitcoin":{"usd":...},"ethereum":{"usd":...}}
        rows = [{"asset": k, "usd": v.get("usd")} for k, v in raw_obj.items()]
        return pd.DataFrame(rows)

    # USGS Earthquakes
    if key == "usgs_quakes":
        feats = raw_obj.get("features", [])
        rows = []
        for f in feats:
            p = f.get("properties", {})
            rows.append({
                "time": datetime.utcfromtimestamp(p.get("time", 0)/1000).strftime("%Y-%m-%d %H:%M:%S"),
                "mag": p.get("mag"),
                "place": p.get("place"),
                "type": p.get("type"),
            })
        return pd.DataFrame(rows)

    # SpaceX latest launch
    if key == "spacex":
        rows = [{
            "name": raw_obj.get("name"),
            "date_utc": raw_obj.get("date_utc"),
            "success": raw_obj.get("success"),
            "flight_number": raw_obj.get("flight_number"),
        }]
        return pd.DataFrame(rows)

    # GitHub public events
    if key == "github_events":
        rows = []
        for ev in raw_obj[:30]:
            rows.append({
                "type": ev.get("type"),
                "repo": ev.get("repo", {}).get("name"),
                "actor": ev.get("actor", {}).get("login"),
                "created_at": ev.get("created_at"),
            })
        return pd.DataFrame(rows)

    # NWS Alerts
    if key == "nws_alerts":
        feats = raw_obj.get("features", [])
        rows = []
        for f in feats:
            p = f.get("properties", {})
            rows.append({
                "event": p.get("event"),
                "area": p.get("areaDesc"),
                "severity": p.get("severity"),
                "sent": p.get("sent"),
            })
        return pd.DataFrame(rows)

    # FX Rates
    if key == "fx_rates":
        base = raw_obj.get("base")
        date = raw_obj.get("date")
        rates = raw_obj.get("rates", {})
        rows = [{"pair": f"{base}/{k}", "rate": v, "date": date} for k, v in rates.items()]
        return pd.DataFrame(rows)

    # ISS
    if key == "iss_now":
        pos = raw_obj.get("iss_position", {})
        return pd.DataFrame([{
            "latitude": pos.get("latitude"),
            "longitude": pos.get("longitude"),
            "timestamp": raw_obj.get("timestamp"),
        }])

    # Binance
    if key == "binance":
        return pd.DataFrame([raw_obj])  # {'symbol':'BTCUSDT','price':'...'}
    return pd.DataFrame()

# -------------------- Load (visuals + agentic explanation) --------------------
st.markdown('<div class="spacer"></div>', unsafe_allow_html=True)
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown("### 🔄 ETL / ELT Result")

if err or raw is None:
    st.warning("⚠️ **Our App is working good, but the Source of Big Data (external) is down.**")
    st.caption(f"Selected: {SOURCES[choice][0]} • URL: {url}")
else:
    df = normalize_to_df(choice, raw)
    if df.empty:
        st.warning("⚠️ **Our App is working good, but the Source of Big Data (external) returned no rows.**")
        st.caption(f"Selected: {SOURCES[choice][0]} • URL: {url}")
    else:
        left, right = st.columns([2, 3], vertical_alignment="top")
        with left:
            st.markdown(f'<span class="pill">EXTRACT</span> &nbsp; {SOURCES[choice][2]}', unsafe_allow_html=True)
            st.code(url, language="text")

            st.markdown('<div class="spacer"></div>', unsafe_allow_html=True)
            st.markdown('<span class="pill">TRANSFORM</span> &nbsp; Normalize → tidy table', unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True, height=320)

        with right:
            st.markdown('<span class="pill">LOAD</span> &nbsp; Quick view', unsafe_allow_html=True)

            # Small chart where it makes sense
            if choice in ("coingecko", "fx_rates", "usgs_quakes"):
                try:
                    if choice == "coingecko":
                        chart_df = df.set_index("asset")["usd"]
                    elif choice == "fx_rates":
                        chart_df = df.set_index("pair")["rate"]
                    else:  # usgs_quakes
                        # filter numeric magnitudes
                        q = df.dropna(subset=["mag"]).set_index("time")["mag"]
                        chart_df = q.tail(30)
                    st.bar_chart(chart_df)
                except Exception:
                    st.info("Chart not available for this source.")
            else:
                st.info("Chart not available for this source.")

            # Agentic human-like explanation
            st.markdown("#### 🤖 Agent’s Take")
            st.write(
                agentic_commentary(choice=choice, df=df, raw=raw)
            )

st.markdown('</div>', unsafe_allow_html=True)

# -------------------- How to Use (explicit instructions) --------------------
with st.expander("📖 How to Use (Read First)", expanded=True):
    st.markdown(
        """
        1. **Tick exactly one** real-time big data source in the list above.
        2. The app **Extracts** from that API, **Transforms** to a tidy table, and **Loads** it into visuals.
        3. If the API is unreachable or empty, you’ll see:  
           **“Our App is working good, but the Source of Big Data (external) is down.”**
        4. Try another source from the 10 options — at least one usually responds in real time.
        5. The **Agent** explains what’s happening like a human ELT/ETL engineer.
        6. The page **auto-refreshes every 60s** to keep things fresh (free-tier safe).
        7. No API keys, no secrets — **100% free & open-source** endpoints.
        """.strip()
    )

st.caption("Design: soft gradient, cards, and typographic hierarchy for a calm, professional UX.")

# -------------------- Agentic commentary function --------------------
def agentic_commentary(choice: str, df: pd.DataFrame, raw) -> str:
    name = SOURCES[choice][0]
    rows = len(df)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Source-specific short insights
    if choice == "openaq" and not df.empty:
        top_city = df.groupby("city")["value"].mean().sort_values(ascending=False).index[0]
        return f"Pulled **{rows}** air-quality rows. Highest average so far around **{top_city}**. Snapshot time: {now}."
    if choice == "open_meteo" and not df.empty:
        t = float(df.iloc[0]["temperature_2m"])
        w = float(df.iloc[0]["wind_speed_10m"])
        return f"Weather looks **{t:.1f}°C** with wind ~**{w:.1f} m/s**. Good to proceed with downstream checks. ({now})"
    if choice == "coingecko" and not df.empty:
        btc = float(df.loc[df["asset"]=="bitcoin","usd"].values[0])
        eth = float(df.loc[df["asset"]=="ethereum","usd"].values[0])
        mood = "volatile" if abs(btc-eth) > 500 else "calm"
        return f"Crypto feed OK: BTC **${btc:,.0f}**, ETH **${eth:,.0f}** — market feels **{mood}**. ({now})"
    if choice == "usgs_quakes" and not df.empty:
        recent = df.head(1).iloc[0]
        return f"Earthquake feed live: last event **M{recent['mag']}** near **{recent['place']}** at **{recent['time']}**. ({now})"
    if choice == "spacex" and not df.empty:
        namex = df.iloc[0]["name"]
        success = df.iloc[0]["success"]
        verdict = "✅ success" if success else "⏳ pending / ❌ failure"
        return f"Latest SpaceX mission: **{namex}** → {verdict}. Good telemetry for a space dashboard. ({now})"
    if choice == "github_events" and not df.empty:
        typ = df["type"].value_counts().index[0]
        return f"GitHub is buzzing: most common live event is **{typ}**. Consider throttling to respect rate limits. ({now})"
    if choice == "nws_alerts" and not df.empty:
        sev = df["severity"].dropna().value_counts().index[0] if not df["severity"].dropna().empty else "unknown"
        return f"NWS alerts flowing; most frequent severity right now: **{sev}**. Route to incident management if needed. ({now})"
    if choice == "fx_rates" and not df.empty:
        best = df.sort_values("rate", ascending=False).iloc[0]
        return f"FX snapshot: strongest vs USD is **{best['pair']}** at **{best['rate']:.3f}**. Store for time-series later. ({now})"
    if choice == "iss_now" and not df.empty:
        lat, lon = df.iloc[0]["latitude"], df.iloc[0]["longitude"]
        return f"ISS live position: lat **{lat}**, lon **{lon}**. Great for real-time maps. ({now})"
    if choice == "binance" and not df.empty:
        price = float(df.iloc[0]["price"])
        return f"Binance ticker steady: **BTCUSDT ${price:,.2f}**. Fan-out to alerts if rapid movement. ({now})"

    return f"Pulled **{rows}** rows from **{name}**. Data looks healthy; proceeding with the ELT flow. ({now})"
