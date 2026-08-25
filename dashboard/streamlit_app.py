import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from app.config import POSTGRES_URL

st.set_page_config(page_title="Stock Data Pipeline", layout="wide")

engine = create_engine(POSTGRES_URL)

st.title("📊 Stock Data Pipeline Dashboard")

col1, col2 = st.columns(2)

query_total_orders = "SELECT COUNT(*) AS total FROM trusted_stock_orders"
query_total_prices = "SELECT COUNT(*) AS total FROM trusted_stock_prices"

total_orders = pd.read_sql(query_total_orders, engine).iloc[0]["total"]
total_prices = pd.read_sql(query_total_prices, engine).iloc[0]["total"]

col1.metric("Total de Ordens", int(total_orders))
col2.metric("Total de Preços", int(total_prices))

st.subheader("Preço médio por ticker")
query_avg_price = """
SELECT ticker, AVG(price) AS avg_price
FROM trusted_stock_prices
GROUP BY ticker
ORDER BY ticker
"""
df_avg_price = pd.read_sql(query_avg_price, engine)
st.bar_chart(df_avg_price.set_index("ticker"))

st.subheader("Volume total por ticker")
query_volume = """
SELECT ticker, SUM(volume) AS total_volume
FROM trusted_stock_prices
GROUP BY ticker
ORDER BY ticker
"""
df_volume = pd.read_sql(query_volume, engine)
st.bar_chart(df_volume.set_index("ticker"))

st.subheader("Últimas ordens")
query_last_orders = """
SELECT order_id, client_id_masked, ticker, order_type, quantity, price, total_value, event_time
FROM trusted_stock_orders
ORDER BY id DESC
LIMIT 10
"""
df_last_orders = pd.read_sql(query_last_orders, engine)
st.dataframe(df_last_orders, use_container_width=True)

st.subheader("Últimos preços")
query_last_prices = """
SELECT ticker, price, volume, event_time, price_band
FROM trusted_stock_prices
ORDER BY id DESC
LIMIT 10
"""
df_last_prices = pd.read_sql(query_last_prices, engine)
st.dataframe(df_last_prices, use_container_width=True)

