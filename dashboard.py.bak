
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# Connect to SQLite database
conn = sqlite3.connect('ecommerce.db')

# Query monthly revenue
query = '''
SELECT strftime('%Y-%m', OrderDate) AS Month, SUM(Sales) AS Revenue
FROM SalesData
GROUP BY Month
ORDER BY Month;
'''
df = pd.read_sql_query(query, conn)

# Streamlit app
st.title("E-Commerce Sales Analysis Dashboard")

# Monthly Revenue Chart
st.subheader("Monthly Revenue")
fig = px.line(df, x='Month', y='Revenue', title='Monthly Revenue Trend')
st.plotly_chart(fig)

# Close connection
conn.close()
