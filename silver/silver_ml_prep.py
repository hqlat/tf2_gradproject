# Databricks notebook source
import pandas as pd
import datetime as dt
import pyspark
from pyspark.sql.functions import to_date, col, date_format

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM adb_hibak.silver_tf2_connect;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_connect = _sqldf
df_connect = df_connect.toPandas()

# COMMAND ----------

df_connect.info()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.gold_tf2_game_mode_connections_count;

# COMMAND ----------

# Converting PySpark df to Pandas df
df_connections_count_gm = _sqldf
df_connections_count_gm = df_connections_count_gm.toPandas()

# COMMAND ----------

# Remove time from datetime in eventTime column
df_connect['event_time'] = pd.to_datetime(df_connect['event_time']).dt.date
df_connections_count_gm['event_time'] = pd.to_datetime(df_connections_count_gm['event_time']).dt.date

# COMMAND ----------

# Merge connections and disconnections df
merged_connections_gm = pd.merge(df_connect, df_connections_count_gm, on=['event_time', 'server_id', 'game_mode'], how='outer')

# COMMAND ----------

merged_connections_gm.display()

# COMMAND ----------

merged_connections_gm = merged_connections_gm.drop(['player_id'], axis=1)

# COMMAND ----------

merged_connections_gm.isna().sum()

# COMMAND ----------

# Remove rows with null values
merged_connections_gm.dropna(subset=['map', 'game_mode'], inplace=True)

# COMMAND ----------

merged_connections_gm.isna().sum()

# COMMAND ----------

merged_connections_gm.info()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.gold_tf2_advertisements;

# COMMAND ----------

# Convert pyspark DF to Pandas
df_ads = _sqldf 
df_ads = df_ads.toPandas()

# COMMAND ----------

# Rename columns for consistency before merging the DFs

df_ads = df_ads.rename(columns={'gameType': 'game_type', 'Source': 'source', 'Human': 'human', 'Author': 'author', 'Time': 'time', 'Date': 'date', 'Weekday': 'weekday'})

merged_connections_gm = merged_connections_gm.rename(columns={'event_time': 'date'})

# COMMAND ----------

# Convert 'date' columns to datetime for consistency
merged_connections_gm['date'] = pd.to_datetime(merged_connections_gm['date']).dt.date
df_ads['date'] = pd.to_datetime(df_ads['date']).dt.date

# COMMAND ----------

# Create a set of unique advertisement dates
advertisement_dates = set(df_ads['date'])

# Create a DataFrame with unique advertisement dates
ads_dates = pd.DataFrame(df_ads['date'].unique(), columns=['ad_date'])

# COMMAND ----------

# Merge with merged_connections_gm on 'date'
merged_connections_gm = merged_connections_gm.merge(ads_dates, left_on='date', right_on='ad_date', how='left')

# COMMAND ----------

# Create 'advertisement' column
merged_connections_gm['advertisement'] = merged_connections_gm['ad_date'].notna().astype(int)

# COMMAND ----------

# Drop the 'ad_date' column as it's no longer needed
merged_connections_gm.drop('ad_date', axis=1, inplace=True)

# COMMAND ----------

merged_connections_gm.columns

# COMMAND ----------

# Select desired features

features = ['date', 'server_id', 'map', 'game_mode', 'game_type',
       'connections_count', 'weekday', 'is_weekend',
       'advertisement']

merged_connections_gm = merged_connections_gm[features]

# COMMAND ----------

# Convert following columns into dummy variables
df_encoded = pd.get_dummies(merged_connections_gm, columns=['weekday', 'game_type', 'game_mode'])

# COMMAND ----------

# Change column names for weekday dummy variables
day_mapping = {
    'weekday_0': 'Monday',
    'weekday_1': 'Tuesday',
    'weekday_2': 'Wednesday',
    'weekday_3': 'Thursday',
    'weekday_4': 'Friday',
    'weekday_5': 'Saturday',
    'weekday_6': 'Sunday'}

df_encoded.rename(columns=day_mapping, inplace=True)

# COMMAND ----------

df_encoded.display()

# COMMAND ----------

# Define high server load as being in the top X percentile of connections
threshold = df_encoded['connections_count'].quantile(0.75) # For example, the 75th percentile
df_encoded['high_traffic'] = (df_encoded['connections_count'] >= threshold).astype(int)

# COMMAND ----------

# Drop the original 'connections_count' to avoid leakage
df_encoded.drop('connections_count', axis=1, inplace=True)

# COMMAND ----------

# Convert map column into dummy variables
map_dummies = pd.get_dummies(df_encoded['map'], prefix='map')

# COMMAND ----------

# Join the dummy variables with the original DataFrame
df_encoded = pd.concat([df_encoded, map_dummies], axis=1)

# COMMAND ----------

# Drop the original 'map' column
df_encoded.drop('map', axis=1, inplace=True)

# COMMAND ----------

df_encoded.display()

# COMMAND ----------

df_encoded.info()

# COMMAND ----------

merged_connections_gm.to_csv('df_encoded.csv', index=False)
