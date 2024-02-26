# Databricks notebook source
import pandas as pd
import pyspark

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_hlstats_events_connects;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_connect = _sqldf
df_connect = df_connect.toPandas()

# COMMAND ----------

features = ['eventTime', 'serverId', 'map', 'playerId']

df_connect = df_connect[features]

# COMMAND ----------

# Remove all rows with empty values in map.
df_connect = df_connect[df_connect['map'] != '']

# COMMAND ----------

# removing prefix workshop/ and .ugc in map
df_connect['map'] = df_connect['map'].str.replace(r'workshop\/(.+?)\.ugc.*', r'\1', regex=True)

# COMMAND ----------

df_connect.display()

# COMMAND ----------

prefix_to_fullname = {
    'arena_': 'arena',
    'ad_': 'attack_defense',
    'ctf_': 'capture_the_flag',
    'cp_': 'control_point',
    'cp': 'control_point',
    'koth_': 'king_of_the_hill',
    'mvm_': 'mann_vs_machine',
    'pass_': 'pass_time',
    'pl_': 'payload',
    'plr_': 'payload_race',
    'pd_': 'player_destruction',
    'rd_': 'robot_destruction',
    'sd_': 'special_delivery',
    'tc_': 'territorial_control',
    'tr_': 'training_mode',
    'vsh_': 'versus_saxton_hale',
    'zi_': 'zombie_infection'
}

# COMMAND ----------

# Function to map prefix to full game mode name
def get_fullname(map_name):
    for prefix, fullname in prefix_to_fullname.items():
        if map_name.startswith(prefix):
            return fullname
    return "Unknown"  # Return "Unknown" or any default value if no prefix matches

# COMMAND ----------

# Apply this function to the 'map' column to create a new 'game_mode' column
df_connect['game_mode'] = df_connect['map'].apply(get_fullname)

# COMMAND ----------

# Change the prefix in map to full names for Gamemode for connect.
for prefix, fullname in prefix_to_fullname.items():
    df_connect['map'] = df_connect['map'].str.replace(f'^{prefix}', f'{fullname}.', regex=True)

# COMMAND ----------

df_connect.display()

# COMMAND ----------

# Removing rows where the game_mode is unknown.
df_connect = df_connect.drop(df_connect[df_connect['game_mode']== 'Unknown'].index)

# COMMAND ----------

df_connect['map'] = df_connect['map'].str.replace(r'_rc\w*', '', regex=True)
df_connect['map'] = df_connect['map'].str.replace(r'_b\w*', '', regex=True)
df_connect['map'] = df_connect['map'].str.replace(r'_a\w*', '', regex=True)

# COMMAND ----------

df_connect.display()

# COMMAND ----------

df_connect.groupby('game_mode').size()

# COMMAND ----------

df_connect.columns

# COMMAND ----------

df_connect = df_connect.rename(columns={'eventTime': 'event_time', 'serverId': 'server_id', 'playerId': 'player_id'})

# COMMAND ----------

# Define a dictionary to map server_id to game type
server_to_game_type = {4: 'BMOD', 6: 'BMOD', 11: 'MM', 32: 'BMOD', 35: 'MM'}

# COMMAND ----------

# Map the server_id to game type and create a new 'game_type' column
df_connect['game_type'] = df_connect['server_id'].map(server_to_game_type)

# COMMAND ----------

df_connect.display()

# COMMAND ----------

spark_df_connect = spark.createDataFrame(df_connect)
spark_df_connect.display()

# COMMAND ----------

spark_df_connect.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_connect")
