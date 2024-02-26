# Databricks notebook source
import pyspark
import pandas as pd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_hlstats_events_chat;

# COMMAND ----------

df_chat = _sqldf
df_chat = df_chat.toPandas()

# COMMAND ----------

df_chat.columns

# COMMAND ----------

features = ['eventTime', 'serverId', 'map', 'playerId','message_mode','message']

df_chat = df_chat[features]

# COMMAND ----------

df_chat.display()

# COMMAND ----------

# Removing prefix workshop/ and .ugc in map column
df_chat['map'] = df_chat['map'].str.replace(r'workshop\/(.+?)\.ugc.*', r'\1', regex=True)

# COMMAND ----------

df_chat.groupby('map').size()

# COMMAND ----------

# Create a dictionary with the fullnames and prefix of Game Modes in TF2.

prefix_to_fullname = {
    'arena_': 'arena',
    'ad_': 'attack_defense',
    'ctf_': 'capture_the_flag',
    'cp_': 'control_point',
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

# Change the prefix in map to full names for Gamemode for df_chat.
for prefix, fullname in prefix_to_fullname.items():
    df_chat['map'] = df_chat['map'].str.replace(f'^{prefix}', f'{fullname}.', regex=True)

# COMMAND ----------

# Remove all rows with empty values in map.
df_chat = df_chat[df_chat['map'] != '']

# COMMAND ----------

df_chat.groupby('map').size()

# COMMAND ----------

# Convert the pandas DF to a spark DF
spark_df_chat = spark.createDataFrame(df_chat)
spark_df_chat.display()

# COMMAND ----------

# Convert the spark DF to a table in the db.
spark_df_chat.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_chat")