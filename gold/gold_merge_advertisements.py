# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adb_hibak.silver_tf2_discord_events;

# COMMAND ----------

discord_events_df = _sqldf
discord_events_df = discord_events_df.toPandas()

# COMMAND ----------

discord_events_df.loc[discord_events_df['Channel'] == 'lets-play-bmod', 'gameType'] = 'BMOD'
discord_events_df.loc[discord_events_df['Channel'] == 'lets-play-mm', 'gameType'] = 'MM'

# COMMAND ----------

discord_events_df = discord_events_df[discord_events_df['Author'] != 'Statera']
discord_events_df["Source"] = "Discord Let's Play Channel"
discord_events_df["Human"] = 1

# COMMAND ----------


features = ["Author","Timestamp","gameType","Source","Human"]

# COMMAND ----------

discord_events_df.display()

# COMMAND ----------

discord_events_df_reduced = discord_events_df[features]
discord_events_df_reduced.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adb_hibak.silver_tf2_discord_serverseeder;

# COMMAND ----------

discord_server_seeder_df = _sqldf
discord_server_seeder_df = discord_server_seeder_df.toPandas()

# COMMAND ----------

discord_server_seeder_df.display()

# COMMAND ----------

discord_server_seeder_df["Source"] = "Discord Server Seeding Channel"
discord_server_seeder_df.loc[discord_server_seeder_df['server_seeder'] != 'Statera', 'Human'] = 1
discord_server_seeder_df.loc[discord_server_seeder_df['server_seeder'] == 'Statera', 'Human'] = 0


# COMMAND ----------

discord_server_seeder_df["Author"] = discord_server_seeder_df["server_seeder"]
discord_server_seeder_df["Timestamp"] = discord_server_seeder_df["timestamp"]
discord_server_seeder_df["gameType"] = discord_server_seeder_df["serverType"]
discord_server_seeder_df.display()

# COMMAND ----------

discord_server_seeder_df_reduced = discord_server_seeder_df[features]
discord_server_seeder_df_reduced.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adb_hibak.silver_tf2_group_organized_events;

# COMMAND ----------

group_event_df = _sqldf
group_event_df = group_event_df.toPandas()


# COMMAND ----------

group_event_df["Source"] = "Steamgroup Event"
group_event_df["Human"] = 1
group_event_df["Author"] = "Steamgroup"

# COMMAND ----------

group_event_df.display()

# COMMAND ----------

discord_events_df_reduced.display()

# COMMAND ----------

discord_server_seeder_df_reduced.display()

# COMMAND ----------

#Fix the datatypes for the union
discord_server_seeder_df_reduced['Human'] = discord_server_seeder_df_reduced['Human'].astype(int)
discord_events_df_reduced['Timestamp'] = pd.to_datetime(discord_events_df_reduced['Timestamp'])
discord_server_seeder_df_reduced['Timestamp'] = pd.to_datetime(discord_server_seeder_df_reduced['Timestamp'])
# print(discord_events_df_reduced['Timestamp'])

# print(discord_events_df_reduced['Timestamp'])
print("Group Events:\n",group_event_df.dtypes)
print("---")
print("Discord Events:\n",discord_events_df_reduced.dtypes)
print("---")
print("Server Seeder:\n",discord_server_seeder_df_reduced.dtypes)

# COMMAND ----------


# union_df = group_event_df.union(discord_events_df_reduced).union(discord_server_seeder_df_reduced)
# result = pd.concat([df1, df2, df3], axis=1, ignore_index=True)
union_df = pd.concat([group_event_df, discord_events_df_reduced, discord_server_seeder_df_reduced], ignore_index=True)

# COMMAND ----------

union_df.display()

# COMMAND ----------

# Convert the timestamp column to datetime format
df = union_df
df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True)

# Convert the timestamp to the Oslo timezone (GMT+1) and extract the time column for all rows
df['Time'] = df['Timestamp'].dt.tz_convert('Europe/Oslo').dt.strftime('%H:%M')

# Extract the date column in YYYY-MM-DD format for all rows
df['Date'] = df['Timestamp'].dt.strftime('%Y-%m-%d')

# Extract the weekday column for all rows
df['Weekday'] = df['Timestamp'].dt.day_name()

# Drop the original Timestamp column
df = df.drop(['Timestamp'], axis=1)

# Print the transformed DataFrame
print(df)

# COMMAND ----------

df.display()

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_advertisements = spark.createDataFrame(df)
spark_df_advertisements.display()

# COMMAND ----------


# Drop the previous table if it exists
spark.sql("DROP TABLE IF EXISTS adb_hibak.gold_tf2_advertisements")
spark_df_advertisements.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_advertisements")

# COMMAND ----------

