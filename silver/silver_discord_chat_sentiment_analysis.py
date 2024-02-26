# Databricks notebook source
!pip install transformers

# COMMAND ----------

!pip install torch

# COMMAND ----------

!pip install langdetect

# COMMAND ----------

import pyspark
from transformers import pipeline
import pandas as pd
from langdetect import detect, DetectorFactory
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.silver_tf2_discord_chat;

# COMMAND ----------

# Converting spark DF to pandas DF
df_chat = _sqldf
df_chat = df_chat.toPandas()

# COMMAND ----------

# Creating a copy DF that will only deal with date and not time. 
df_chat_date = df_chat.copy()

# COMMAND ----------

# Remove time from datetime in eventTime column
df_chat_date['Timestamp'] = pd.to_datetime(df_chat_date['Timestamp']).dt.date

# COMMAND ----------

# Remove all rows with empty values in message.
df_chat = df_chat[df_chat['Message'].str.strip() != '']
df_chat_date = df_chat_date[df_chat_date['Message'].str.strip() != '']

# COMMAND ----------

df_chat.display()

# COMMAND ----------

df_chat_date.display()

# COMMAND ----------

chat_sentiment_classifier = pipeline(
    model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
    return_all_scores=True
)

# COMMAND ----------

df_chat.info()

# COMMAND ----------

# Convert Messages to string in both DFs

df_chat = df_chat.copy()
df_chat.loc[:, 'Message'] = df_chat['Message'].astype(str)

df_chat_date = df_chat_date.copy()
df_chat_date.loc[:, 'Message'] = df_chat_date['Message'].astype(str)

# COMMAND ----------

# Create a new column with the sentiment scores.

df_chat['sentiment_scores'] = df_chat['Message'].apply(chat_sentiment_classifier)

# COMMAND ----------

# Extract the sentiment scores in the correct format.
# Original output from chat_sentiment_classifier returns a list in a list with three dictionaries with label positive, neutral and negative

def extract_sentiment_info(sentiment_scores):
    if sentiment_scores:
        sentiment_info = sentiment_scores[0]
        return {
            'positive': sentiment_info[0]['score'],
            'neutral': sentiment_info[1]['score'],
            'negative': sentiment_info[2]['score']
        }
    else:
        return {
            'positive': None,
            'neutral': None,
            'negative': None
        }

# COMMAND ----------

df_chat[['positive', 'neutral', 'negative']] = df_chat['sentiment_scores'].apply(extract_sentiment_info).apply(pd.Series)

# COMMAND ----------

df_chat.display()

# COMMAND ----------

df_chat.drop('sentiment_scores', axis=1, inplace=True)

# COMMAND ----------

df_chat.display()

# COMMAND ----------

# Set a seed for the langdetect detector for reproducibility
DetectorFactory.seed = 0

# Function to detect language, with error handling
def detect_language(text):
    try:
        return detect(text)
    except:
        return 'unknown'

# COMMAND ----------

# Create a new language column with the detected language for each message

df_chat['language'] = df_chat['Message'].apply(detect_language)


# COMMAND ----------

df_chat.head()

# COMMAND ----------

language_fullname = {
    'af': 'Afrikaans',
    'bg': 'Bulgarian',
    'ca': 'Catalan',
    'cs': 'Czech',
    'cy': 'Welsh',
    'da': 'Danish',
    'de': 'German',
    'en': 'English',
    'es': 'Spanish',
    'et': 'Estonian',
    'fi': 'Finnish',
    'fr': 'French',
    'hr': 'Croatian',
    'hu': 'Hungarian',
    'id': 'Indonesian',
    'it': 'Italian',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'mk': 'Macedonian',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'pl': 'Polish',
    'pt': 'Portuguese',
    'ro': 'Romanian',
    'ru': 'Russian',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'so': 'Somali',
    'sq': 'Albanian',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'tl': 'Tagalog',
    'tr': 'Turkish',
    'uk': 'ukrainian',
    'uknown': 'unknown',
    'vi': 'Vietnamese'
}

# COMMAND ----------

df_chat['language'] = df_chat['language'].replace(language_fullname)

# COMMAND ----------

df_chat.display()

# COMMAND ----------

df_chat.groupby('language').size()

# COMMAND ----------

# Count the number of languages.

language_counts = df_chat['language'].value_counts()

# COMMAND ----------

# Create a bar chart
plt.figure(figsize=(10, 6))
language_counts.plot(kind='bar')
plt.title('Occurrence of Each Language in Chat Messages')
plt.xlabel('Language')
plt.ylabel('Number of Messages')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# Create a dataframe with only English messages
df_chat_en = df_chat[df_chat['language'] == 'English']

# COMMAND ----------

df_chat_en.display()

# COMMAND ----------

# Convert the pandas DF to a spark DF
spark_df_chat_en = spark.createDataFrame(df_chat_en)
spark_df_chat_en.display()

# COMMAND ----------

# Convert the spark DF to a table in the db.
spark_df_chat_en.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_discord_chat_time_sentiment_analysis_en")

# COMMAND ----------

# Convert the pandas DF to a spark DF
spark_df_chat_all_languages = spark.createDataFrame(df_chat)
spark_df_chat_all_languages.display()

# COMMAND ----------

# Convert the spark DF to a table in the db.
spark_df_chat_all_languages.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_discord_chat_time_sentiment_analysis_all_languages")

# COMMAND ----------

