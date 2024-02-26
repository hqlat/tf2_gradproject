# Databricks notebook source
import pandas as pd
import pyspark
import datetime as dt
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.gold_tf2_map_connections_count;

# COMMAND ----------

df_connections = _sqldf
df_connections = df_connections.toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.gold_tf2_advertisements;

# COMMAND ----------

df_ads = _sqldf 
df_ads = df_ads.toPandas()

# COMMAND ----------

df_ads.to_csv('advertisements.csv', index=False)

# COMMAND ----------

df_connections.to_csv('connections.csv', index=False)

# COMMAND ----------

df_ads = df_ads.rename(columns={'gameType': 'game_type', 'Source': 'source', 'Human': 'human', 'Author': 'author', 'Time': 'time', 'Date': 'date', 'Weekday': 'weekday'})

# COMMAND ----------

# Convert 'date' columns to datetime for consistency
df_connections['date'] = pd.to_datetime(df_connections['date']).dt.date
df_ads['date'] = pd.to_datetime(df_ads['date']).dt.date

# COMMAND ----------

# Create a set of unique advertisement dates
advertisement_dates = set(df_ads['date'])

# COMMAND ----------

# Create a DataFrame with unique advertisement dates
ads_dates = pd.DataFrame(df_ads['date'].unique(), columns=['ad_date'])

# Merge with df_connections on 'date'
df_connections = df_connections.merge(ads_dates, left_on='date', right_on='ad_date', how='left')

# Create 'advertisement' column
df_connections['advertisement'] = df_connections['ad_date'].notna().astype(int)

# COMMAND ----------

# Drop the 'ad_date' column as it's no longer needed
df_connections.drop('ad_date', axis=1, inplace=True)

# COMMAND ----------

df_connections.columns

# COMMAND ----------

df_connections = df_connections[['map', 'date', 'server_id', 'connections_count', 'disconnections_count', 'weekday', 'is_weekend', 'game_type', 'advertisement']]

# COMMAND ----------

df_connections.display()

# COMMAND ----------

df_encoded = pd.get_dummies(df_connections, columns=['weekday', 'game_type'])

# COMMAND ----------

df_encoded.display()

# COMMAND ----------

day_mapping = {
    'weekday_0': 'Monday',
    'weekday_1': 'Tuesday',
    'weekday_2': 'Wednesday',
    'weekday_3': 'Thursday',
    'weekday_4': 'Friday',
    'weekday_5': 'Saturday',
    'weekday_6': 'Sunday'}

# COMMAND ----------

df_encoded.rename(columns=day_mapping, inplace=True)

# COMMAND ----------

df_encoded.display()

# COMMAND ----------

# Define high server load as being in the top X percentile of connections
threshold = df_encoded['connections_count'].quantile(0.75) # For example, the 75th percentile
df_encoded['high_traffic'] = (df_encoded['connections_count'] >= threshold).astype(int)

# COMMAND ----------

df_encoded.display()

# COMMAND ----------

df_encoded.head()

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

df_encoded.columns

# COMMAND ----------

# Splitting the data into train and test sets
X = df_encoded.drop(['high_traffic', 'date'], axis=1) 
y = df_encoded['high_traffic']

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# COMMAND ----------

# Initialize the model
rf_classifier = RandomForestClassifier(random_state=42)

# COMMAND ----------

# Train the model
rf_classifier.fit(X_train, y_train)

# COMMAND ----------

# Make predictions
y_pred = rf_classifier.predict(X_test)

# COMMAND ----------

# Evaluate the model
print(classification_report(y_test, y_pred))
print("Accuracy:", accuracy_score(y_test, y_pred))

# COMMAND ----------

# Create naive predictions based on the length of y_test
most_common_class = df_encoded['high_traffic'].mode()[0]
naive_predictions = [most_common_class] * len(y_test)

# COMMAND ----------

# Calculate naive model accuracy
naive_accuracy = accuracy_score(y_test, naive_predictions)
print("Naive Prediction Accuracy:", naive_accuracy)

# COMMAND ----------

# Assuming you have a trained model named 'model'
# and your feature set for predictions is 'X_test'
model_predictions_proba = rf_classifier.predict_proba(X_test)[:, 1]

# COMMAND ----------

# Calculate the proportion of the majority class
majority_class_proportion = df_encoded['high_traffic'].value_counts(normalize=True).max()

# Create an array with the majority class proportion for all predictions
naive_predictions_proba = np.array([majority_class_proportion] * len(y_test))


# COMMAND ----------

print(naive_predictions_proba.shape)

# COMMAND ----------

# Calculate ROC curve from both models
fpr_model, tpr_model, _ = roc_curve(y_test, model_predictions_proba)  # model_predictions_proba are the probability outputs from your model
roc_auc_model = auc(fpr_model, tpr_model)

# Use y_test instead of actual_values
fpr_naive, tpr_naive, _ = roc_curve(y_test, naive_predictions_proba)  # naive_predictions_proba can be a constant array if your naive model doesn't output probabilities
roc_auc_naive = auc(fpr_naive, tpr_naive)

plt.figure()
plt.plot(fpr_model, tpr_model, color='blue', lw=2, label='Model (area = %0.2f)' % roc_auc_model)
plt.plot(fpr_naive, tpr_naive, color='orange', lw=2, label='Naive (area = %0.2f)' % roc_auc_naive)

plt.plot([0, 1], [0, 1], color='gray', lw=1, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic')
plt.legend(loc="lower right")
plt.show()


# COMMAND ----------

print(y_pred.shape)
print(y_test.shape)
print(model_predictions_proba.shape)
print(naive_predictions_proba.shape)