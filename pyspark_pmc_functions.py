import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import asc, to_date, avg, col
from datetime import datetime
from scipy.signal import find_peaks
import matplotlib.pyplot as plt

plt.rcParams["figure.figsize"] = (20, 6)

# MongoDB configuration
db_name = "performance"
collection = "player_data_temporary_timeseries_collection"
db_URI = f"mongodb+srv://your_user:your_password@cluster0.wqrsi.mongodb.net/{db_name}.{collection}"

# Initialize Spark session
my_spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Cluster0") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0') \
    .config("spark.mongodb.read.connection.uri", db_URI) \
    .config("spark.mongodb.write.connection.uri", db_URI) \
    .getOrCreate()

# Read MongoDB collection into Spark DataFrame
df = my_spark.read \
    .format("mongodb") \
    .option("uri", db_URI) \
    .option("database", db_name) \
    .option("collection", collection) \
    .load()

# Get distinct session/date pairs for a given athlete
def get_session_days(player):
    return (df.where(col("metadata.athleteId") == player)
            .select(to_date('timestamp').alias('date'),
                    col("metadata.session").alias('session'))
            .distinct()
            .sort(asc('date'))
            .toJSON()
            .collect())

# Compute rolling average over a given duration for a selected column
def duration_avg(player, session, date, duration, column):
    print(f'Duration AVG for: {player} - {session} - {date} - {duration}s - {column}')
    avg_window = Window.partitionBy(col("metadata.session")).orderBy(col('timestamp').cast('long')).rangeBetween(0, duration)
    return (df.where(
        (col("metadata.athleteId") == player) &
        (col("metadata.session") == session) &
        (to_date(col("timestamp")) == date))
        .select(
            col('timestamp').alias('timestamp'),
            col(column).alias(column),
            avg(column).over(avg_window).alias('rolling_average')
        )
        .toJSON()
        .collect())

# Rolling average for activity
def acc_duration_avg(player, session, date, duration):
    return duration_avg(player, session, date, duration, 'activity')

# Detect peaks for a given session/date and column
def peaks_per_session(player, session, date, column):
    data = pd.json_normalize([eval(item) for item in df.where(
        (col("metadata.athleteId") == player) &
        (col("metadata.session") == session) &
        (to_date(col("timestamp")) == date))
        .sort(asc('timestamp'))
        .toJSON()
        .collect()])
    peaks, _ = find_peaks(data[column].values, prominence=1, wlen=10)
    return data, peaks

# Main script
if __name__ == '__main__':
    # Test inputs
    tplayer = 'ATHLETE-062'
    tdate = datetime(2024, 10, 17)
    tsession = 'Game'
    tduration = 60  # 1-minute window

    # Get session/day list
    session_days = get_session_days(tplayer)
    print(session_days)

    # Plot rolling average for activity
    acc_data = acc_duration_avg(tplayer, tsession, tdate, tduration)
    acc_df = pd.json_normalize([eval(item) for item in acc_data])  # JSON -> DataFrame

    if not acc_df.empty and {'timestamp', 'activity', 'rolling_average'}.issubset(acc_df.columns):
        acc_df['timestamp'] = pd.to_datetime(acc_df['timestamp'])  # ensure datetime dtype
        plt.plot(acc_df['timestamp'], acc_df['activity'], label="Activity")
        plt.plot(acc_df['timestamp'], acc_df['rolling_average'], label="Rolling Average")
        plt.xticks(fontsize=7, rotation=25)
        plt.title(f'Activity Average per {tduration}s Window')
        plt.legend()
        plt.show()
    else:
        print("Error: Missing required columns in activity data or data is empty.")

    # Plot peaks for the session
    data_df, tpeaks = peaks_per_session(tplayer, tsession, tdate, 'activity')

    if not data_df.empty and {'timestamp', 'activity'}.issubset(data_df.columns):
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])  # ensure datetime dtype
        plt.plot(data_df['timestamp'], data_df['activity'], label="Activity")
        plt.scatter(data_df.iloc[tpeaks]['timestamp'],
                    data_df.iloc[tpeaks]['activity'],
                    marker='x', color='r', label="Peaks")
        plt.xticks(fontsize=7, rotation=25)
        plt.title(f'Peaks Detected for Session {tsession} on {tdate.strftime("%Y-%b-%d")}')
        plt.legend()
        plt.show()
    else:
        print("Error: Missing required columns in peak detection data or data is empty.")

