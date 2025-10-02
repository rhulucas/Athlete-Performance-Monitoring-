import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, asc, to_date

db_name = "performance"
collection = "player_data_temporary_timeseries_collection"
db_URI = f"mongodb+srv://your_user:your_password@cluster0.wqrsi.mongodb.net/{db_name}.{collection}"

# Set session with mongo-spark connector and URIs for db
my_spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Cluster0") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0') \
    .config("spark.mongodb.read.connection.uri", db_URI) \
    .config("spark.mongodb.write.connection.uri", db_URI) \
    .getOrCreate()

# Connect to DB
df = my_spark.read \
    .format("mongodb") \
    .option("uri", db_URI) \
    .option("database", db_name) \
    .option("collection", collection) \
    .load()

# Prints schema of connected DB
df.printSchema()

# Result
# root
#  |-- _id: string (nullable = true)
#  |-- activity: decimal(5,2) (nullable = true)
#  |-- heart_rate: decimal(5,2) (nullable = true)
#  |-- metadata: struct (nullable = true)
#  |    |-- session: string (nullable = true)
#  |    |-- athleteId: string (nullable = true)
#  |-- timestamp: timestamp (nullable = true)

print("## Examples ##")
print("Session Types: ", df.select('metadata.session').distinct().show())

print("Min/Max Activity: ")
df.select('activity').summary("min", "max").show()

print("Distinct session days: ")
df.select(year('timestamp').alias('year'),
        month('timestamp').alias('month'),
        dayofmonth('timestamp').alias('day')
        ) \
    .distinct() \
    .sort(asc('month'), asc('day')) \
    .show()

print("Athlete List: ")
athletes = df.select('metadata.athleteId').distinct().collect()
print(f'{len(athletes)} athletes')
for athlete in athletes:
    print(athlete.asDict()['athleteId'])

print('Session durations: ')
df.groupby(to_date(df.timestamp), df.metadata.athleteId).count().show()


print('Session count per athlete: ')

df.select(df.metadata.athleteId.alias('athlete'), to_date(df.timestamp)).distinct().groupby('athlete').count().show()
