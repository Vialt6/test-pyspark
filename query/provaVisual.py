from pyspark.sql.functions import to_timestamp, col, lit, desc
import panel as pn
import matplotlib.pyplot as plt
import pyspark
import seaborn as sns

# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


df = spark.read.csv('..\\IMDB Dataset.csv', header=True)
df.show(5)
df.printSchema()


df.select("sentiment").show(truncate = False)

film = df.groupby("review").count()
film.sort(desc("count")).show()

filter_df = df.filter((df.sentiment == "positive") | (df.sentiment == "negative"))
filter_df.show()

