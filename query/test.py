from bs4 import BeautifulSoup
from pyspark.sql.functions import to_timestamp, col, lit, desc
import panel as pn
import matplotlib.pyplot as plt
import pyspark
import seaborn as sns
import numpy as np
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
#print((df.count(), len(df.columns)))

df.select("sentiment").show(truncate = False)

#count_sentiment = df.groupby("sentiment").count()
#count_sentiment.sort(desc("count")).show()
#res = df.withColumn("id", monotonicallyIncreasingId())

film = df.groupby("review").count()
film.sort(desc("count")).show()

filter_df = df.filter((df.sentiment == "positive") | (df.sentiment == "negative"))
filter_df.show()

#cleaning reviews
def html_parser(text):
    soup = BeautifulSoup(text, "html_parser")


















pdf1 = filter_df.toPandas()

def func(input="green"):
    plot = sns.displot(pdf1, x = "sentiment")
    fig0 = plot.fig
    fig0.set_size_inches(11, 8)
    return fig0

pn.extension()

select = pn.widgets.Select(value="#6082A2", options=["#a2a160", "#6082A2", "#a26061"])
interactive_func = pn.bind(func, input = select)

template = pn.template.FastListTemplate(
    site="Panel", title="Works With The Tools You Know And Love",
    sidebar=[select,
            pn.pane.Markdown("# Sentiment analysis on movie reviews")],
    main=[pn.Row(interactive_func)],
    header_background="#6082A2", accent_base_color="#6082A2"
)
template.servable()


