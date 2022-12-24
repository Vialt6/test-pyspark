import re
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from pyspark.sql.functions import to_timestamp, col, lit, desc, udf
import panel as pn
import matplotlib.pyplot as plt
import pyspark
import seaborn as sns
import numpy as np
# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

#import nltk
#nltk.download('stopwords')

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
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()

html_parser_udf = udf(lambda x : html_parser(x), StringType())

def remove_square_brackets(text):
    return re.sub('\[[^]]*\]', '', text)

remove_square_brackets_udf = udf(lambda x : remove_square_brackets(x), StringType())

def remove_url(text):
    return re.sub(r'http\S+', '', text)

remove_url_udf = udf(lambda x : remove_url(x), StringType())


stopwords = set(stopwords.words("english"))
def remove_stopwords(text):
    final_text = []
    for i in text.split():
        if i.strip().lower() not in stopwords and i.strip().lower().isalpha():
            final_text.append(i.strip().lower())
    return " ".join(final_text)
remove_stopwords_udf = udf(lambda x : remove_stopwords(x), StringType())

def clean_text(text):
    text = html_parser_udf(text)
    text = remove_square_brackets_udf(text)
    text = remove_url_udf(text)
    text = remove_stopwords_udf(text)
    return text
clean_text_udf = udf(lambda x : clean_text(x), StringType())
clean_df = filter_df.select(clean_text_udf(col("review")), col("sentiment"))
clean_df.show()


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


