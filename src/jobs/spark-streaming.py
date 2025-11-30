import pyspark
import openai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import from_json, col, udf, when
from config.config import config


def sentiment_analysis(comment) -> str:
    if comment:
        openai.api_key = config['openai']['api_key']
        completion = openai.ChatCompletion.create(
            model="gpt-4.1-mini",
            messages=[
                {
                    "role": "system",
                    "content": """
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        
                        {comment}
                    """.format(comment=comment)
                }
            ]
        )
        return completion.choices[0].message['content'].strip()
    return "Empty Comment"


def start_streaming(spark):
    topic = "customers_review"

    # 1) Read from socket
    stream_df = (spark.readStream
                 .format("socket")
                 .option("host", "0.0.0.0")
                 .option("port", 9999)
                 .option("truncate", "false")
                 .load())

    # 2) Schema for a single review object
    review_schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        # StructField("useful", IntegerType()),
        # StructField("funny", IntegerType()),
        # StructField("cool", IntegerType()),
        StructField("text", StringType()),
        StructField("date", StringType()),
    ])

    # 3) Parse each JSON line into columns
    stream_df = (stream_df
                 .select(from_json(col('value'), review_schema).alias("review"))
                 .select("review.*"))

    # 4) Add feedback column with UDF
    sentiment_analysis_udf = udf(sentiment_analysis, StringType())

    stream_df = stream_df.withColumn(
        'feedback',
        when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
        .otherwise(None)
    )

    # 5) Prepare Kafka key/value
    kafka_df = stream_df.selectExpr(
        "CAST(review_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # 6) Write to Kafka
    query = (kafka_df.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
             .option("kafka.security.protocol", config['kafka']['security.protocol'])
             .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
             .option(
                 "kafka.sasl.jaas.config",
                 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                 'password="{password}";'.format(
                     username=config['kafka']['sasl.username'],
                     password=config['kafka']['sasl.password']
                 )
             )
             .option("checkpointLocation", "/tmp/checkpoint_v1")
             .option("topic", topic)
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn)
