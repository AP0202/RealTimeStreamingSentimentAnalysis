Realtime Data Streaming With TCP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch | End-to-End Data Engineering Project
Table of Contents

    Introduction
    System Architecture
    Technologies


Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline using TCP/IP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch. It covers each stage from data acquisition, processing, sentiment analysis with ChatGPT, production to kafka topic and connection to elasticsearch.
System Architecture

<img width="3333" height="1036" alt="image" src="https://github.com/user-attachments/assets/1302c266-82b8-4d28-a65b-54dac185d2ff" />


The project is designed with the following components:

    Data Source: We use yelp.com dataset for our pipeline.
    TCP/IP Socket: Used to stream data over the network in chunks
    Apache Spark: For data processing with its master and worker nodes.
    Confluent Kafka: Our cluster on the cloud
    Control Center and Schema Registry: Helps in monitoring and schema management of our Kafka streams.
    Kafka Connect: For connecting to elasticsearch
    Elasticsearch: For indexing and querying



Technologies

    Python
    TCP/IP
    Confluent Kafka
    Apache Spark
    Docker
    Elasticsearch
