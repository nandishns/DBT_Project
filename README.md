# DBT Mini Project
 
### Team Members : Nandish N S, Ritvik ...


## Overview

This project automates the listing of B2B software products , ensuring that new software is promptly and efficiently added to our database. By leveraging advanced web scraping techniques, real-time data streaming, and automated workflows, this system maximizes the visibility and accessibility of new software products.

## Project Goals

- **Fast and Efficient Listings:** Automate the detection and listing of new software products to ensure real-time updates.
- **Global Reach:** Capture and list software launches worldwide, especially from underrepresented regions.
- **Technological Innovation:** Utilize modern technologies including web scraping, real-time data streams, and cloud-native services to maintain an efficient workflow.

### Product Information Sites

- **Description:** These are the primary sources where detailed and technical data about software products can be found. Key sources include software directories, official product pages, and industry-specific news portals.
- **Scraping Techniques:** Utilize BeautifulSoup for parsing HTML content from static pages and Selenium for interacting with JavaScript-driven dynamic web pages to extract critical data about software releases and updates.
- **Websites** ProductHunt, Slashdot, Betalist and many more tech news sites regularly post about new software products.

## Technology Stack

- **Web Scraping:** BeautifulSoup, Selenium
- **Data Streaming:** Apache Kafka, Spark
- **Data Storage and Management:** MongoDB, Docker, Kubernetes
- **APIs and Advanced Processing:** Large Language Models (LLMs)

## System Design
![image](https://github.com/nandishns/DBT_Project/assets/92267208/38e6b40b-2e71-4547-a286-51ebad229bc5)

### Data Streaming

Extracted data is streamed in real-time into Kafka topics designed to segment the data efficiently:

- **software** for direct product data
- **x-llm** for processed textual data needing further extraction
- **news** for updates from news sources about software products

### Real-time Processing

Kafka consumers process data on-the-fly. If new products are detected , they are added to MongoDB.

### Advanced Text Analysis

LLMs analyze textual data from news and social media to extract and verify new product details.


## Kafka Setup

run this command in root directory of the project

```bash
# start zookeeper and kafka
 docker-compose up -d
```

shutdown the kafka and zookeeper

```bash
# stop zookeeper and kafka
 docker-compose down
```
## Spark Setup
```bash
# pull spark image
 docker pull apache/spark

# start spark
docker exec -it spark /bin/bash
pip install python-dotenv pymongo pip install pydantic_core pyspark
```

# start master
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
  --master spark://localhost:7077 \
  /opt/application/consumer_software.py
```

## run scrapper

```bash
# Build the image
docker build -t scrape-products .
# run the image
docker run --network="host" scrape-products
```

## run consumers

```bash
# Build the product consumer
docker build -t software-consumer .
# run the image
docker run --network="host" software-consumer
```

```bash
# Build the product consumer
docker build -t twitter-consumer .
# run the image
docker run --network="host" twitter-consumer
```

```bash
# Build the product consumer
docker build -t news-consumer .
# run the image
docker run --network="host" news-consumer
```

## .env file
```bash  
MONGO_CONN_STRING=
TWITTER_USER_NAME=
TWITTER_PASSWORD=
#Gemini API KEY
GOOGLE_API_KEY=
```

## Reference

https://hub.docker.com/r/bitnami/spark/
https://hub.docker.com/r/confluentinc/cp-kafka
https://hub.docker.com/layers/confluentinc/cp-zookeeper/



## mongodb atlas clustor
![image](https://github.com/Manoj-2702/G2Hack_TryCatchDevs/assets/92267208/a5e87fd9-2b8c-4b7d-a45e-50089ddbfaca)


