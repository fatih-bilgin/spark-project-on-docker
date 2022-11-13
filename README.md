# Spark Project with Yelp Dataset on Docker

## Introduction

This project is a datalake project that includes raw, cleaned, and aggregated data. The Docker volume (shared-workspace directory) is simulated as HDFS.

The Docker compose project includes a Spark cluster with a Spark Master and two Spark Nodes. It also includes a Jupyterlab for interactive testing of Spark codes. 

## Quick Start

1. Clone the repository to your local environment:

```bash
git clone https://github.com/fatih-bilgin/spark-project-on-docker.git
```

2. Move to the project directory:

```bash
cd spark-project-on-docker
```

3. Download [Yelp Dataset](https://www.yelp.com/dataset/download).

4. Extract the datasets to shared-workspace folder:

```bash
tar -xf yelp_dataset -C shared-workspace
```

5. Start the cluster:

```bash
docker-compose up
```

## Project Steps

There are two data processing steps one of them creates raw and cleaned data and the other one creates aggregated data. Codes are available on shared-workspace\code.

1. Data Cleaning step (dataCleaning.py) takes 15-20 minutes to complete with 4 gb driver and executor memory.

Submit the code to the Spark cluster:

```bash
docker exec -it spark-master ./bin/spark-submit \
			     --master spark://spark-master:7077 \
			     /opt/workspace/code/dataCleaning.py
```
At the end of the process, two folders named "raw" and "cleaned" are created with datasets (in parquet format) in the shared-workspace directory. 


2. Data Processing step (dataProcessing.py) takes 2-3 minutes to complete with 4 gb driver and executor memory.

Submit the code to the Spark cluster:
```bash
docker exec -it spark-master ./bin/spark-submit \
			     --master spark://spark-master:7077 \
			     /opt/workspace/code/dataProcessing.py
```

At the end of the process, a folder named "aggregated" is created with datasets (in csv format) in the shared-workspace directory. 
