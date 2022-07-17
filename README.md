# PySpark ETL Job

## Apache Spark
Apache Spark is a distributed general purpose data analysis framework for large-scale datasets that has extended and generalized MapReduce model.
One of the main features of Spark as a cluster computing platform is in-memory computation which increases the speed compared to MapReduce-Hadoop and traditional frameworks. 
Spark provides rich built-in libraries for various domains such as, machine learning, graph analytics, stream processing and SQL queries. Also, for highly accessibility Apache spark is offered  different simple APIs in Python, Java and Scala. 

## ETL pipeline
ETL (Extraction, Transform and Load) is the process of extracting data from multiple sources, then after performing some transformations, loading it into a database or data warehouse for further analysis. Indeed ETL is the first step in a data pipeline.

We create simple but robust ETL pipeline using Apache Spark and its Python APIs  as a common language (‘PySpark’).
PySpark offers a worthy solution for an ETL pipeline deployment. PySpark is able to rapidly process massive amounts of data and supports many data transformation features on various types of data (structured, semi-structured, or unstructured). Using this APIs, different data formats transform into Data frames and SQL for analysis purpose.

## Scalability
Apache Spark is a salable bulk synchronous data parallel processing system that scales the distributed application over terabytes of data by moving the computation to the data. Note, Spark platform in addition to use multiple computational workers, has the ability for multi-threading based on the CPU cores at each worker. Horizontal and vertical scaling improve the runtime and scalability, especially in the large datasets.

## Packaging project Dependencies
The libraries used in the script are lised in requirements.txt. To install them follow the instruction:
```
pip install -r requirements.txt --target=dependencies
cd dependencies
zip -r dependencies.zip *
mv dependencies.zip ../ 
```

## Running the ETL job
```bash
spark-submit --py-files dependencies.zip src/main_etl_pyspark.py -i <input_data_path> -o <output_data_path>
```
Two parameters must be passed to python script: -i for input data path and -o for output data path.
We used parquet format as input data files. Please set the path of input data in following directories:  
```
{input_data_path}/evidence/sourceId=eva/
{input_data_path}/diseases
{input_data_path}/targets
```
Plaese put the input data in HDFS file system.
```
hdfs dfs -put input_datasets
```

The details of all options can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html). Note, that we have left some options to be defined within the job (which is actually a Spark application) - e.g. `spark.cores.max` and `spark.executor.memory` are defined in the Python script as it is felt that the job should explicitly contain the requests for the required cluster resources.

## Outputs
The sample output files are in the "Outputs" directory .
