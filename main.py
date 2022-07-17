
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType
import numpy as np
import getopt
import sys


def main(ds_path, output_path):
    """
    Main ETL script definition.
    :param
    :return: None
    """

    # start Spark application and get Spark Context and Spark session
    sc, spark = start_spark()

    ### execute ETL pipeline ###

    # read 3 datasets (evidence, diseases and targets) from input path to Dataframes
    rdd_eva, df_targ, df_dis = extract_data(spark, ds_path)

    df_transformed = transform_data(spark, rdd_eva, df_targ, df_dis)

    load_data(df_transformed, output_path)

    spark.stop()
    return None


def start_spark():
    """
    create SparkContext and Spark Session
    :return spark_context, spark_session
    """

    # set
    conf = SparkConf()
    # conf = SparkConf().getAll()
    spark_context = SparkContext(conf=conf).getOrCreate()
    # spark_context = SparkContext(conf=conf)
    spark_session = SparkSession(spark_context)

    # set Logging Level to ERROR. Stop INFO & DEBUG message logging to console
    spark_session.sparkContext.setLogLevel("ERROR")
    return spark_context, spark_session


def extract_data(spark, ds_path):
    """
    read data from input datasets
    :param spark: spark session
    :param ds_path: input data directory
    :return: rdd_evidence, df_target, df_disease
    """

    # Parse each evidence object to extract the `diseaseId`, `targetId`, and `score` fields.
    # read evidence dataset from parqet files to dataframe and then convert to rdd for the next transformation
    rdd_evidence = spark.read.parquet(ds_path + "/evidence/sourceId=eva/p*").select("diseaseid", "targetid", "score")\
        .rdd

    # read targets and disease datasets from parquet files to dataframes
    df_target = spark.read.parquet(ds_path + "/targets/p*")
    df_disease = spark.read.parquet(ds_path + "/diseases/p*")
    return rdd_evidence, df_target, df_disease


def transform_data(spark, rdd_eva, df_targ, df_dis):
    """

    :param spark:
    :param rdd_eva:
    :param df_targ:
    :param df_dis:
    :return:
    """

    # 2. For each `targetId` and `diseaseId` pair, calculate the median and 3 greatest `score`
    # values.
    print("2. For each `targetId` and `diseaseId` pair, calculate the median and top 3 scores ...")
    rdd_eva = rdd_eva.map(lambda x: ((x[0], x[1]), [x[2]])).reduceByKey(lambda x, y: x + y)
    rdd_eva = rdd_eva.map(lambda x: (x[0], float(np.median(x[1])), sorted(x[1], reverse=True)[:3]))

    # define a schema for convert rdd to dataframe
    schema_eva = StructType([StructField("diseaseid", StringType(), True),
                             StructField("targetid", StringType(), True),
                             StructField("median_score", FloatType(), True),
                             StructField("top3_score", ArrayType(FloatType()), True)])
    df_eva = rdd_eva.map(lambda x: (x[0][0], x[0][1], x[1], x[2])).toDF(schema_eva)
    df_eva.show()

    # 3. Join the targets and diseases datasets on the `targetId` = `target.id` and `diseaseId` = `disease.id` fields.
    # for this reason we should join disease dataset to eva and then join the result to target dataset
    # 4. Add the `target.approvedSymbol` and `disease.name` fields to your table
    print("3. Join the targets and diseases datasets on the `targetId`=`target.id` and `diseaseId`=`disease.id` ...")
    print("4. Add the `target.approvedSymbol` and `disease.name` fields ...")
    df_joined = df_dis.alias("diseases").join(df_eva.alias("eva"), df_dis.id == df_eva.diseaseid).\
        select("eva.*", "diseases.name").alias("dis_eva").\
        join(df_targ.alias("targets"), df_eva.targetid == df_targ.id).\
        select("dis_eva.*", "targets.approvedSymbol")

    # 5. Output the resulting table in JSON format, sorted in ascending order by the median value of
    # the `score`.
    print("5. sort in ascending order by the median ...")
    df_result = df_joined.orderBy(df_joined.median_score.asc())
    df_result.show()

    # Using the same dataset, extend script to:
    # 1. Count how many target-target pairs share a connection to at least two diseases.
    df_extend = df_result.groupBy("targetid").agg(F.collect_list("diseaseid").alias("diseaseid_list")).where(
        F.size("diseaseid_list") > 1)
    df_extend.show()

    df_new = df_extend.alias("df1").join(df_extend.alias("df2"), (F.col("df1.targetid") > F.col("df2.targetid")) &
                                         (F.size(F.array_intersect("df1.diseaseid_list", "df2.diseaseid_list")) >= 2))\
        # .select(F.col("df1.targetid").alias("targetid_1"), F.col("df2.targetid").alias("targetid_2"))
    df_new.show()

    print(f">>> Number of target-target pairs share a connection to at least two diseases >>> {df_new.count()}")
    return df_result


def load_data(df_result, out_path):
    """

    :param df_result:
    :return:
    """
    print(f"\nOutput the resulting table in JSON format in {out_path} directory...")
    df_result.coalesce(1).write.mode("overwrite").json(out_path)
    print(f"<<<Output is ready in: {out_path}>>>")


def get_args(argv):
    """
    Set input parameters
    :param argv: input parameters
    :return:
    """
    try:
        opts, args = getopt.getopt(argv, "d:", ["output="])
    except getopt.GetoptError:
        print('test.py -d <> -o <>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d"):
            data_path = arg
            print(f"Input Data Path >>> {data_path}")
        elif opt in ("-o", "--output"):
            output_path = arg
            # print(f"Dataset Path: {data_path}")

    return data_path, output_path

if __name__ == '__main__':
    # ds_path = "/home/myuser/Downloads/ftp.ebi.ac.uk/pub/databases/opentargets/platform/21.11/output/etl/parquet"
    ds_path, output_path = get_args(sys.argv[1:])
    main(ds_path, output_path)