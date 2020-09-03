"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from pyspark.sql.functions import mean, to_date, col, lit, count, current_date
from pyspark.sql import SparkSession


def main():
    """Main ETL script definition.

    :return: None
    """

    # start Spark application and get Spark session, logger and config
    spark = SparkSession.builder\
        .appName("demo")\
        .config("spark.mongodb.input.uri", "mongodb+srv://tfm:frei1996@tfm-real-estate.kovd1.gcp.mongodb.net/real_estate.flats?retryWrites=true&w=majority")\
        .config("spark.mongodb.output.uri", "mongodb+srv://tfm:frei1996@tfm-real-estate.kovd1.gcp.mongodb.net/real_estate.summary?retryWrites=true&w=majority")\
        .getOrCreate()

    spark_logger = spark._jvm.org.apache.log4j
    log = spark_logger.LogManager.getLogger(__name__)

    # log that main ETL job is starting
    log.warn('real_estate_processing is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.
        : param spark: Spark session object.: return: Spark DataFrame.
    """
    df = (
        spark.read.format("mongo").load()
    )

    return df


def get_summary_by(df, summaryBy):
    calculations = [mean("price").alias('avg_price'), mean(
        "size").alias('avg_size'), count(lit(1)).alias('total')]

    all_provinces_all_operations = df.groupBy(summaryBy).agg(
        *calculations).select(lit('all').alias('operation'), lit('all').alias('province'), '*')

    by_province_all_operations = df.groupBy(
        "province", summaryBy).agg(*calculations).select(lit('all').alias('operation'), '*')

    all_provinces_by_operation = df.groupBy(
        'operation', summaryBy).agg(*calculations)
    all_provinces_by_operation = all_provinces_by_operation.select('operation', lit('all').alias(
        'province'), *[c for c in all_provinces_by_operation.columns if c != 'operation'])

    by_province_by_operation = df.groupBy(
        'operation', 'province', summaryBy).agg(*calculations)

    summary = all_provinces_all_operations.union(by_province_all_operations).union(
        all_provinces_by_operation).union(by_province_by_operation)

    summary.show()

    return summary


def get_summary(df):
    calculations = [mean("price").alias('avg_price'), mean(
        "size").alias('avg_size'), count(lit(1)).alias('total')]

    filtered_df = df.filter(col('updated_at_date') == current_date())

    all_provinces_all_operations = filtered_df.agg(
        *calculations).select(lit('all').alias('operation'), lit('all').alias('province'), '*')

    by_province_all_operations = filtered_df.groupBy(
        "province").agg(*calculations).select(lit('all').alias('operation'), '*')

    all_provinces_by_operation = filtered_df.groupBy(
        'operation').agg(*calculations)
    all_provinces_by_operation = all_provinces_by_operation.select('operation', lit('all').alias(
        'province'), *[c for c in all_provinces_by_operation.columns if c != 'operation'])

    by_province_by_operation = filtered_df.groupBy(
        'operation', 'province').agg(*calculations)

    summary = all_provinces_all_operations.union(by_province_all_operations).union(
        all_provinces_by_operation).union(by_province_by_operation)

    summary.show()

    return summary


def transform_data(df):
    """Transform original dataset.
        :  param df: Input DataFrame.: return: Transformed DataFrame.
    """
    df_transformed = df\
        .withColumn("created_at_date", to_date(col("created_at")))\
        .withColumn("updated_at_date", to_date(col("updated_at")))

    by_created_at = get_summary_by(
        df_transformed, 'created_at_date').select('created_at_date', '*')

    today_summary = get_summary(
        df_transformed).select(lit(None).alias('created_at_date'), '*')

    return today_summary.union(by_created_at)


def load_data(df):
    """Collect data locally and write to CSV.
        : param df: DataFrame to print.: return: None
    """
    df.write.format("mongo").mode("overwrite").save()

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
