from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark

def get_dbutils(spark):
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

def setup():
  spark.sparkContext.appName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()




