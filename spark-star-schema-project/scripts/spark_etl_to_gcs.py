import os
import logging
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

# Paths
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../datalake/processed')
GCS_BUCKET = 'gs://spark-star-schema-bucket/processed/'
JAR_PATH = os.path.join(os.path.dirname(__file__), '../jars/gcs-connector-hadoop3-latest.jar')

# Table names and files
TABLES = {
    'dim_product': 'dim_product.csv',
    'dim_aisle': 'dim_aisle.csv',
    'dim_department': 'dim_department.csv',
    'dim_user': 'dim_user.csv',
    'dim_date': 'dim_date.csv',
    'fact_order_products': 'fact_order_products.csv',
}

def get_spark():
    logger.info('Starting Spark session...')
    spark = SparkSession.builder \
        .appName('StarSchemaETL') \
        .config('spark.jars', JAR_PATH) \
        .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('spark.hadoop.google.cloud.auth.type', 'APPLICATION_DEFAULT') \
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) \
        .config('spark.driver.memory', '4g') \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()
    logger.info('Spark session started.')
    return spark

def process_table(spark, table_name, file_name):
    try:
        logger.info(f'Processing {table_name}...')
        file_path = os.path.join(PROCESSED_DIR, file_name)
        df = spark.read.option('header', True).csv(file_path)
        logger.info(f'Schema for {table_name}:')
        df.printSchema()
        logger.info(f'Sample data for {table_name}:')
        df.show(5)
        gcs_path = os.path.join(GCS_BUCKET, table_name)
        logger.info(f'Writing {table_name} to {gcs_path} as Parquet...')
        df.write.mode('overwrite').parquet(gcs_path)
        logger.info(f'{table_name} written to {gcs_path}')
    except Exception as e:
        logger.error(f'Error processing {table_name}: {e}', exc_info=True)

def main():
    spark = get_spark()
    for table_name, file_name in TABLES.items():
        process_table(spark, table_name, file_name)
    spark.stop()
    logger.info('Spark session stopped.')

if __name__ == '__main__':
    main() 