import logging
import os
import argparse
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.config.county_configs import get_county_config
from src.extract.excel_extractor import ExcelExtractor
from src.extract.json_extractor import JSONExtractor
from src.transform.transformer import Transformer
from src.load.postgres_loader import PostgresLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main(county: str, input_file: str):
    # Load environment variables
    load_dotenv()
    db_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME'),
        'driver': 'org.postgresql.Driver',
        'url': f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
    }

    # Validate environment variables
    missing_vars = [key for key, value in db_config.items() if not value]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64/"
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RealEstateETL") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    try:
        # Load configuration
        config = get_county_config(county)

        # Select extractor
        file_extension = os.path.splitext(input_file)[1].lower()
        if file_extension in ('.xlsx', '.xls'):
            extractor = ExcelExtractor(spark)
        elif file_extension == '.json':
            extractor = JSONExtractor(spark)
        else:
            logger.error(f"Unsupported file format: {file_extension}")
            return

        # Extract data
        df = extractor.extract(input_file, config)
        if df is None or df.count() == 0:
            logger.error("Failed to extract data or data is empty. Exiting.")
            return

        # Transform data
        transformer = Transformer(spark, config)
        transformed_df = transformer.transform(df)
        if transformed_df is None or transformed_df.count() == 0:
            logger.error("No valid data after transformation. Exiting.")
            return

        # Load data
        loader = PostgresLoader(spark, db_config)
        if not loader.check_permissions(config['schema_name'], config['table_name']):
            logger.error("Insufficient permissions. Exiting.")
            return

        if not loader.load(transformed_df, config):
            logger.error("Failed to load data. Exiting.")
            return

        logger.info(f"ETL process for {county} completed successfully")
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL pipeline for county property sales')
    file = "/home/angelo/Downloads/Title.xlsx"
    county = "osceola"
    main(county, file)