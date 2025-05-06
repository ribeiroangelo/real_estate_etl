import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import Dict, Optional
from .base_extractor import Extractor

logger = logging.getLogger(__name__)

class TextExtractor(Extractor):
    """Extractor for .txt files using PySpark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, file_path: str, config: Dict) -> Optional[DataFrame]:
        """Extract data from a .txt file into a Spark DataFrame."""
        try:
            # Read the .txt file into a DataFrame (assuming it's CSV-like)
            df = self.spark.read.option("delimiter", ",") \
                               .option("quote", "\"") \
                               .csv(file_path, header=True, inferSchema=True)
            
            # Check if DataFrame is empty
            if df.count() == 0:
                logger.error("Text file is empty")
                return None

            # Add county_code column (like the Excel extractor)
            df = df.withColumn("county_code", lit(config['county_code']))

            # Log the number of rows and the first few columns
            logger.info(f"Extracted {df.count()} rows from text file with columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            
            return df

        except Exception as e:
            logger.error(f"Error reading text file: {str(e)}")
            return None