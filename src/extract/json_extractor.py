import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import Dict, Optional
from .base_extractor import Extractor

logger = logging.getLogger(__name__)

class JSONExtractor(Extractor):
    """Extractor for JSON files using PySpark."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def extract(self, file_path: str, config: Dict) -> Optional[DataFrame]:
        """Extract data from a JSON file into a Spark DataFrame."""
        try:
            df = self.spark.read.option("multiline", "true").json(file_path)
            
            if df.count() == 0:
                logger.error("JSON file is empty")
                return None

            # Add county_code column
            df = df.withColumn("county_code", lit(config['county_code']))
            
            logger.info(f"Extracted {df.count()} rows from JSON with columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            return df
        except Exception as e:
            logger.error(f"Error reading JSON file: {str(e)}")
            return None