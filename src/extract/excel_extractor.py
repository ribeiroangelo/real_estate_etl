import logging
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import Dict, Optional
from .base_extractor import Extractor

logger = logging.getLogger(__name__)

class ExcelExtractor(Extractor):
    """Extractor for Excel files using PySpark."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def extract(self, file_path: str, config: Dict) -> Optional[DataFrame]:
        """Extract data from an Excel file into a Spark DataFrame."""
        try:
            pdf = pd.read_excel(file_path, sheet_name="People")
            spark = SparkSession.builder.appName("ExcelReader").getOrCreate()
            df = spark.createDataFrame(pdf)
            
            if df.count() == 0:
                logger.error("Excel file is empty")
                return None

            # Add county_code column
            df = df.withColumn("county_code", lit(config['county_code']))
            
            logger.info(f"Extracted {df.count()} rows from Excel with columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            return None