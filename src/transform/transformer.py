import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_date, lower, lit
from pyspark.sql.types import IntegerType, FloatType, BooleanType
from typing import Dict, Optional
from src.utils.utils import clean_text_udf, validate_integer_udf, validate_numeric_udf

logger = logging.getLogger(__name__)

class Transformer:
    """Transforms Spark DataFrames according to county configuration."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.integer_columns = [
            'bed', 'sqft', 'yos', 'or_book', 'or_page', 'hotel_units', 'apt_units',
            'gross_ar', 'yr_roll', 'ayb'
        ]
        self.numeric_columns = {
            'bath': (4, 2),
            'price': (15, 2),
            'adj_price': (15, 2),
            'price_per_sqft': (10, 2),
            'sales_ratio': (8, 2),
            'jst_val': (15, 2),
            'acreage': (10, 4),
        }
        self.column_mapping = self.config['column_mapping']
        self.excel_columns = {v: k for k, v in self.column_mapping.items()}

    def transform(self, df: DataFrame) -> Optional[DataFrame]:
        """Transform DataFrame by validating, cleaning, and deduplicating data."""
        try:
            # Rename columns to match table schema
            for table_col, excel_col in self.column_mapping.items():
                if excel_col in df.columns:
                    df = df.withColumnRenamed(excel_col, table_col)

            # Apply transformations
            for col_name in df.columns:
                if col_name == 'county_code':
                    continue
                if col_name in self.integer_columns:
                    df = df.withColumn(col_name, validate_integer_udf(col(col_name), lit(col_name)).cast(IntegerType()))
                elif col_name in self.numeric_columns:
                    precision, scale = self.numeric_columns[col_name]
                    df = df.withColumn(col_name, validate_numeric_udf(col(col_name), lit(col_name), lit(precision), lit(scale)).cast(FloatType()))
                elif col_name in ['date_sold', 'dos']:
                    df = df.withColumn(col_name, to_date(col(col_name)))
                elif col_name in ['pool', 'is_primary', 'is_secondary', 'multi_parcel']:
                    df = df.withColumn(col_name, when(lower(col(col_name)).isin('true', '1'), True)
                                             .when(lower(col(col_name)).isin('false', '0'), False)
                                             .otherwise(None).cast(BooleanType()))
                else:
                    df = df.withColumn(col_name, clean_text_udf(col(col_name)))
            
            # Deduplicate based on county_code, parcel_strap, date_sold
            original_count = df.count()
            df = df.dropDuplicates(['parcel_strap', 'date_sold', 'price'])
            new_count = df.count()
            logger.info(f"Deduplicated data: removed {original_count - new_count} duplicate rows")

            # Ensure all table columns exist
            for table_col in self.config['table_columns']:
                if table_col not in df.columns:
                    df = df.withColumn(table_col, lit(None))

            # Select only table columns
            df = df.select(self.config['table_columns'])
            
            logger.info(f"Transformed DataFrame with {df.count()} rows and columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            return df
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            return None