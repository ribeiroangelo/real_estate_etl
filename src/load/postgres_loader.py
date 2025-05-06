import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, BooleanType, StringType, DateType, DecimalType
from typing import Dict
import psycopg2

logger = logging.getLogger(__name__)

class PostgresLoader:
    """Loads Spark DataFrames into PostgreSQL."""
    
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config

    def check_permissions(self, schema_name: str, table_name: str) -> bool:
        """Check if the user has INSERT permissions on the table."""
        try:
            conn = psycopg2.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database']
            )
            with conn.cursor() as cur:
                cur.execute("SELECT current_user;")
                current_user = cur.fetchone()[0]
                logger.info(f"Current PostgreSQL user: {current_user}")

                cur.execute(
                    """
                    SELECT has_table_privilege(%s, %s, 'INSERT') AS can_insert;
                    """,
                    (current_user, f"{schema_name}.{table_name}")
                )
                can_insert = cur.fetchone()[0]
                if not can_insert:
                    logger.error(f"User '{current_user}' does not have INSERT permission on {schema_name}.{table_name}")
                    return False
                logger.info(f"User '{current_user}' has INSERT permission on {schema_name}.{table_name}")
                return True
        except Exception as e:
            logger.error(f"Error checking table permissions: {str(e)}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def load(self, df: DataFrame, config: Dict) -> bool:
        """Load DataFrame into PostgreSQL table."""
        try:
           
            df.write \
                .format("jdbc") \
                .option("url", self.db_config['url']) \
                .option("dbtable", f"{config['schema_name']}.{config['table_name']}") \
                .option("user", self.db_config['user']) \
                .option("password", self.db_config['password']) \
                .option("driver", self.db_config['driver']) \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully loaded {df.count()} rows into {config['schema_name']}.{config['table_name']}")
            return True
        except Exception as e:
            logger.error(f"Error loading data to PostgreSQL: {str(e)}")
            return False