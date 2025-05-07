import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, FloatType
from typing import Optional

logger = logging.getLogger(__name__)

def clean_text(value: Optional[str]) -> Optional[str]:
    """Clean text by stripping spaces and normalizing multiple spaces to single."""
    if value is None or not isinstance(value, str):
        return value
    return " ".join(value.split(' ')).strip()

clean_text_udf = udf(clean_text, StringType())

def validate_integer(value: any, column_name: str) -> Optional[int]:
    """Validate and convert value to integer or None."""
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        logger.debug(f"Invalid integer value '{value}' for column '{column_name}', converting to None")
        return None

validate_integer_udf = udf(validate_integer, IntegerType())

def validate_numeric(value: any, column_name: str, precision: int, scale: int) -> Optional[float]:
    """Validate and convert value to numeric within precision and scale or None."""
    if value is None or value == '':
        return None
    try:
        num = float(value)
        max_abs_value = 10 ** (precision - scale)
        if abs(num) >= max_abs_value:
            logger.debug(
                f"Numeric overflow for '{column_name}': value '{num}' exceeds max "
                f"absolute value {max_abs_value - 0.01:.2f}, converting to None"
            )
            return None
        return round(num, scale)
    except (ValueError, TypeError):
        logger.debug(f"Invalid numeric value '{value}' for column '{column_name}', converting to None")
        return None

validate_numeric_udf = udf(lambda value, col_name, precision, scale: validate_numeric(value, col_name, precision, scale), FloatType())