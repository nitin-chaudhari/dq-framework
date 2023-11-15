import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType, StructType, StructField
from datetime import datetime
import re

# Int check function
@udf(returnType=BooleanType())
def int_chk(column_name):
    try:
        if column_name % 1 == 0:
            True
        else:
            False
    except Exception as e:
        raise Exception(e)

# Decimal check function
@udf(returnType=BooleanType())
def decimal_chk(column_name):
    if column_name % 1 != 0:
        return True
    else:
        return False

# Email check function
@udf(returnType=BooleanType())
def email_chk(column_name):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(re.fullmatch(regex,column_name)):
        return True
    else:
        return False

# Numeric check function
@udf(returnType=BooleanType())
def numeric_chk(column_name):
    if str(column_name).isnumeric():
        return True
    else:
        return False

# Datetime check function
@udf(returnType=BooleanType())
def datetime_chk(column_name):
    try:
        return bool(datetime.strptime(str(column_name), "%Y-%m-%d"))
    except ValueError:
        return False

# Boolean check function
@udf(returnType=BooleanType())
def boolean_chk(column_name):
    if type(column_name) == bool:
        return True
    elif str(column_name).lower() == 'true' or str(column_name).lower() == 'false':
        return True
    else:
        return False

# Not null check function
@udf(returnType=BooleanType())
def not_null_chk(column_name):
    if str(column_name) != '' and str(column_name).lower() != 'null':
        return True
    else:
        return False

# Length check function
@udf(returnType=BooleanType())
def length_chk(column_name,length,condition="equal"):
    if condition == "less_than":
        if len(str(column_name)) <= length:
            return True
        else:
            return False
    elif condition == "greater_than":
        if len(str(column_name)) >= length:
            return True
        else:
            return False
    else:
        if len(str(column_name)) == length:
            return True
        else:
            return False