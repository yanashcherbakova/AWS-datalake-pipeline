import pandas as pd
import boto3

def athena_type_mapping(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    elif pd.api.types.is_float_dtype(dtype):
        return "double"
    elif pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "timestamp"
    else:
        return "string"
    
def generate_athena_schema(df, table_name, location):
    columns = []
    for col in df.columns:
        athena_type = athena_type_mapping(df[col].dtype)
        columns.append(f'{col} {athena_type}')
    schema = ',\n'.join(columns)
    query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS default.{table_name} (
{schema} 
)
STORED AS PARQUET
LOCATION '{location}'
;
"""
    return query