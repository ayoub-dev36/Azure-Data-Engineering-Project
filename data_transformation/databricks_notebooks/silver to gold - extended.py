# Databricks notebook source
# MAGIC %md
# MAGIC # Single table column transformation (col names)

# COMMAND ----------

dbutils.fs.ls('mnt/silver/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/gold/')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/silver/SalesLT/Address/')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

def rename_columns_to_snake_case(df):
    """
    Convert column names from PascalCase or camelCase to snake_case in a PySpark DataFrame.

    Args:
        df (DataFrame): The input DataFrame with columns to be renamed.

    Returns:
        DataFrame: A new DataFrame with column names converted to snake_case.
    """
    # Get the list of column names
    column_names = df.columns

    # Dictionary to hold old and new column name mappings
    rename_map = {}

    for old_col_name in column_names:
        # Convert column name from PascalCase or camelCase to snake_case
        new_col_name = "".join([
            "_" + char.lower() if (
                char.isupper()              # Check if the current character is uppercase
                and idx > 0                 # Ensure it's not the first character
                and not old_col_name[idx - 1].isupper()  # Ensure the previous character is not uppercase
            ) else char.lower()  # Convert character to lowercase
            for idx, char in enumerate(old_col_name)
        ]).lstrip("_")  # Remove any leading underscore

        # Avoid renaming to an existing column name
        if new_col_name in rename_map.values():
            raise ValueError(f"Duplicate column name found after renaming: '{new_col_name}'")

        # Map the old column name to the new column name
        rename_map[old_col_name] = new_col_name

    # Rename columns using the mapping
    for old_col_name, new_col_name in rename_map.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)

    return df

# Example usage
# df = rename_columns_to_snake_case(df)



# COMMAND ----------

df = rename_columns_to_snake_case(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # All table columns transformation (col names)
# MAGIC

# COMMAND ----------

# To show the basic format of ls
table_name = []

for i in dbutils.fs.ls('mnt/silver/SalesLT'):
    table_name.append(i)

table_name

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/silver/SalesLT'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

# If you get Schema error, run this
# Check schema of Silver data
#silver_df = spark.read.format("delta").load("/mnt/silver/SalesLT/")
#silver_schema = silver_df.printSchema()

# Check schema of Gold data
#gold_df = spark.read.format("delta").load("/mnt/gold/SalesLT/")
#gold_schema = gold_df.printSchema()


# COMMAND ----------

for name in table_name:
    path = '/mnt/silver/SalesLT/' + name
    print(path)
    df = spark.read.format('delta').load(path)

    df = rename_columns_to_snake_case(df)

    output_path = '/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

display(df)