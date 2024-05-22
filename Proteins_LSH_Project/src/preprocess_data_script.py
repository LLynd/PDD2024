from functions import (get_all_json_files_from_dir,
                        read_json_file,
                        read_json_files,
                        join_dfs_on_name,
                        df_shape)

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast


def preprocess_data_script(spark, PROJECT_PATH):
    json_path = os.path.join(PROJECT_PATH, "data", "group_definition.json")
    json_files = get_all_json_files_from_dir(os.path.join(PROJECT_PATH, "data", "fasta"))

    df = read_json_file(spark, json_path)
    print(df_shape(df))

    protein_df = read_json_files(spark, os.path.join(os.getcwd(), "data", "fasta"))
    protein_df = protein_df.withColumnRenamed('value', 'protein')
    #print(df_shape(protein_df))

    data = join_dfs_on_name(df, protein_df)
    print(df_shape(data))

    return df, protein_df, data