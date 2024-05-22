import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast


def get_all_json_files_from_dir(directory):
    return [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".json")]
    
def dict_to_df(spark: SparkSession, dict_data: dict):
    data = [(cluster, name) for cluster, names in dict_data.items() for name in names]
    df = spark.createDataFrame(data, ["cluster", "name"])
    
    return df

def read_json_file(spark: SparkSession, json_path: str):
    with open(json_path) as json_file:
        data = json.load(json_file)
    df = dict_to_df(spark, data)
    
    return df

def df_shape(df):
    return (df.count(), len(df.columns))

def read_json_files(spark, directory):
    sc = spark.sparkContext
    rdd = sc.wholeTextFiles(directory).map(lambda x: json.loads(x[1]))
    df = spark.createDataFrame(rdd.map(lambda x: Row(**x)))

    return df

def join_dfs_on_name(df1, df2):
    df1 = df1.alias('df1')
    df2 = df2.alias('df2')

    data = df1.join(broadcast(df2), col("df1.name") == col("df2.name"), 'inner')
    data = data.select('df1.cluster', 'df1.name', 'df2.protein')

    return data

def create_shingles(protein, k=5):
    return [protein[i:i+k] for i in range(len(protein) - k + 1)]

def calculate_accuracy(ground_truth, predicted):
    total = 0
    correct = 0
    for gt, pred in zip(ground_truth, predicted):
        if set(gt) == set(pred):
            correct += 1
        total += 1
    return correct / total