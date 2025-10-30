import findspark
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.getOrCreate()

path = 'Dataset/log_content/20220410.json'
save_path = "/.../ ETL Pipeline/ETL_one_day"

def read_data_from_path(path):
    df = spark.read.json(path)
    return df

def transform_data(df):
    df = df.select('_source.*')
    df = df.withColumn("Type",
            when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "TVDuration")
            .when(col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"), "MovieDuration") 
            .when(col('AppName') == 'RELAX', "RelaxDuration")
            .when(col('AppName') == 'CHILD', "ChildDuration")
            .when(col('AppName') == 'SPORT', "SportDuration")
            .otherwise("Error")
        )
    return df


def summarize_data(df):
    df = df.select('Contract', 'Type', 'TotalDuration')
    df = df.filter(df.Contract != '0')
    df = df.filter(df.Type != 'Error')
    df = df.groupBy('Contract','Type').sum('TotalDuration')
    df = df.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    return df

def pivot_data(df):
    result =  df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    result = result.withColumn('Date',lit('2025-07-22'))
    return result




def main_task(path, save_path):
    print('------------------------Read data from HDFS--------------')
    df = read_data_from_path(path)
    df.show(5)


    print('-------------------Show data structure---------------')
    df.printSchema()

    print('----------------Transform data--------------')
    df = transform_data(df)
    # df.show(5)
    

    print('------------------Summarize data--------------')
    df = summarize_data(df)
    # df.show(5)

    print('----------------------------PIVOT data-----------------')
    result = pivot_data(df)

    result.show()

    print('------------Save file------------')
    result.repartition(1).write.mode("overwrite").csv(save_path,header=True)
    return print('Task Ran Successfully')


main_task(path,save_path)