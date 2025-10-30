import findspark  # type: ignore
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime, timedelta


spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()


def select_fields(df):
    df = df.select("_source.*")
    return df

def calculate_devices(df):
    total_devices = df.select("Contract", "Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count', 'TotalDevices')
    return total_devices


def transform_category(df):
    df = df.withColumn("Type",
            when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "TVDuration")
            .when(col("AppName").isin("VOD", "FIMS_RES", "BHD_RES", "VOD_RES", "FIMS", "BHD", "DANET"), "MovieDuration") 
            .when(col('AppName') == 'RELAX', "RelaxDuration")
            .when(col('AppName') == 'CHILD', "ChildDuration")
            .when(col('AppName') == 'SPORT', "SportDuration")
            .otherwise("Error")
            )
    return df

def calculate_statistics(df):
    df = df.filter(df.Contract != '0')
    df = df.filter(df.Type != 'Error')
    statistics = df.select('Contract', 'TotalDuration', 'Type').groupBy('Contract', 'Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics



def finalize_result(statistics, total_devices):
    result = statistics.join(total_devices, "Contract", "inner")
    return result

def save_data(result, save_path):
    result.repartition(1).write.option("header","true").mode("overwrite").csv(save_path)
    return print("Data saved Successfully")

def etl_main(df):
    print("-----------Selecting fields--------------")
    df = select_fields(df)

    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)

    print('-------------Transforming Category --------------')
    df = transform_category(df)

    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)

    print('-------------Finalizing result --------------')
    result = finalize_result(statistics, total_devices)

    return result



# def input_path():
#     url = str(input('Please provide data source folder'))
#     return url

# def output_path():
#     url = str(input('Please provide destination folder'))
#     return url


# input_path = input_path()
# output_path = output_path()

input_path = 'C:/Users/admin/Documents/STUDY_DE/Dataset/log_content'
output_path = 'C:/Users/admin/Documents/STUDY_DE/Buổi 4 ETL Pipeline/test_method1_readALL_output'

def list_files(input_path):
    list_files = os.listdir(input_path)
    print(list_files)
    print('How many file do you want to ETL?')
    return list_files

list_files = list_files(input_path)

start_date = datetime.strptime(input('Enter start_date (yyyymmdd): '), '%Y%m%d').date()
to_date = datetime.strptime(input('Enter to_date (yyyymmdd): '), '%Y%m%d').date()

date_list = []

current_date = start_date 
end_date = to_date

while (current_date <= end_date):
    date_list.append(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
print(date_list)



start_time =datetime.now()
all_paths = [input_path + '/' + date + '.json' for date in date_list]
df = spark.read.json(all_paths)

result = etl_main(df)
save_data(result, output_path)
end_time = datetime.now()
print((end_time - start_time).total_seconds())


'''
Đây là hướng thứ nhất:
Đọc dữ liệu của 30 files --> sau đó mới thực hiện tính toán
Output: 114.20013s
'''

