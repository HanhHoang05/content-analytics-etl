import findspark  # type: ignore
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime, timedelta


spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def read_data_from_path(path):
    df = spark.read.json(path)
    return df

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

def etl_main(path):
    print('............Reading data from path..................')
    df = read_data_from_path(path)

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
output_path = 'C:/Users/admin/Documents/STUDY_DE/Buổi 4 ETL Pipeline/test_method1_eachFILE_output'

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



start_time = datetime.now()

final_result = None

for i in date_list:
    print("ETL_TASK: " + input_path + "/" + i + ".json")
    result = etl_main(input_path +"/"+ i + '.json')

    if final_result is None:
        final_result = result
    else:
        final_result = final_result.union(result)

print("Calculation on final output")                        #Khac nhau o buoc nay 

save_data(final_result, output_path)
end_time = datetime.now()
print((end_time - start_time).total_seconds())



'''
Đây là hướng thứ hai:
Đọc và xử lí từng file, sau đó gộp tất cả các kết quả lại
Output: 279.606513s
--> Lâu nhưng dễ recover
'''