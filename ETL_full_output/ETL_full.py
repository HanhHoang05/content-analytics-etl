import findspark  # type: ignore
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime, timedelta

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def READ_multi_file(input_base_path):
    """Đọc nhiều file JSON vào DataFrame."""
    print("Files in input directory:")
    files_in_dir = os.listdir(input_base_path)
    print(files_in_dir)

    print('Ready to start ETL process.')
    start_date = datetime.strptime(input('Enter start_date (yyyymmdd): '), '%Y%m%d').date()
    to_date = datetime.strptime(input('Enter to_date (yyyymmdd): '), '%Y%m%d').date()

    date_list = []

    current_date = start_date 
    end_date = to_date

    while (current_date <= end_date):
        date_list.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    print(f"Dates to process: {date_list}")

    all_input_paths = [input_base_path + '/' + date + '.json' for date in date_list]
    print(f"-------------Reading data from {len(all_input_paths)} files--------------")
    
    initial_df = spark.read.json(all_input_paths)

    return initial_df


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
    print("Data saved Successfully")

def ETL_process(df):
    print("-----------Selecting fields--------------")
    df = select_fields(df)

    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)

    print('-------------Transforming Category --------------')
    df = transform_category(df)

    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)

    print('-------------Finalizing result --------------')
    processed_etl_df = finalize_result(statistics, total_devices)

    return processed_etl_df


def add_most_watch_column(df):
    max_col = greatest(
        col("ChildDuration"),
        col("MovieDuration"),
        col("RelaxDuration"),
        col("SportDuration"),
        col("TVDuration")
    )
    df = df.withColumn('most_watch',
                    when(col("ChildDuration") == max_col, "Thiếu nhi")
                    .when(col("MovieDuration") == max_col, "Phim truyện")
                    .when(col("RelaxDuration") == max_col, "Giải trí")
                    .when(col("SportDuration") == max_col, "Thể thao")
                    .when(col("TVDuration") == max_col, "Truyền hình")
                    )
    return df

def add_taste_column(df):
    df = df.withColumn('Taste', concat_ws('-', 
          *[
            when(col("ChildDuration") != 0, "Thiếu nhi").otherwise(None),
            when(col("MovieDuration") != 0, "Phim truyện").otherwise(None),
            when(col("RelaxDuration") != 0, "Giải trí").otherwise(None),
            when(col("SportDuration") != 0, "Thể thao").otherwise(None),
            when(col("TVDuration") != 0, "Truyền hình").otherwise(None)
        ]
        ))
    return df

def add_total_days_column(df):
    TotalSeconds = (
        col("ChildDuration") +
        col("MovieDuration") +
        col("RelaxDuration") +
        col("SportDuration") +
        col("TVDuration")
    ) 

    TotalDays = TotalSeconds/ 86400

    df = df.withColumn('Active_day',
                       when(TotalDays < 10, 'Low')
                       .when((TotalDays >= 10) & (TotalDays < 20), 'Medium')
                       .when(TotalDays >= 20, 'High')
                       )
    return df
               
def OLAP_process(processed_etl_df):
    """Đọc dữ liệu đã xử lý và thêm các cột phân tích."""
    print("-------------Adding 'most_watch' column--------------")
    df = add_most_watch_column(processed_etl_df)
    print("-------------Adding 'Taste' column--------------")
    df = add_taste_column(df)
    print("-------------Adding 'Active_day' column--------------")
    df = add_total_days_column(df)
    print("-------------Final analyzed data:--------------")
    df.show()
    return df

def main():
    input_path = 'C:/Users/admin/Documents/STUDY_DE/Dataset/log_content'
    etl_output_path = 'C:/Users/admin/Documents/STUDY_DE/Buổi 6 ETL/ETL_full_output'

    start_time =datetime.now()

    # Bước 1: Đọc dữ liệu (Extract)
    initial_df = READ_multi_file(input_path)

    # Bước 2: Xử lý ETL (Transform)
    processed_etl_df = ETL_process(initial_df)

    # Bước 3: Xử lý OLAP (Analyze/Load cho mục đích báo cáo/phân tích)
    result_df = OLAP_process(processed_etl_df)

    # Bước 4: Lưu dữ liệu cuối cùng (Load)
    save_data(result_df, etl_output_path)
    
    end_time = datetime.now()
    print(f"ETL process completed in {(end_time - start_time).total_seconds()} seconds")

    spark.stop()

if __name__ == "__main__":
    main()
