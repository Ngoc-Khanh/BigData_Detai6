from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("LaLiga Analysis") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu CSV
schema = StructType() \
    .add("Div", StringType()) \
    .add("Date", StringType()) \
    .add("Time", StringType()) \
    .add("HomeTeam", StringType()) \
    .add("AwayTeam", StringType()) \
    .add("FTHG", IntegerType()) \
    .add("FTAG", IntegerType()) \
    .add("FTR", StringType()) \
    .add("HTHG", IntegerType()) \
    .add("HTAG", IntegerType()) \
    .add("HTR", StringType()) \
    .add("HS", IntegerType()) \
    .add("AS", IntegerType()) \
    .add("HST", IntegerType()) \
    .add("AST", IntegerType()) \
    .add("HF", IntegerType()) \
    .add("AF", IntegerType()) \
    .add("HC", IntegerType()) \
    .add("AC", IntegerType()) \
    .add("HY", IntegerType()) \
    .add("AY", IntegerType()) \
    .add("HR", IntegerType()) \
    .add("AR", IntegerType()) \
    .add("B365H", StringType()) \
    .add("B365D", StringType()) \
    .add("B365A", StringType()) \
    .add("BWH", StringType()) \
    .add("BWD", StringType()) \
    .add("BWA", StringType()) \
    .add("IWH", StringType()) \
    .add("IWD", StringType()) \
    .add("IWA", StringType()) \
    .add("LBH", StringType()) \
    .add("LBD", StringType()) \
    .add("LBA", StringType()) \
    .add("PSH", StringType()) \
    .add("PSD", StringType()) \
    .add("PSA", StringType()) \
    .add("WHH", StringType()) \
    .add("WHD", StringType()) \
    .add("WHA", StringType()) \
    .add("VCH", StringType()) \
    .add("VCD", StringType()) \
    .add("VCA", StringType()) \
    .add("Bb1X2", StringType()) \
    .add("BbMxH", StringType()) \
    .add("BbAvH", StringType()) \
    .add("BbMxD", StringType()) \
    .add("BbAvD", StringType()) \
    .add("BbMxA", StringType()) \
    .add("BbAvA", StringType()) \
    .add("BbOU", StringType()) \
    .add("BbMx>2.5", StringType()) \
    .add("BbAv>2.5", StringType()) \
    .add("BbMx<2.5", StringType()) \
    .add("BbAv<2.5", StringType()) \
    .add("BbAH", StringType()) \
    .add("BbAHh", StringType()) \
    .add("BbMxAHH", StringType()) \
    .add("BbAvAHH", StringType()) \
    .add("BbMxAHA", StringType()) \
    .add("BbAvAHA", StringType()) \
    .add("PSCH", StringType()) \
    .add("PSCD", StringType()) \
    .add("PSCA", StringType())

# Đọc dữ liệu từ tập tin CSV
df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("C:/Users/Admin/OneDrive - tuyenquang.edu.vn/Study/Spark/BigData_Detai6/streaming/")

# Lọc các trận đấu mà đội chủ nhà đã thua
home_losses = df.filter((col("FTR") == "A") & (col("FTHG") < col("FTAG")))

# Nhóm các trận đấu theo tên của đội chủ nhà và tính tổng số trận thua và tổng số bàn thua của mỗi đội
total_losses = home_losses.groupBy("HomeTeam") \
    .agg({"FTHG": "sum", "FTAG": "sum", "*": "count"}) \
    .withColumnRenamed("sum(FTHG)", "total_home_goals_conceded") \
    .withColumnRenamed("sum(FTAG)", "total_away_goals_scored") \
    .withColumnRenamed("count(1)", "total_losses") \
    .withColumn("total_goals_conceded", col("total_home_goals_conceded") + col("total_away_goals_scored"))

# Tìm ra đội có số trận thua trên sân nhà nhiều nhất và tổng số bàn thua của họ
most_home_losses = total_losses.orderBy(col("total_losses").desc()).limit(1)

# In thông tin về đội có số trận thua trên sân nhà nhiều nhất và tổng số bàn thua của họ
query = most_home_losses \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Chờ quá trình streaming kết thúc
query.awaitTermination()