import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Định nghĩa schema cho dữ liệu CSV
schema = StructType([
    StructField("Div", StringType(), nullable=True),
    StructField("Date", StringType(), nullable=True),
    StructField("Time", StringType(), nullable=True),
    StructField("HomeTeam", StringType(), nullable=True),
    StructField("AwayTeam", StringType(), nullable=True),
    StructField("FTHG", IntegerType(), nullable=True),
    StructField("FTAG", IntegerType(), nullable=True),
    StructField("FTR", StringType(), nullable=True),
    StructField("HTHG", IntegerType(), nullable=True),
    StructField("HTAG", IntegerType(), nullable=True),
    StructField("HTR", StringType(), nullable=True),
    StructField("HS", IntegerType(), nullable=True),
    StructField("AS", IntegerType(), nullable=True),
    StructField("HST", IntegerType(), nullable=True),
    StructField("AST", IntegerType(), nullable=True),
    StructField("HF", IntegerType(), nullable=True),
    StructField("AF", IntegerType(), nullable=True),
    StructField("HC", IntegerType(), nullable=True),
    StructField("AC", IntegerType(), nullable=True),
    StructField("HY", IntegerType(), nullable=True),
    StructField("AY", IntegerType(), nullable=True),
    StructField("HR", IntegerType(), nullable=True),
    StructField("AR", IntegerType(), nullable=True),
    StructField("B365H", StringType(), nullable=True),
    StructField("B365D", StringType(), nullable=True),
    StructField("B365A", StringType(), nullable=True),
    StructField("BWH", StringType(), nullable=True),
    StructField("BWD", StringType(), nullable=True),
    StructField("BWA", StringType(), nullable=True),
    StructField("IWH", StringType(), nullable=True),
    StructField("IWD", StringType(), nullable=True),
    StructField("IWA", StringType(), nullable=True),
    StructField("LBH", StringType(), nullable=True),
    StructField("LBD", StringType(), nullable=True),
    StructField("LBA", StringType(), nullable=True),
    StructField("PSH", StringType(), nullable=True),
    StructField("PSD", StringType(), nullable=True),
    StructField("PSA", StringType(), nullable=True),
    StructField("WHH", StringType(), nullable=True),
    StructField("WHD", StringType(), nullable=True),
    StructField("WHA", StringType(), nullable=True),
    StructField("VCH", StringType(), nullable=True),
    StructField("VCD", StringType(), nullable=True),
    StructField("VCA", StringType(), nullable=True),
    StructField("Bb1X2", StringType(), nullable=True),
    StructField("BbMxH", StringType(), nullable=True),
    StructField("BbAvH", StringType(), nullable=True),
    StructField("BbMxD", StringType(), nullable=True),
    StructField("BbAvD", StringType(), nullable=True),
    StructField("BbMxA", StringType(), nullable=True),
    StructField("BbAvA", StringType(), nullable=True),
    StructField("BbOU", StringType(), nullable=True),
    StructField("BbMx>2.5", StringType(), nullable=True),
    StructField("BbAv>2.5", StringType(), nullable=True),
    StructField("BbMx<2.5", StringType(), nullable=True),
    StructField("BbAv<2.5", StringType(), nullable=True),
    StructField("BbAH", StringType(), nullable=True),
    StructField("BbAHh", StringType(), nullable=True),
    StructField("BbMxAHH", StringType(), nullable=True),
    StructField("BbAvAHH", StringType(), nullable=True),
    StructField("BbMxAHA", StringType(), nullable=True),
    StructField("BbAvAHA", StringType(), nullable=True),
    StructField("PSCH", StringType(), nullable=True),
    StructField("PSCD", StringType(), nullable=True),
    StructField("PSCA", StringType(), nullable=True)
])

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("LaLiga Analysis").getOrCreate()

# Đọc dữ liệu từ tập tin CSV
streaming_df = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .format("csv") \
    .option("path", "C:/Users/Admin/OneDrive - tuyenquang.edu.vn/Study/Spark/BigData_Detai6/streaming/") \
    .load()

# Lọc các trận đầu mà chủ nhà đã thua
home_losses = streaming_df.filter((col("FTR") == "A") & (col("FTHG") < col("FTAG")))

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

