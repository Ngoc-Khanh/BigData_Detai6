import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import count,desc, year,sum, lit,when,min,col, max,avg,split,explode,count,lower

spark = SparkSession.builder.appName("TEST_FILE").getOrCreate()

# print("3.1")
df = spark.read.csv("census_1000.csv", header = True, inferSchema = True)
# df.show(5)

# print("3.2")
# age_groups = df.select("age").distinct().orderBy("age")
# total_age_groups = age_groups.count()
# print("Số nhóm tuổi được đề cập trong dữ liệu", total_age_groups)
# print("Liệt kê")
# age_groups.show(total_age_groups, truncate = False)\

# print("3.3")
# bachelors_age_groups = df.filter(df["education"] == " Bachelors").select("age").distinct()
# total_bachelors = bachelors_age_groups.count()
# bachelors_age_groups.show(total_bachelors, truncate = False)

# print("3.4")
# df = df.withColumn("education-num", df["education-num"].cast(IntegerType()))
# df = df.withColumn("age", df["age"].cast(IntegerType()))
# df.printSchema()

print(3.5)
max_education_age_groups = df.groupBy("age", "education-num").count().groupBy(col("count").desc()).select("age", "education-num").first()
print("Tổng: ", max_education_age_groups.age)
filter_data = df.filter(df["age"] == max_education_age_groups.age)
filter_data.select("education-num").distinct().show()