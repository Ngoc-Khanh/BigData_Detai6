from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import sum, col, when, lower, count
# Khởi tạo Spark session
spark = SparkSession.builder.appName("EducationSumByAge").getOrCreate()

# Đọc dữ liệu từ file CSV vào DataFrame
df = spark.read.csv("census_1000.csv", header=True, inferSchema=True)

# Lọc các hàng thỏa mãn điều kiện "never-married" và chủng tộc là da trắng
filtered_df = df.filter((col("marital-status") == " Never-married") & (col("ethnicity") == " White") & (col("gender") == " Female"))

# Tính toán và hiển thị nhóm tuổi của phụ nữ này
age_groups = filtered_df.groupBy("age").count().orderBy("age")
age_groups.show()

# Đếm số lượng nhóm tuổi
num_age_groups = age_groups.count()
print("Số lượng nhóm tuổi của phụ nữ chưa kết hôn và có chủng tộc là da trắng là:", num_age_groups)

# Đóng SparkSession
spark.stop()