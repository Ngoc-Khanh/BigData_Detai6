import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import count,desc, year,sum, lit,when,min,col, max,avg,split,explode,count,lower
spark = SparkSession.builder.appName("BAI TAP LON").getOrCreate()

print("Câu 3.1")
df=spark.read.format('csv').option('header','true').load('D:/NhuQuynh/Programming_language/PySpark/BTL/csdl/DeTai6/census_1000.csv')
df.show(5)

print("Câu 3.2")
age_groups = df.select("age").distinct().orderBy("age")
total_age_groups = age_groups.count()
print("Số lượng các nhóm tuổi là:", total_age_groups)
print("Các nhóm tuổi:")
age_groups.show(total_age_groups, truncate=False)

print("Câu 3.3")
# Lọc các hàng có education là "Bachelors"
bachelors_age_groups = df.filter(df["education"] == " Bachelors").select("age").distinct()
total_bachelors=bachelors_age_groups.count()#phải lấy dc số lượng bao nhiêu thì mới in ra hết dc 
# Hiển thị kết quả
print("Số lượng nhóm tuổi của những người có bằng cấp là 'Bachelors':",bachelors_age_groups)
bachelors_age_groups.show(total_bachelors,truncate=False)

print("câu 3.4")
#Thay đổi định dạng dữ liệu của cột "education-num" và "age" thành Integer
df = df.withColumn("education-num", df["education-num"].cast(IntegerType()))
df = df.withColumn("age", df["age"].cast(IntegerType()))
df.printSchema()

print("Câu 3.5")
# Lấy ra nhóm tuổi có số lượng education-num lớn nhất,.asc:tăng dần
max_education_age_group = df.groupBy("age","education-num").count().orderBy(col("count").desc()).select("age", "education-num").first()
# In ra nhóm tuổi có số lượng education-num lớn nhất
print("\nNhóm tuổi có số lượng education-num lớn nhất:", max_education_age_group.age)
# Lọc dữ liệu với điều kiện là tuổi giống với nhóm tuổi có số lượng education-num lớn nhất
filtered_data = df.filter(df["age"] == max_education_age_group.age)
print("\nTất cả các giá trị của cột 'education-num' trong nhóm tuổi có số lượng education-num lớn nhất:")
filtered_data.select("education-num").distinct().show()

print("câu 3.6:")
filtered_data = df.filter((df["gender"] == " Female") & (df["marital-status"] == " Never-married") & (df["ethnicity"] == " White")).select("age").distinct()
total_age_female=filtered_data.count()
# Hiển thị kết quả
print("Số lượng tuổi là nữ, chưa kết hôn 'never-married', và là người da trắng:",total_age_female)
filtered_data.show(total_age_female,truncate=False)

print("câu 3.7")
# Lọc các dòng có giá trị của cột "ethnicity" là "White"
white_age_groups = df.filter(df["ethnicity"] == " White").select("age").distinct()#.orderBy("age"):tăng dần,.orderBy("age", ascending=False):giảm
num_white_age_groups = white_age_groups.count()
print("Số lượng nhóm tuổi có người da trắng:", num_white_age_groups)
print("Các nhóm tuổi là người da trắng:")
white_age_groups.show(num_white_age_groups,truncate=False)

print("câu 3.8")
start_married_groups = df.filter(col("marital-status").startswith(" Married")).select("age").distinct()
total_start_married=start_married_groups.count()
print("Số lượng nhóm tuổi có marital-status bắt đầu bằng 'Married':",total_start_married)
start_married_groups.show(total_start_married,truncate=False)

print("Câu 3.9.")
# Xoay giá trị trong cột "gender" thành các cột mới, mỗi cột là tổng số "education-num" của nhóm dữ liệu nghề nghiệp (Occupation)
pivot_df = df.groupBy("occupation").pivot("gender").agg(sum("education-num"))
pivot_df.show()

print("câu 10")
new_df = df.withColumn("gender_category", when((df["gender"]) == " Male", "M").otherwise("F"))
# Hiển thị một số dòng của DataFrame để kiểm tra kết quả
new_df.show(15)

spark.stop()