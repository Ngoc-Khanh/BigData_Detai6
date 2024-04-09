import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum, col, when, lower

# Tạo SparkSession
spark = SparkSession.builder.appName("detai3").getOrCreate()

# 3.1
print("\n3.1. Đọc dữ liệu từ file census_1000.csv vào DataFrame.")
# Đọc dữ liệu từ file CSV vào DataFrame
df = spark.read.csv("census_1000.csv", header=True, inferSchema=True)
# Hiển thị một số dòng của DataFrame
df.show(5)

# 3.2
print("\n3.2. Có bao nhiêu nhóm tuổi được đề cập trong dữ liệu? Liệt kê các nhóm tuổi.")
# Tìm số lượng và liệt kê các nhóm tuổi
age_groups = df.select("age").distinct().orderBy("age").collect()
num_age_groups = len(age_groups)
print("Số lượng nhóm tuổi:", num_age_groups)
print("Các nhóm tuổi:")
for row in age_groups:
    print(row["age"])

# 3.3
print("\n3.3. Tìm những nhóm tuổi của bằng cấp là “Bachelors”.")
# Tìm những nhóm tuổi có bằng cấp "Bachelors"
bachelors_age_groups = df.filter(df["education"] == " Bachelors").select("age").distinct().orderBy("age").collect()
# Hiển thị kết quả
print("Nhóm tuổi của những người có bằng cấp 'Bachelors':")
for row in bachelors_age_groups:
    print(row["age"])

# 3.4
print("\n3.4. Thay đổi định dạng dữ liệu của cột education-num, age thành Integer")
# Thay đổi định dạng dữ liệu của cột "education-num" thành Integer
df = df.withColumn("education-num", df["education-num"].cast(IntegerType()))
# Thay đổi định dạng dữ liệu của cột "age" thành Integer
df = df.withColumn("age", df["age"].cast(IntegerType()))
# Hiển thị một số dòng của DataFrame để kiểm tra kết quả
df.show(5)

# 3.5
print("\n3.5. Tìm nhóm tuổi có số lượng “Education-num” lớn nhất")
# Nhóm dữ liệu theo cột "age" và tính tổng số lượng "Education-num" trong mỗi nhóm
max_education_age_group = df.groupBy("age").agg(sum("education-num").alias("total_education_num")).orderBy("total_education_num", ascending=False).first()
# Hiển thị kết quả
print("Nhóm tuổi có số lượng 'Education-num' lớn nhất:")
print("Tuổi:", max_education_age_group["age"])
print("Tổng số lượng 'Education-num':", max_education_age_group["total_education_num"])

# 3.6
print("\n3.6. Tìm nhóm tuổi là nữ chưa kết hôn “never-married” là người da trắng ") 
# Lọc dữ liệu để chỉ giữ lại các dòng thỏa mãn các điều kiện
filtered_data = df.filter((df["gender"] == " Female") & (df["marital-status"] == " Never-married") & (df["ethnicity"] == " White"))
# Kiểm tra xem có dòng nào thỏa mãn không
if filtered_data.count() == 0:
    print("Không có nhóm tuổi nào là nữ chưa kết hôn ('never-married') và là người da trắng.")
else:
    # Hiển thị kết quả
    print("Nhóm tuổi là nữ chưa kết hôn ('never-married') và là người da trắng:")
    filtered_data.show()

# 3.7
print("\n3.7. Có bao nhiêu nhóm tuổi có người da trắng? Liệt kê các nhóm tuổi là người da trắng")
# Lọc các dòng có giá trị của cột "ethnicity" là "White"
white_age_groups = df.filter(df["ethnicity"] == " White").select("age").distinct().orderBy("age")
# Đếm số lượng nhóm tuổi
num_white_age_groups = white_age_groups.count()
# Hiển thị số lượng và liệt kê các nhóm tuổi là người da trắng
print("Số lượng nhóm tuổi có người da trắng:", num_white_age_groups)
print("Các nhóm tuổi là người da trắng:")
white_age_groups.show()

# 3.8
print("\n3.8. Tìm những nhóm tuổi có marital-status bắt đầu bằng “Married”")
# Lọc các dòng có trạng thái hôn nhân bắt đầu bằng "Married"
married_age_groups = df.filter(col("marital-status").startswith(" Married")).select("age").distinct().orderBy("age")
# Hiển thị kết quả
print("Những nhóm tuổi có marital-status bắt đầu bằng 'Married':")
married_age_groups.show()

# 3.9
print("\n3.9. Xoay giá trị trong cột gender thành nhiều cột với mỗi cột là một giá trị theo yêu cầu sau: dữ liệu trong cột là tổng số education-num của nhóm dữ liệu nghề nghiệp (Occupitation)")
# Xoay giá trị trong cột "gender" thành các cột mới, mỗi cột là tổng số "education-num" của nhóm dữ liệu nghề nghiệp (Occupation)
pivot_df = df.groupBy("occupation").pivot("gender").agg(sum("education-num"))
# Hiển thị kết quả
pivot_df.show()

# 3.10
print("\n3.10. Tạo một cột mới có giá trị là: Nếu gender = Male thì điền giá trị là M, Nếu gender = Female thì điền giá trị là")
# Tạo cột mới dựa trên giá trị của cột "gender" (chuẩn hóa sang chữ thường)
new_df = df.withColumn("gender_category", when(lower(df["gender"]) == " male", "M").otherwise("F"))
# Hiển thị một số dòng của DataFrame để kiểm tra kết quả
new_df.show(5)