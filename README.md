# BigData_Detai6

## Câu 1. Nêu các đặc điểm của MapReduce Hadoop – so sánh với spark?
## Câu 2. Thế nào là tính toán phân tán, tính toán song song? Nếu một vài ví dụ thực tiễn sử dụng tính toán phân tán và tính toán song song?
## Câu 3. Sử dụng file census_1000.csv (Điều tra dân số)
3.1. Đọc dữ liệu từ file census_1000.csv vào dataframe hoặc RDD.  
3.2. Có bao nhiêu nhóm tuổi được đề cập trong dữ liệu? Liệt kê các nhóm tuổi.  
3.3. Tìm những nhóm tuổi của bằng cấp là “Bachelors”.  
3.4. Thay đổi định dạng dữ liệu của cột education-num, age thành Integer  
3.5. Tìm nhóm tuổi có số lượng “Education-num” lớn nhất  
3.6. Tìm nhóm tuổi là nữ chưa kết hôn “never-married” là người da trắng  
3.7. Có bao nhiêu nhóm tuổi có người da trắng? Liệt kê các nhóm tuổi là người da trắng  
3.8. Tìm những nhóm tuổi có marital-status bắt đầu bằng “Married”  
3.9. Xoay giá trị trong cột gender thành nhiều cột với mỗi cột là một giá trị theo yêu cầu sau: dữ liệu trong cột là tổng số education-num của nhóm dữ liệu nghề nghiệp (Occupitation)  
| Header | Header | Right  |
|:-------|:------:|-------:|
|  Occupitation  |  Male  |   Femail  |
|  Adm-clerical  |  10  |   50  |
|  Other-service  |  Null  |   100  |
| ====== | ====== | =====: |
| Footer | Footer | Footer |  

3.10. Tạo một cột mới có giá trị là: Nếu gender = Male thì điền giá trị là M, Nếu gende = Female thì điền giá trị là\\
## Câu 4. Sử dụng dữ liệu giải đấu laliga từ năm 2018 đến 2021 (file đính kèm) để thực hành stream tổng hợp tìm ra đội có số trận thua trên sân nhà nhiều nhất mùa giải, cùng với đó là tổng số bàn thua của họ.
FTR and Res = Full Time Result (H=Home Win, D=Draw, A=Away Win)\\
FTHG and HG = Full Time Home Team Goals,\\
FTAG and AG = Full Time Away Team Goals)\\
