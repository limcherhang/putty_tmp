from openpyxl import load_workbook

# 加载 Excel 文件
workbook = load_workbook('溫室氣體盤查清冊v1.2_帆宣系統科技股份有限公司_2023.xlsx')

# 获取指定的工作表（例如第一个工作表）
worksheet = workbook['2-定性盤查']

my_width = []

# 遍历 A 到 Z 列，并获取每列的宽度
for column in range(1, 30):
    column_letter = chr(64 + column)  # 将列的索引转换为字母标识符
    column_dimension = worksheet.column_dimensions[column_letter]
    width = column_dimension.width
    my_width.append(width)
    print(f"Column {column_letter}: {width}")

print(my_width)

my_height = []

for row in range(1, 43):
    row_dimension = worksheet.row_dimensions[row]
    height = row_dimension.height
    if height:
        my_height.append(height)
    else:
        my_height.append(15.75)
    print(f"Row {row}: {height}")

print(my_height)