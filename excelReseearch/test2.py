import pandas as pd

# 创建一个DataFrame
data = {'Value': [3.1415, 2.71828, 1.41423]}
df = pd.DataFrame(data)

# 将数值四舍五入到指定的小数位数
decimal_places = 10
df['Value'] = df['Value'].round(decimal_places)

# 将 DataFrame 输出到 Excel 文件
output_file = 'output_with_color.xlsx'

# 创建 ExcelWriter
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 将 DataFrame 写入 Excel 文件
df.to_excel(writer, index=False, header=False, startrow=1)

# 获取创建的 workbook 和 worksheet 对象
workbook = writer.book
worksheet = writer.sheets['Sheet1']

# 设置标题格式
title_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_color': 'white', 'bg_color': 'blue'})
worksheet.write(0, 0, 'Value', title_format)

# 设置单元格格式以显示小数位数
cell_format = workbook.add_format({'num_format': '0.0000000000'})

# 设置单元格颜色
for row_num in range(1, len(df) + 1):
    worksheet.write(row_num, 0, df.iloc[row_num - 1, 0], cell_format)
    worksheet.set_row(row_num, None, None, {'level': 1, 'color': 'yellow'})

# 关闭 ExcelWriter 并保存 Excel 文件
writer.close()

print("Excel 文件已保存：", output_file)