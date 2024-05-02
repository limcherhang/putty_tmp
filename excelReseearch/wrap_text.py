import pandas as pd

# 创建一个DataFrame
data = {'Value': ['This is a long text that will be wrapped in Excel cell.', 'Short text', 'Another long text that will be wrapped in Excel cell.']}
df = pd.DataFrame(data)

# 将 DataFrame 输出到 Excel 文件
output_file = 'output_with_wrapped_text.xlsx'

# 创建 ExcelWriter
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

# 将 DataFrame 写入 Excel 文件
df.to_excel(writer, index=False, header=False, startrow=1)

# 获取创建的 workbook 和 worksheet 对象
workbook = writer.book
worksheet = writer.sheets['Sheet1']

# 设置列的宽度
worksheet.set_column('A:A', 10)

# 设置单元格格式以自动换行
cell_format = workbook.add_format({'text_wrap': True})

# 将格式应用于指定范围的单元格
worksheet.set_column('A:A', None, cell_format)

# 关闭 ExcelWriter 并保存 Excel 文件
writer.close()

print("Excel 文件已保存：", output_file)