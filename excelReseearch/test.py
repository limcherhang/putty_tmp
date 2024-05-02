import pandas as pd

# 创建一个新的Excel文件
workbook = pd.ExcelWriter('questions.xlsx', engine='xlsxwriter')
worksheet = workbook.book.add_worksheet()

# 在第1行第1列插入问题
question = "你最喜欢的颜色是什么？"
# worksheet.write(0, 0, question)

# 插入文本框并输入问题
worksheet.insert_textbox('A2', question)

# 保存Excel文件
workbook.close()