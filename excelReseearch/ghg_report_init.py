import pandas as pd
import datetime
from openpyxl.drawing.image import Image
from openpyxl import load_workbook

if __name__ == "__main__":

    filename = "溫室氣體盤查清冊v1.4_By_Aaron.xlsx"

    today = datetime.datetime.now().date()

    chr_ord = {}

    for i in range(65, 91):
        chr_ord[i-64] = chr(i)
    for i in range(65, 91):
        chr_ord[i-38] = "A"+chr(i)
    #{1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z', 27: 'AA', 28: 'AB', 29: 'AC', 30: 'AD', 31: 'AE', 32: 'AF', 33: 'AG', 34: 'AH', 35: 'AI', 36: 'AJ', 37: 'AK', 38: 'AL', 39: 'AM', 40: 'AN', 41: 'AO', 42: 'AP', 43: 'AQ', 44: 'AR', 45: 'AS', 46: 'AT', 47: 'AU', 48: 'AV', 49: 'AW', 50: 'AX', 51: 'AY', 52: 'AZ'}

    df = pd.DataFrame({})
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="1-基本資料", )

        workbook = writer.book

        # ------------------------- 1-基本資料 -----------------------------
        worksheet = writer.sheets["1-基本資料"]

        cf1 = workbook.add_format({
            "bold": True,       # 粗体
            "font_size": 14,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1
        })
        cf2 = workbook.add_format({
            "font_size": 14,
            "border": 1,
            "text_wrap": True
        })
        cf3 = workbook.add_format({
            "bold": True,       # 粗体
            "font_size": 14,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter"
        })

        cf4 = workbook.add_format({
            "bold": True,       # 粗体
            "font_size": 14,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })

        cf5 = workbook.add_format({
            "font_size": 14,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "left",
            "valign": "vcenter",
            "text_wrap": True
        })

        # 将格式应用于单个单元格 A1
        worksheet.write("A1", "版次", cf1)
        worksheet.write("B1", "V1.4", cf2)
        worksheet.write("C1", "更新時間", cf1)
        worksheet.write("D1", today.strftime("%m/%d/%Y"), cf2)

        worksheet.set_column("A:A", 9.89)
        worksheet.set_column("B:B", 12.89)
        worksheet.set_column("C:C", 11.67)
        worksheet.set_column("D:D", 14.44)
        worksheet.set_column("G:G", 32.67)
        for i in range(10):
            worksheet.set_row(i, 18.75)

        worksheet.merge_range("A3:G3", "盤查基本資料", cf3)
        worksheet.merge_range("A4:B4", "盤查場所", cf3)
        worksheet.merge_range("A5:B5", "盤查地址", cf3)
        worksheet.merge_range("A6:B6", "盤查年度", cf3)
        worksheet.merge_range("A7:A8", "資料蒐集期間", cf4)
        worksheet.write("B7", "起", cf3)
        worksheet.write("B8", "迄", cf3)
        worksheet.merge_range("A9:B9", "行業別", cf3)
        worksheet.merge_range("A10:B10", "行業", cf3)
        worksheet.set_row(10, 115.5)
        worksheet.merge_range("A11:B11", "報告邊界", cf4)
        
        company_name = "三光米股份有限公司"
        company_address = "彰化縣二水鄉南通路一段60號"
        inventory_year = "2022"
        inventory_start = "2022.01.01"
        inventory_end = "2022.12.31"
        industry_num = "0132"
        industry = "=VLOOKUP(C9,附表一!B2:C558,2,)"
        operationalBoundaries = "三光米股份有限公司111年溫室氣體盤查採用「營運控制權法」，報告邊界有以下場址：彰化縣二水鄉南通路一段60號"

        insert_list = [company_name, company_address, inventory_year, inventory_start, inventory_end, industry_num, industry]

        for i in range(len(insert_list)):
            worksheet.merge_range(f"C{i+4}:G{i+4}", insert_list[i], cf2)

        worksheet.merge_range("C11:G11", operationalBoundaries, cf2)
        # ------------------------- 2-定性盤查 -----------------------------
        df.to_excel(writer, sheet_name="2-定性盤查", )
        worksheet = writer.sheets["2-定性盤查"]

        process_list = [10, 14, 26, 24, 14.5, 14, 14]
        for i in range(1, 8):
            worksheet.set_column(f"{chr_ord[i]}:{chr_ord[i]}", process_list[i-1])
        worksheet.set_column("H:N", 5.89)
        worksheet.set_column("O:O", 8.56)


        worksheet.set_row(1, 31.5)
        for i in range(2, 100):
            worksheet.set_row(i, 17.25)

        cf6 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
        
        process_list = ["編號", "廠區歸屬", "製程名稱", "排放源", "是否屬於生質能源"]

        for i in range(1, 6):
            worksheet.merge_range(f"{chr_ord[i]}2:{chr_ord[i]}3", process_list[i-1], cf6)

        worksheet.write("F2", "類別", cf6)
        worksheet.write("G2", "類別\n排放類型", cf6)
        worksheet.merge_range("H2:N2", "可能產生溫室氣體種類", cf6)
    
        process_list = ["1~6", "(E,P,T,F)", "CO₂", "CH₄", "N₂O"]
        for i in range(6, 11):
            worksheet.write(f"{chr_ord[i]}3", process_list[i-8], cf6)

        worksheet.write("K3", "HFC\u209b", cf6)
        worksheet.write("L3", "PFC\u209b", cf6)
        worksheet.write("M3", "SF₆", cf6)
        worksheet.write("N3", "NF₃", cf6)
        worksheet.merge_range("O2:O3", "備住", cf6)

        ## 之後補上2-定性盤查的內容

        # ------------------------- 2.1-重大性準則 -----------------------------
        df.to_excel(writer, sheet_name="2.1-重大性準則", )
        worksheet = writer.sheets["2.1-重大性準則"]

        cf7 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#D9E1F2",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
        cf8 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
        })
        cf9 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
        })
        cf10 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
        cf11 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFF00",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter"
        })
        cf12 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFF00",  # 橙色的 Accent 6
            "border": 1,
            "text_wrap": True
        })
        cf13 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
            "text_wrap": True
        })


        content = """重大性評估因子及評估門檻
· 重大間接溫室氣體排放源之評估門檻：10
· 法規或客戶要求即為該年度重大間接排放源，須進行量化
· 未來評估門檻得依營運情形滾動式修正，惟須於盤查報告書闡明修正緣由
· 非法規要求之間接排放源重大性評分方式：
S= R × C × P
S：重大性(Significance)
R：排放源量化風險(Risk)
C：活動數據可信度(Credibility)
P：減量措施推行可行度(Practicability)
                """
        
        worksheet.insert_textbox('A2', content, {'width': 520, 'height': 300})

        process_list = [19, 27, 11, 9, 9, 11, 25, 40, 13, 13, 12, 14, 17, 10]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        process_list = [48, 16, 32, 16, 32, 32, 48, 32, 32, 16, 32, 16, 16, 16, 55, 32, 32, 16]

        for i in range(len(process_list)):
            worksheet.set_row(i, process_list[i])

        process_list = ["因子", "評分項目", "評分"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+1]}{19}", process_list[i], cf7)

        worksheet.merge_range("A20:A25", "排放源量化風險\n(R)", cf8)
        worksheet.merge_range("A26:A29", "活動數據可信度\n(C)", cf8)
        worksheet.merge_range("A30:A33", "減量措施推行\n可信度\n(P)", cf8)

        process_list = ["自廠量測係數", "設備商提供之係數", "地區排放係數", "國家排放係數", "國際排放係數", "無排放係數", "第三方提供之佐證單據", "財務或系統報表", "內部已簽核之報表", "無數據紀錄", "1~2年內可進行減量措施", "3~5年內可進行減量措施", "6年以上可進行減量措施","無法進行減量措施"]

        for i in range(20, 34):
            worksheet.write(f"B{i}", process_list[i-20], cf9)

        process_list = [5,4,3,2,1,0,5,3,1,0,4,3,2,1]
        for i in range(20, 34):
            worksheet.write(f"C{i}", process_list[i-20], cf8)

        process_list = ["類別", "排放源名稱", "說明", "法規或客戶\n要求", "排放源\n量化風險(R)", "活動數據\n可信度(C)",	"減量措施\n推行可行度(P)", "重大性總分(S)" ,"是否量化\n計算"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+6]}1", process_list[i], cf10)

        process_list = [
            ["2", "外購電力", "使用之台電電力", "X", "2", "5", "4", "40", "V"],
            ["3", "下游運輸及輸配", "組織售出產品運輸至零售商或倉儲產生的排放", "X", "2", "3", "4", "24", "V"],
            ["3", "上游原物料運輸及配送", "組織購買之原物料運輸與配送產生的排放", "X", "2", "3", "4", "24", "V"], 
            ["3", "員工通勤", "組織員工由家裡至辦公場址的通勤排放（交通運具非組織所有）", "X", "2", "1", "4", "8", "X"],
            ["3", "商務旅行", "組織員工的公務差旅運輸排放（交通運具非組織所有）", "X", "2", "1", "4", "8", "X"],
            ["4", "燃料與能源相關活動", "組織購買燃料與能源（未列入類別一、類別二）之原料開採、製造、運輸產生的排放", "X", "2", "5", "4", "40", "V"],
            ["4", "購買產品及服務", "組織購買產品與服務之原料開採、製造、運輸產生的排放", "X", "2", "3", "4", "24", "V"],
            ["4", "資本物品", "組織購買資本物品產品與服務之原料開採、製造、運輸產生的排放", "X", "0", "0", "1", "0", "X"],
            ["4", "營運產生之廢棄物處理", "組織營運衍生的廢棄物處理排放", "X", "2", "0", "4", "0", "X"],
            ["4", "上游租賃資產", "組織（承租者）租賃資產的排放（未列入類別一、類別二），由承租者報告", "X", "2", "0", "1", "0", "X"],
            ["5", "售出產品之加工", "下游製造商加工組織售出產品產生的排放", "X", "1", "0", "1", "0", "X"],
            ["5", "售出產品之使用", "使用組織售出產品產生的排放", "X", "1", "0", "1", "0", "X"],
            ["5", "售出產品的最終處置", "組織售出產品的廢棄處理排放", "X", "1", "0", "1", "0", "X"],
            ["5", "下游租賃資產", "組織（出租者）出租資產的排放（未列入類別一、類別二），由出租者報告", "X", "2", "0", "1", "0", "X"],
            ["5", "連鎖/特許經銷（加盟）", "報告期間特許經銷排放（未列入類別一、類別二），由授權者報告", "X", "1", "0", "1", "0", "X"],
            ["5", "投資", "報告期間投資（股權、債務、融資）產生的排放（未列入類別一、類別二）", "X", "1", "0", "1", "0", "X"],
            ["6", "其他", "非以上類別", "X", "0", "0", "0", "0", "X"]
        ]

        process_list_color = [
            [cf11, cf12, cf12, cf11, cf11, cf11, cf11, cf11, cf8],
            [cf11, cf12, cf12, cf11, cf11, cf11, cf11, cf11, cf8],
            [cf11, cf12, cf12, cf11, cf11, cf11, cf11, cf11, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf11, cf12, cf12, cf11, cf11, cf11, cf11, cf11, cf8],
            [cf11, cf12, cf12, cf11, cf11, cf11, cf11, cf11, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
            [cf8, cf13, cf13, cf8, cf8, cf8, cf8, cf8, cf8],
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+6]}{i+2}", process_list[i][j], process_list_color[i][j])

        # ------------------------- 3-定量盤查 -----------------------------
        df.to_excel(writer, sheet_name="3-定量盤查", )
        worksheet = writer.sheets["3-定量盤查"]

        cf14 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#D9D9D9",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })

        content = """使用說明
· 灰底為公式自動帶入，請手動填寫白底空格處
                """
        
        worksheet.insert_textbox('B2', content, {'width': 700, 'height': 50})

        process_list = [15, 15, 15, 15, 15, 15, 31.5, 49.5] + [30 for i in range(500)]
        # grey_list = [1, 2, 3, 4, 5, 6, 9, 12, 14, 15, 18, 20, 21, 24, 26, 27, 28, 29]
        for i in range(len(process_list)):
            worksheet.set_row(i, process_list[i])

        worksheet.conditional_format("A9:F500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("G9:G500", {"type": "formula", "criteria": "TRUE", "format":cf13})
        worksheet.conditional_format("H9:H500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("I9:I500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("J9:K500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("L9:L500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("M9:M500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("N9:O500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("P9:Q500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("R9:R500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("S9:S500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("T9:U500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("V9:W500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("X9:X500", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format("Y9:Y500", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("Z9:AC500", {"type": "formula", "criteria": "TRUE", "format":cf14})

        process_list = [14.68359375, 13.0, 20.20703125, 14.68359375, 13.0, 13.0, 21.68359375, 21.68359375, 14.68359375, 16.89453125, 14.68359375, 13.0, 13.0, 8.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i] + 7)
        

        process_list = ['編號', "製程名稱", "排放源", "是否屬於\n生質能源", ["類別", "(1~6)"], ["類別一\n排放類型", "(E,P,T,F)"], "年活動數據", "單位"] + [None for i in range(18)] + ["單一排放源排放當量小計(CO2e公噸/年)", "單一排放源生質燃料CO2排放當量小計(CO2e公噸/年)", "單一排放源占排放總量比(%)"]

        for i, con in enumerate(process_list):
            if con:
                if type(con) == str:
                    worksheet.merge_range(f"{chr_ord[i+1]}7:{chr_ord[i+1]}8", con, cf10)
                else:
                    for j, c in enumerate(con):
                        worksheet.write(f"{chr_ord[i+1]}{j+7}", c, cf10)

        process_list = ["溫室氣體#1", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        # "溫室氣體#2": ["溫室氣體#2", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"], "溫室氣體#3": ["溫室氣體#3", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]}
        
        worksheet.merge_range("I7:N7", "溫室氣體#1", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+9]}8", process_list[i], cf10)

        process_list = ["溫室氣體#2", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        worksheet.merge_range("O7:T7", "溫室氣體#2", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+15]}8", process_list[i], cf10)

        process_list = ["溫室氣體#3", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        worksheet.merge_range("U7:Z7", "溫室氣體#3", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+21]}8", process_list[i], cf10)

        for i in range(4, 505):
            formula = f"""=IF('2-定性盤查'!A{i}<>"",'2-定性盤查'!A{i},"")"""
            worksheet.write_formula(f"A{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!C{i}<>"",'2-定性盤查'!C{i},"")"""
            worksheet.write_formula(f"B{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!D{i}<>"",'2-定性盤查'!D{i},"")"""
            worksheet.write_formula(f"C{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!E{i}<>"",'2-定性盤查'!E{i},"")"""
            worksheet.write_formula(f"D{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!F{i}<>"",'2-定性盤查'!F{i},"")"""
            worksheet.write_formula(f"E{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!G{i}<>"",'2-定性盤查'!G{i},"")"""
            worksheet.write_formula(f"F{i+5}", formula)

            formula = f"""='3.1-活動數據'!R{i}"""
            worksheet.write_formula(f"G{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!X{i}<>"",IF('2-定性盤查'!X{i}<>0,'2-定性盤查'!X{i},""),"")"""
            worksheet.write_formula(f"I{i+5}", formula)

            formula = f"""='3.2-排放係數'!D{i-1}"""
            worksheet.write_formula(f"J{i+5}", formula)

            formula = f"""='3.2-排放係數'!E{i-1}"""
            worksheet.write_formula(f"K{i+5}", formula)

            formula = f"""=IF(I{i+5}="","",G{i+5}*J{i+5})"""
            worksheet.write_formula(f"L{i+5}", formula)

            formula = f"""=IF(L{i+5}="","",L{i+5}*M{i+5})"""
            worksheet.write_formula(f"N{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!Y{i}<>"",IF('2-定性盤查'!Y{i}<>0,'2-定性盤查'!Y{i},""),"")"""
            worksheet.write_formula(f"O{i+5}", formula)

            formula = f"""='3.2-排放係數'!H{i-1}"""
            worksheet.write_formula(f"P{i+5}", formula)

            formula = f"""='3.2-排放係數'!I{i-1}"""
            worksheet.write_formula(f"Q{i+5}", formula)

            formula = f"""=IF(O{i+5}="","",$G{i+5}*P{i+5})"""
            worksheet.write_formula(f"R{i+5}", formula)

            formula = f"""=IF(R{i+5}="","",R{i+5}*S{i+5})"""
            worksheet.write_formula(f"T{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!Z{i}<>"",IF('2-定性盤查'!Z{i}<>0,'2-定性盤查'!Z{i},""),"")"""
            worksheet.write_formula(f"U{i+5}", formula)

            formula = f"""='3.2-排放係數'!L{i-1}"""
            worksheet.write_formula(f"V{i+5}", formula)

            formula = f"""='3.2-排放係數'!M{i-1}"""
            worksheet.write_formula(f"W{i+5}", formula)

            formula = f"""=IF(U{i+5}="","",$G{i+5}*V{i+5})"""
            worksheet.write_formula(f"X{i+5}", formula)

            formula = f"""=IF(X{i+5}="","",X{i+5}*Y{i+5})"""
            worksheet.write_formula(f"Z{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!E{i}="是",IF(I{i+5}="CO2",SUM(T{i+5},Z{i+5}),SUM(N{i+5},T{i+5},Z{i+5})),IF(SUM(N{i+5},T{i+5},Z{i+5})<>0,SUM(N{i+5},T{i+5},Z{i+5}),""))"""
            worksheet.write_formula(f"AA{i+5}", formula)

            formula = f"""=IF('2-定性盤查'!E{i}="是",IF(I{i+5}="CO2",N{i+5},""),"")"""
            worksheet.write_formula(f"AB{i+5}", formula)

            formula = f"""=IF(AA{i+5}<>"",AA{i+5}/'6-彙總表'!$J$5,"")"""
            worksheet.write_formula(f"AC{i+5}", formula)

        # ------------------------- 3.1-活動數據 -----------------------------
        df.to_excel(writer, sheet_name="3.1-活動數據", )
        worksheet = writer.sheets["3.1-活動數據"]

        cf15 = workbook.add_format({
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
        })
        cf16 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#DAEEF3",  # 橙色的 Accent 6
            "border": 1,
            "text_wrap": True
        })
        cf17 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })

        process_list = [2.41796875, 15.7890625, 31.89453125, 14.89453125, 12.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 12.68359375, 7.68359375, 9.1015625, 12.1015625, 8.68359375, 12.3125, 12.68359375, 6.5234375, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        worksheet.write("B1", "活動數據彚總", workbook.add_format({"bold": True, "font_size": 14, "pattern": 1, "bg_color": "#FFFFFF", "align": "center"}))

        process_list = ["廠區歸屬", "製程名稱", "排放源", "單位"]
        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+2]}2:{chr_ord[i+2]}3", process_list[i], cf6)

        worksheet.write("F2", "年度", cf6)
        worksheet.merge_range("G2:S2", "2022", cf6)
        process_list = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月", "年總量", "佐證資料"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[6+i]}3", process_list[i], cf6)
            if i < 12:
                worksheet.merge_range(f"{chr_ord[(i+1)*2]}29:{chr_ord[(i+1)*2+1]}29", process_list[i], cf8)

        worksheet.conditional_format("B4:D25", {"type": "formula", "criteria": "TRUE", "format":cf8})
        worksheet.conditional_format("E4:E25", {"type": "formula", "criteria": "TRUE", "format":cf13})
        worksheet.conditional_format("F4:Q25", {"type": "formula", "criteria": "TRUE", "format":cf16})
        
        for i in range(4, 26):
            formula = f"""=IF('2-定性盤查'!B{i}="","",'2-定性盤查'!B{i})"""
            worksheet.write_formula(f"B{i}", formula)

            formula = f"""=IF('2-定性盤查'!C{i}="","",'2-定性盤查'!C{i})"""
            worksheet.write_formula(f"C{i}", formula)

            formula = f"""=IF('2-定性盤查'!D{i}="","",'2-定性盤查'!D{i})"""
            worksheet.write_formula(f"D{i}", formula)

            formula = f"""=SUM(F{i}:Q{i})"""
            worksheet.write_formula(f"R{i}", formula)

        # ------------------------- 3.2-排放係數 -----------------------------
        df.to_excel(writer, sheet_name="3.2-排放係數", )
        worksheet = writer.sheets["3.2-排放係數"]

        process_list = [8.3125, 23.7890625, 13.0, 17.3125, 16.1015625, 18.1015625, 18.3125, 13.3125, 13.3125, 15.68359375, 16.3125, 14.0, 14.5234375, 15.3125, 17.0, 10.41796875, 10.41796875, 11.1015625, 17.41796875, 9.68359375, 9.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        worksheet.merge_range("B1:B2", "排放源", cf6)
        worksheet.merge_range("C1:C2", "排放源", cf6)
        
        worksheet.merge_range(f"D1:G1", "CO2排放係數", cf6)
        worksheet.merge_range("H1:K1", "CH4排放係數", cf6)
        worksheet.merge_range("L1:O1", "N2O排放係數", cf6)
        worksheet.merge_range("P1:T1", "其他排放係數", cf6)

        process_list = ["數值", "單位", "來源分類", "來源說明", "數值", "單位", "係數種類", "來源說明", "數值", "單位", "係數種類", "來源說明", "排放源", "數值", "單位", "係數種類", "來源說明"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+4]}2", process_list[i], cf6)

        worksheet.conditional_format("B3:C501", {"type": "formula", "criteria": "TRUE", "format":cf6})
        worksheet.conditional_format("D3:T501", {"type": "formula", "criteria": "TRUE", "format":cf17})

        # ------------------------- 3.3-冷媒設備清單(冷氣、飲水機、冰箱)2022 -----------------------------
        df.to_excel(writer, sheet_name="3.3-冷媒設備清單(冷氣、飲水機、冰箱)2022", )
        worksheet = writer.sheets["3.3-冷媒設備清單(冷氣、飲水機、冰箱)2022"]

        cf18 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#DAEEF3",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })

        process_list = [23.0, 11.5234375, 19.68359375, 12.20703125, 8.41796875, 9.7890625, 9.89453125, 7.89453125, 18.7890625, 49.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        process_list = ["項次", "類別", "設備廠牌", "設備型號", "冷媒類型", "原始填充量", "填充量單位", "年度區分", "佐證來源", "照片", "數量"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+1]}2", process_list[i], cf6)

        ### 補資料
        ### 補表格
        ### 補照片

        # ------------------------- 3.4-上游產品運輸 -----------------------------

        df.to_excel(writer, sheet_name="3.4-上游產品運輸", )
        worksheet = writer.sheets["3.4-上游產品運輸"]

        process_list = [13.0, 13.0, 24.89453125, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 11.68359375, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        process_list = ["編號", "原料名稱", "運輸起點", "運輸距離", "單位"]

        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+1]}1:{chr_ord[i+1]}2", process_list[i], cf6)

        worksheet.write("F1", "年度", cf6)
        worksheet.write("G1", today.year, cf6)
        worksheet.merge_range("H1:Q1", "每月總重量數據", cf6)
        worksheet.conditional_format("R1:U1", {"type": "formula", "criteria": "TRUE", "format":cf6})

        process_list = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月", "年總量", "單位", "備住", "總延噸公里"]
        
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+6]}2", process_list[i], cf6)

        ### 補資料

        # ------------------------- 3.5-下游產品運輸 -----------------------------
        df.to_excel(writer, sheet_name="3.5-下游產品運輸", )
        worksheet = writer.sheets["3.5-下游產品運輸"]

        process_list = [16.89453125, 25.7890625, 8.89453125, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 19.3125, 14.68359375, 11.1015625, 8.89453125, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        process_list = ["", "目的地/中繼站", "運輸終點", "運輸距離", "單位"]

        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+1]}1:{chr_ord[i+1]}2", process_list[i], cf6)

        worksheet.write("F1", "", cf6)
        worksheet.write("F2", "", cf6)
        worksheet.write("G1", "年度", cf6)
        worksheet.write("H1", today.year, cf6)
        worksheet.merge_range("I1:R1", "每月總重量數據", cf6)
        worksheet.conditional_format("S1:V1", {"type": "formula", "criteria": "TRUE", "format":cf6})

        process_list = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月", "年總量", "單位", "備住", "總延噸公里"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+7]}2", process_list[i], cf6)

        ### 補資料

        # ------------------------- 4-數據品質管理 -----------------------------

        df.to_excel(writer, sheet_name="4-數據品質管理", )
        worksheet = writer.sheets["4-數據品質管理"]

        process_list = [4.68359375, 13.0, 23.1015625, 13.20703125, 18.68359375, 17.0, 23.41796875, 10.68359375, 25.5234375, 7.41796875, 13.1015625, 13.68359375, 7.89453125, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)): 
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])
        
        worksheet.merge_range("B2:B3", "編號", cf6)
        worksheet.merge_range("C2:C3", "製程名稱", cf6)
        worksheet.merge_range("D2:D3", "排放源", cf6)
        worksheet.merge_range("E2:H2", "排放量", cf6)
        worksheet.merge_range("I2:J2", "排放量", cf6)
        worksheet.merge_range("K2:N2", "排放量", cf6)
        process_list = ["活動數據種類", "活動數據種類等級", "活動數據可信種類(儀器校正誤差等級)", "活動數據可信等級", "排放係數種類", "係數種類等級", "單一排放源數據誤差等級", "單一排放源占排放總量比(%)", "評分區間範圍", "排放量佔比加權平均"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+5]}3", process_list[i], cf6)

        ### 補資料

        # ------------------------- 5-不確定性之評估 -----------------------------
        df.to_excel(writer, sheet_name="5-不確定性之評估", )
        worksheet = writer.sheets["5-不確定性之評估"]

        process_list = [9.0, 8.3125, 20.41796875, 13.1015625, 11.0, 10.41796875, 13.89453125, 10.7890625, 11.7890625, 9.41796875, 10.41796875, 14.0, 13.41796875, 13.68359375, 10.41796875, 11.1015625, 13.1015625, 13.0, 13.7890625, 14.5234375, 14.20703125, 9.0, 12.5234375, 14.5234375, 13.5234375, 13.68359375, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        worksheet.merge_range("B2:B4", "編號", cf6)
        worksheet.merge_range("C2:C4", "製程名稱", cf6)
        worksheet.merge_range("D2:D4", "排放源", cf6)
        worksheet.merge_range("E2:G2", "活動數據之不確定性", cf6)
        worksheet.merge_range("H2:N2", "溫室氣體#1之排放係數不確定性", cf6)
        worksheet.merge_range("O2:U2", "溫室氣體#2之排放係數不確定性", cf6)
        worksheet.merge_range("V2:AB2", "溫室氣體#3之排放係數不確定性", cf6)
        worksheet.merge_range("AC2:AD3", "單一排放源不確定性", cf6)

        process_list = ["95%信賴區間之下限", "95%信賴區間之上限", "數據來源"]
        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+5]}3:{chr_ord[i+5]}4", process_list[i], cf6)

        process_list = ["溫室氣體", "溫室氣體排放當量(噸CO2e/年)", "95%信賴區間之下限", "95%信賴區間之上限", "係數不確定性資料來源"]

        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+8]}3:{chr_ord[i+8]}4", process_list[i], cf6)
            worksheet.merge_range(f"{chr_ord[i+15]}3:{chr_ord[i+15]}4", process_list[i], cf6)
            worksheet.merge_range(f"{chr_ord[i+22]}3:{chr_ord[i+22]}4", process_list[i], cf6)

        worksheet.merge_range("M3:N3", "單一溫室氣體不確定性", cf6)
        worksheet.merge_range("T3:U3", "單一溫室氣體不確定性", cf6)
        worksheet.merge_range("AA3:AB3", "單一溫室氣體不確定性", cf6)

        worksheet.write("M4", "95%信賴區間之下限", cf6)
        worksheet.write("N4", "95%信賴區間之上限", cf6)
        worksheet.write("T4", "95%信賴區間之下限", cf6)
        worksheet.write("U4", "95%信賴區間之上限", cf6)
        worksheet.write("AA4", "95%信賴區間之下限", cf6)
        worksheet.write("AB4", "95%信賴區間之上限", cf6)

        worksheet.write("AC4", "95%信賴區間之下限", cf6)
        worksheet.write("AD4", "95%信賴區間之上限", cf6)
        
        ### 補資料

        # ------------------------- 6-彙總表 -----------------------------
        df.to_excel(writer, sheet_name="6-彙總表", )
        worksheet = writer.sheets["6-彙總表"]

        cf19 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })

        process_list = [14.68359375, 13.0, 20.20703125, 14.68359375, 13.0, 13.0, 21.68359375, 21.68359375, 14.68359375, 16.89453125, 14.68359375, 13.0, 13.0, 8.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        worksheet.merge_range("A3:K3", "彙整表一、全廠七大溫室氣體排放量統計表", cf8)
        worksheet.merge_range("A4:B4", "", cf8)

        process_list = ["CO2", "CH4", "N2O", "HFCs", "PFCs", "SF6", "NF3", "七種溫室氣體年總排放當量", "生質排放當量"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}4", process_list[i], cf19)

        ### 補資料

        worksheet.merge_range("A9:J9", "彙整表二、類別一七大溫室氣體排放量統計表", cf8)
        worksheet.merge_range("A10:B10", "", cf8)

        process_list = ["CO2", "CH4", "N2O", "HFCs", "PFCs", "SF6", "NF3", "七種溫室氣體年總排放當量", "生質排放當量"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}10", process_list[i], cf19)

        ### 補資料

        worksheet.merge_range("A14:L14", "彙整表三、全廠溫室氣體類別及類別一排放型式排放量統計表", cf8)
        worksheet.merge_range("A15:B16", "", cf8)
        worksheet.merge_range("C15:F15", "類別一", cf8)

        process_list = ["類別2", "類別3", "類別4", "類別5", "類別6", "總排放當量"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+7]}15", process_list[i], cf8)

        process_list = ["固定排放", "製程排放", "移動排放", "逸散排放", "能源間接排放", "運輸之間接排放", "上游之間接排放", "下游之間接排放", "其他間接排放", ""]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}16", process_list[i], cf8)

        ### 補資料

        worksheet.merge_range("B22:E22", "彙整表四、全廠溫室氣體數據等級評分結果", cf8)
        
        process_list = ["等級", "第一級", "第二級", "第三級"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+2]}23", process_list[i], cf8)

        process_list = ["評分範圍", "X<10分", "10分≦X<19分", "19≦X≦27分"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+2]}24", process_list[i], cf8)

        ### 補資料

        worksheet.merge_range("G22:L22", "彙整表五、溫室氣體不確定性量化評估結果", cf8)
        worksheet.write("G23", "進行不確定性評估之排放量絕對值加總", cf19)
        worksheet.write("H23", "排放總量絕對值加總", cf19)
        worksheet.merge_range("I23:L24", "本清冊之總不確定性", cf19)
        worksheet.merge_range("G25:H25", "進行不確定性評估之排放量佔總排放量之比例", cf19)
        worksheet.merge_range("I25:J25", "95%信賴區間下限", cf19)
        worksheet.merge_range("K25:L25", "95%信賴區間上限", cf19)

        ### 補資料

        # ------------------------- 附表一 -----------------------------
        df.to_excel(writer, sheet_name="附表一", )
        worksheet = writer.sheets["附表一"]

        cf20 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFF99",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })

        process_list = [10.1015625, 13.0, 42.1015625, 36.1015625]
        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        process_list = ["序號", "行業代碼", "行業名稱", "序號. 行業代碼 行業名稱"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+1]}1", process_list[i], cf20)

        df_list = pd.read_excel("溫室氣體盤查清冊v1.4.xlsx", sheet_name="附表一").values.tolist()
        for i in range(len(df_list)):
            for j in range(len(df_list[i])):
                if j == 1:
                    if len(str(df_list[i][j])) == 3:
                        df_list[i][j] = "0" + str(df_list[i][j])

                worksheet.write(f"{chr_ord[j+1]}{i+2}", 
                df_list[i][j], cf8)

        # ------------------------- 含氟氣體之GWP值 -----------------------------
        df.to_excel(writer, sheet_name="含氟氣體之GWP值", )
        worksheet = writer.sheets["含氟氣體之GWP值"]

        cf21 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFF99",
            "border": 1,
            "text_wrap": True,
            "bold": True,
        })
        cf22 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
            "text_wrap": True,
            "valign": "vcenter",
        })

        process_list = [16.1015625, 71.7890625, 19.3125, 13.0, 13.0, 13.0, 13.0, 26.20703125, 8.7890625, 8.20703125, 11.68359375, 9.89453125, 9.89453125, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])
        
        worksheet.merge_range("A1:A2", "代碼", cf20)
        worksheet.merge_range("B1:H1", "預設GWP值", cf20)
        worksheet.merge_range("I1:J1", "自定 GWP 值", cf20)
        worksheet.merge_range("K1:K2", "Montreal Protocol\n*", cf20)
        
        process_list = ["溫室氣體化學式", "IPCC第二次評估報告（1995）", "IPCC第三次評估報告(2001)", "IPCC第四次評估報告(2007)", "IPCC第五次評估報告(2013)", "IPCC第六次評估報告(2021)", "備註", "數值", "來源"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+2]}2", process_list[i], cf20)

        process_list = [["180014", "CO2二氧化碳", "1", "1", "1", "1", "1", "", "", "", ""], ["180177", "CH4甲烷", "21", "23", "25", "28", "28", "", "", "", ""], ["GG1802", "N2O氧化亞氮", "310", "296", "298", "265", "273", "", "", "", ""]]
        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+3}", process_list[i][j], cf8)
        
        worksheet.merge_range("A6:H6", "Chlorofluorocarbons, 氟氯碳化物", cf21)
        worksheet.write("I6", "", cf8)
        worksheet.write("J6", "", cf8)
        worksheet.write("K6", "", cf8)

        process_list = [
            ["", "CFC-11，CCl3F", "3800", "4600", "4,750", "4,460", "", "", "", "", "*"], 
            ["", "CFC-12，CCl2F2", "8100", "10600", "10,900", "10,200", "", "", "", "", "*"], 
            ["", "CFC-13，CClF3", "", "14000", "14,400", "13,900", "", "", "", "", "*"], 
            ["", "CFC-113，CCl2FCClF2", "4800", "6000", "6,130", "5,820", "", "", "", "", "*"], 
            ["", "CFC-114，CClF2CClF2", "", "9800", "10,000", "8,590", "", "", ",", "", "*"], 
            ["", "CFC-115，CClF2CF3", "", "7200", "7,370", "7,670", "", "", "", "", "*"]
        ]
        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+7}", process_list[i][j], cf8)

        worksheet.merge_range("A13:H13", "Hydrofluorocarbons, HFCs, 氫氟碳化物", cf21)
        worksheet.write("I13", "", cf8)
        worksheet.write("J13", "", cf8)
        worksheet.write("K13", "", cf8)

        process_list = [
            ["GG1840", "HFC-23/R-23三氟甲烷，CHF3", "11,700", "12,000", "14,800", "12,400", "14,600", "", "", "", ""],
            ["GG1839", "HFC-32/R-32二氟甲烷，CH2F2", "650", "550", "675", "677", "771", "", "", "", ""],
            ["GG1838", "HFC-41一氟甲烷，CH3F", "150", "97", "92", "116", "135", "", "", "", ""],
            ["GG1837", "HFC-125/R-125，1,1,1,2,2-五氟乙烷，C2HF5", "2,800", "3,400", "3,500", "3,170", "3,740", "", "", "", ""],
            ["GG1836", "HFC-134，1,1,2,2-四氟乙烷，C2H2F4", "1,000", "1,100", "1,100", "1,120", "1,260", "", "", "", ""],
            ["GG1835", "HFC-134a/R-134a，1,1,1,2-四氟乙烷，C2H2F4", "1,300", "1,300", "1,430", "1,300", "1,530", "", "", "", ""],
            ["GG1834", "HFC-143，1,1,2-三氟乙烷，CHF2CH2F", "300", "330", "353", "328", "364", "", "", "", ""],
            ["GG1833", "HFC-143a/R-143a，1,1,1-三氟乙烷，C2H3F3", "3,800", "4,300", "4,470", "4,800", "5,810", "", "", "", ""],
            ["GG1832", "HFC-152，1,2-二氟乙烷，CH2FCH2F", "-", "43", "53", "16", "22", "", "", "", ""],
            ["GG1831", "HFC-152a/R-152a，1,1-二氟乙烷，C2H4F2", "140", "120", "124", "138", "164", "", "", "", ""],
            ["GG1830", "HFC-161，一氟乙烷，CH3CH2F", "-", "12", "12", "4", "5", "", "", "", ""],
            ["", "HFC-227ca，CF3CF2CHF2", "─", "─", "─", "2,640", "", "", "", "", ""],
            ["GG1829", "HFC-227ea，1,1,1,2,3,3,3-七氟丙烷，CF3CHFCF3", "2,900", "3,500", "3,220", "3,350", "3,600", "", "", "", ""],
            ["GG1828", "HFC-236cb，1,1,1,2,2,3-六氟丙烷，CH2FCF2CF3", "-", "1,300", "1,340", "1,210", "1,350", "", "", "", ""],
            ["GG1827", "HFC-236ea，1,1,1,2,3,3,3-六氟丙烷，CHF2CHFCF3", "-", "1,200", "1,370", "1,330", "1,500", "", "", "", ""],
            ["GG1826", "HFC-236fa，1,1,1,3,3,3-六氟丙烷，C3H2F6", "6,300", "9,400", "9,810", "8,060", "8,690", "", "", "", ""],
            ["GG1825", "HFC-245ca，1,1,2,2,3-五氟丙烷，CH2FCF2CHF2", "560", "640", "693", "716", "787", "", "", "", "", ""],
            ["", "HFC-245cb，CF3CF2CF3", "", "", "", "4,620", "", "", "", "", ""], 
            ["", "HFC-245ea，CHF2CHFCHF2", "", "", "", "235", "", "", "", "", ""], 
            ["", "HFC-245eb，CH2FCHFCF3", "", "", "", "290", "", "", "", "", ""], 
            ["GG1824", "HFC-245fa，1,1,1,3,3-五氟丙烷，CHF2CH2CF3", "-", "950", "1,030", "858", "962", "", "", "", ""],
            ["", "HFC-263fb，CH3CH2CF3", "", "", "", "76", "", "", "", "", ""],
            ["", "HFC-272ca，CH3CF2CH3", "", "", "", "144", "", "", "", "", ""],
            ["", "HFC-329p，CHF2CF2CF2CF3", "", "", "", "2,360", "", "", "", "", ""],
            ["GG1823", "HFC-365mfc，1,1,1,3,3-五氟丁烷，CF3CH2CF2CH3", "-", "890", "794", "804", "914", "", "", "", ""],
            ["GG1822", "HFC-43-10mee，1,1,1,2,2,3,4,5,5,5-十氟戊烷，CF3CHFCHFCF2CF3", "1,300", "1,500", "1,640", "1,650", "1,600", "", "", "", ""],
            ["", "HFC-1132a，CH2=CF2", "", "", "", "<1", "", "", "", "", ""],
            ["", "HFC-1141，CH2=CHF", "", "", "", "<1", "", "", "", "", ""],
            ["", "(Z)-HFC-1225ye，CF3CF=CHF(Z)", "", "", "", "<1", "", "", "", "", ""],
            ["", "(E)-HFC-1225ye，CF3CF=CHF(E)", "", "", "", "<1", "", "", "", "", ""],
            ["", "(Z)-HFC-1234ze，CF3CH=CHF(Z)", "", "", "", "<1", "", "", "", "", ""],
            ["", "HFC-1234yf，CF3CF=CH2", "", "", "", "<1", "", "", "", "", ""],
            ["", "(E)-HFC-1234ze，trans-CF3CH=CHF", "", "", "", "<1", "", "", "", "", ""],
            ["", "(Z)-HFC-1336，CF3CH=CHCF3(Z)", "", "", "", "2", "", "", "", "", ""],
            ["", "HFC-1243zf，CF3CH=CH2", "", "", "", "<1", "", "", "", "", ""],
            ["", "HFC-1345zfc，C2F5CH=CH2", "", "", "", "<1", "", "", "", "", ""],
            ["" ,"3,3,4,4,5,5,6,6,6-Nonafluorohex-1-ene，C4F9CH=CH2", "", "", "", "<1", "", "", "", "", ""],
            ["", "3,3,4,4,5,5,6,6,7,7,8,8,8-Tridecafluorooct-1-ene，C6F13CH=CH2", "", "", "", "<1", "", "", "", "", ""],
            ["", "3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,10-Hep-tadecafluorodec-1-ene，C8F17CH=CH2", "", "", "", "<1", "", "", "", "", ""],
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+14}", process_list[i][j], cf8)

        worksheet.merge_range("A53:H53", "Hydrochlorofluorocarbons, HCFCs, 氫氟氯碳化物", cf21)
        worksheet.write("I53", "", cf8)
        worksheet.write("J53", "", cf8)
        worksheet.write("K53", "", cf8)

        process_list = [
            ["", "HCFC-21，CHCl2F", "", "210", "151", "148", "", "", "", "", "*"], 
            ["GG1841", "HCFC-22，CHF2Cl", "─", "1,700", "1,810", "1,760", "", "", "", "", "*"],
            ["", "HCFC-122，CHCl2CF2Cl", "", "", "", "59", "", "", "", "", ""], 
            ["", "HCFC-122a，CHFClCFCl2", "", "", "", "258", "", "", "", "", ""], 
            ["", "HCFC-123，CHCl2CF3", "90", "120", "77", "79", "", "", "", "", "*"], 
            ["", "HCFC-123a，CHClFCF2Cl", "", "", "", "370", "", "", "", "", ""], 
            ["", "HCFC-124，CHClFCF3", "470", "620", "609", "527", "", "", "", "", "*"],         
            ["", "HCFC-132c，CH2FCFCl2", "", "", "", "338", "", "", "", "", ""], 
            ["", "HCFC-141b，CH3CCl2F", "600", "700", "725", "782", "", "", "", "", "*"], 
            ["", "HCFC-142b，CH3CClF2", "1800", "2400", "2,310", "1,980", "", "", "", "", "*"], 
            ["", "HCFC-225ca，CHCl2CF2CF3", "", "1800", "122", "127", "", "", "", "", "*"], 
            ["", "HCFC-225cb，CHClFCF2CClF2", "", "620", "595", "525", "", "", "", "", "*"], 
            ["", "(E)-1-Chloro-3,3,3-trifluoroprop-1-ene，trans-CF3CH=CHCl", "", "", "", "1", "", "", "", "", ""]
        ]
        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+54}", process_list[i][j], cf8)

        worksheet.merge_range("A67:H67", "Chlorocarbons and Hydrochlorocarbons", cf21)
        worksheet.write("I67", "", cf8)
        worksheet.write("J67", "", cf8)
        worksheet.write("K67", "", cf8)

        process_list = [
            ["", "Methyl chloroform，CH3CCl3", "100", "140", "146", "160", "", "", "", "", ""],
	        ["", "Carbon tetrachloride，CCl4", "1400", "1800", "1,400", "1,730", "", "", "", "", "*"],
	        ["", "Methyl chloride，CH3Cl", "4", "16", "13", "12", "", "", "", "", ""],			
	        ["", "Methylene chloride ，CH2Cl2", "9", "10", "8.7", "9", "", "", "", "", ""],					
	        ["", "Chloroform ，CHCl3", "", "30", "31", "16", "", "", "", "", ""],			
	        ["", "1,2-Dichloroethane，CH2ClCH2Cl", "", "", "", "<1", "", "", "", "", ""]
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+68}", process_list[i][j], cf8)

        worksheet.merge_range("A74:H74", "Bromocarbons, Hydrobromocarbons and Halons", cf21)
        worksheet.write("I74", "", cf8)
        worksheet.write("J74", "", cf8)
        worksheet.write("K74", "", cf8)

        process_list = [
            ["", "Methyl bromide，CH3Br", "", "5", "5", "2", "", "", "", "", "*"],
            ["", "Methylene bromide，CH2Br2", "", "1", "1.54", "1", "", "", "", "", "*"],
            ["", "Halon-1201，CHBrF2", "", "470", "404", "376", "", "", "", "", ""],
            ["", "Halon-1202，CBrF2", "", "", "", "231", "", "", "", "", ""],
            ["", "Halon-1211，CBrClF2", "", "1300", "1,890", "1,750", "", "", "", "", "*"],
            ["", "Halon-1301，CBrF3", "5400", "6900", "7,140", "6,290", "", "", "", "", "*"],
            ["", "Halon-2301，CH2BrCF3", "", "", "", "173", "", "", "", "", ""],
            ["", "Halon-2311/Halothane，CHBrClCF3", "", "", "", "41", "", "", "", "", ""],
            ["", "Halon-2401，CHFBrCF3", "", "", "", "184", "", "", "", "", ""],
            ["", "Halon-2402，CBrF2CBrF2", "", "", "1,640", "1,470", "", "", "", "", "*"],
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+75}", process_list[i][j], cf8)

        worksheet.merge_range("A85:H85", "Bromocarbons, Hydrobromocarbons and Halons", cf21)
        worksheet.write("I85", "", cf8)
        worksheet.write("J85", "", cf8)
        worksheet.write("K85", "", cf8)

        process_list = [
            ["180123", "NF3，三氟化氮", "─", "10,800", "17,200", "16,100", "17,400", "", "", "", ""],
            ["180122", "SF6，六氟化硫", "23,900", "22,200", "22,800", "23,500", "25,200", "", "", "", ""],
            ["", "Trifluoromethyl sulphur pentafluoride ，SF5CF3", "", "", "17,700", "17,400", "", "", "", "", ""],
            ["", "Sulphuryl fluoride，SO2F2", "", "", "", "4,090", "", "", "", "", ""],
            ["GG1803", "PFC-14，四氟化碳，CF4", "6,500", "5,700", "7,390", "6,630", "7,380", "", "", "", ""],		
            ["GG1804", "PFC-116，六氟乙烷，C2F6", "9,200", "11,900", "12,200", "11,100", "12,400", "", "", "", ""],	
            ["", "Perfluorocyclopropane  (PFC-c216)，c-C3F6", "", "", ">17,340","9,200", "", "", "", "", ""],	
            ["GG1809", "PFC-218 ，C3F8，全氟丙烷", "7,000", "8,600", "8,830", "8,900", "9,290", "", "", "", ""],			
            ["GG1808", "PFC-318 。c-C4F8，八氟環丁烷", "8,700", "10,000", "10,300", "9,540", "", "", "", "", ""],			
            ["GG1807", "C4F10，全氟丁烷", "7,000", "8,600", "8,860", "9,200", "10,000", "", "", "", ""],
            ["GG1886", "Perfluorocyclopentene，c-C5F8，八氟環戊烯", "", "", "", "2", "", "", "", "", ""],
            ["GG1806", "PFC-4-1-12，C5F12 (n-C5F12)，全氟戊烷", "7,500", "8,900", "9,160", "8,550", "9,220", "", "", "", ""],
            ["GG1805", "PFC-5-1-14 ，C6F14 (n-C6F14)，全氟己烷", "7,400", "9,000", "9,300", "7,910", "8,620", "", "", "", ""],
            ["", "PFC-61-16，n-C7F16", "", "", "", "7,820", "", "", "", "", ""],
            ["", "PFC-71-18，C8F18", "", "", "", "7,620", "", "", "", "", ""],
            ["", "PFC-9-1-18 ，C10F18", "", "", ">7,500", "7,190", "", "", "", "", ""],
            ["", "Perfluorodecalin (cis)，Z-C10F18", "", "", "", "7,240", "", "", "", "", ""],
            ["", "Perfluorodecalin (trans)，E-C10F18", "", "", "", "6,290", "", "", "", "", ""],
            ["", "PFC-1114，CF2=CF2", "", "", "", "<1", "", "", "", "", ""],
            ["", "PFC-1216，CF3CF=CF2", "", "", "", "<1", "", "", "", "", ""],
            ["", "Perfluorobuta-1,3-diene，CF2=CFCF=CF2", "", "", "", "<1", "", "", "", "", ""],	
            ["", "Perfluorobut-1-ene，CF3CF2CF=CF2", "", "", "", "<1", "", "", "", "", ""],
            ["", "Perfluorobut-2-ene，", "", "", "", "2", "", "", "", "", ""]
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+86}", process_list[i][j], cf8)

        worksheet.merge_range("A109:H109", "混合冷媒", cf21)
        worksheet.write("I109", "", cf8)
        worksheet.write("J109", "", cf8)
        worksheet.write("K109", "", cf8)

        process_list = [
            ["GG1821", "R-401A，HCFC-22/HFC-152a/HCFC-124 (53.0/13.0/34.0)", "1,126", "1,127", "1,182", "1,130", "1,263"],
            ["GG1820", "R-401B，HCFC-22/HFC-152a/HCFC-124 (61.0/11.0/28.0)", "1,223", "1,224", "1,288", "1,236", "1,381"],
            ["GG1842", "R-401C，HCFC-22/HFC-152a/HCFC-124 (33.0/15.0/52.0)", "899", "901", "933", "876", "982"],
            ["GG1843", "R-402A，HFC-125/HC-290/HCFC-22 (60.0/2.0/38.0)", "2,326", "2,686", "2,788", "2,571", "2,989"],
            ["GG1844", "R-402B，HFC-125/HC-290/HCFC-22 (38.0/2.0/60.0)", "2,084", "2,312", "2,416", "2,261", "2,597"],
            ["GG1845", "R-403A，HC-290/HCFC-22/PFC-218 (5.0/75.0/20.0)", "1,415", "1,415", "1,534", "3,100", "3,328"],
            ["GG1846", "R-403B，HC-290/HCFC-22/PFC-218 (5.0/56.0/39.0)", "3,682", "3,682", "4,457", "4,457", "4,721"],
            ["GG1819", "R-404A，HFC-125/HFC-143a/HFC-134a (44.0/52.0/4.0)", "3,260", "3,784", "3,922", "3,943", "4,728"],
            ["GG1847", "R-405A，HCFC-22/ HFC-152a/ HCFC-142b/PFC-318 (45.0/7.0/5.5/42.5)", "4,571", "5,155", "5,328", "4,965", "941"],
            ["GG1848", "R-406A，HCFC-22/HC-600a/HCFC-142b (55.0/14.0/41.0)", "1,673", "1,919", "1,943", "1,780", "1,431"],
            ["GG1818", "R-407A，HFC-32/HFC-125/HFC-134a (20.0/40.0/40.0)", "1,770", "1,990", "2,107", "1,923", "2,262"],
            ["GG1817", "R-407B，HFC-32/HFC-125/HFC-134a (10.0/70.0/20.0)", "2,285", "2,695", "2,804", "2,547", "3,001"],
            ["GG1816", "R-407C，HFC-32/HFC-125/HFC-134a (23.0/25.0/52.0)", "1,526", "1,653", "1,774", "1,624", "1,908"],
            ["GG1849", "R-407D，HFC-32/HFC-125/HFC-134a (15.0/15.0/70.0)", "1,428", "1,503", "1,627", "1,487", "1,748"],
            ["GG1850", "R-407E，HFC-32/HFC-125/HFC-134a (25.0/15.0/60.0)", "1,363", "1,428", "1,552", "1,425", "1,672"],
            ["GG1815", "R-408A，HFC-125/HFC-143a/HCFC-22 (7.0/46.0/47.0)", "2,743", "3,015", "3,152", "3,257", "3,856"],
            ["GG1851", "R-409A，HCFC-22/HCFC-124/HCFC-142b (60.0/25.0/15.0)", "1,442", "1,535", "1,585", "1,485", "1,454"], 
            ["GG1852", "R-409B，HCFC-22/HCFC-124/HCFC-142b (65.0/25.0/10.0)", "1,437", "1,500", "1,560", "1,474", "1,509 "],
            ["GG1814", "R-410A，HFC-32/HFC-125 (50.0/50.0)", "1,725", "1,975", "2,088", "1,924", "2,256"],
            ["GG1853", "R-410B，HFC-32/HFC-125 (45.0/55.0)", "1,618", "1,833", "1,946", "2,048", "2,404"],
            ["GG1854", "R-411A，HC-1270/HCFC-22/HFC-152a (1.5/87.5/11.0)", "1,503", "1,501", "1,597", "1,555", "1,733"],
            ["GG1855", "R-411B，HC-1270/HCFC-22/HFC-152a (3.0/94.0/3.0)", "1,602", "1,602", "1,705", "1,659", "1,847"],
            ["GG1856", "R-411C，HC-1270/HCFC-22/HFC-152a (3.0/95.5/1.5)", "1,626", "1,625", "1,730", "1,683", "1,874"],
            ["GG1857", "R-412A，HCFC-22/PFC-218/HCFC-142b (70.0/5.0/25.0)", "1,990", "2,140", "2,286", "2,172", "2,052"],
            ["GG1813", "R-413A，PFC-218/HFC-134a/HC-600a (9.0/88.0/3.0)", "1,774", "1,774", "2,053", "1,945", "2,183"],
            ["GG1858", "R-414A，HCFC-22/HCFC-124/HC-600a/HCFC-142b (51.0/28.5/4.0/16.5)", "1,338", "1,440", "1,478", "1,375", "1,312"],
            ["GG1859", "R-414B，HCFC-22/HCFC-124/HC-600a/HCFC-142b (50.0/39.0/1.5/9.5)", "1,259", "1,320", "1,362", "1,274", "1,295"],
            ["GG1860", "R-415A，HCFC-22/HFC-152a (82.0/18.0)", "1,419", "1,416", "1,507", "1,468", "1,637"],
            ["GG1861", "R-415B，HCFC-22/HFC-152a (25.0/75.0)", "530", "515", "546", "544", "613"],
            ["GG1862", "R-416A，HFC-134a/HCFC-124/HC-600 (59.0/39.5/1.5)", "1,008", "1,012", "1,084", "975", "1,139 "],
            ["GG1812", "R-417A，HFC-125/HFC-134a/HC-600 (46.6/50.0/3.4)", "1,955", "2,234", "2,346", "2,127", "2,508 "],
            ["GG1863", "R-418A，HC-290/HCFC-22/HFC-152a (1.5/96.0/2.5)", "1,636", "1,635", "1,741", "1,693", "1,886 "],
            ["GG1864", "R-419A，HFC-125/HFC-134a/HE-E170 (77.0/19.0/4.0)", "2,403", "2,865", "2,967", "2,688", "3,171 "],
            ["GG1865", "R-420A，HFC-134a/HCFC-142b (88.0/12.0)", "1,360", "1,432", "1,536", "1,382", "1,450 "],
            ["GG1866", "R-421A，HFC-125/HFC-134a (58.0/42.0)", "2,170", "2,518", "2,631", "2,385", "2,812 "],
            ["GG1867", "R-421B，HFC-125/HFC-134a (85.0/15.0)", "2,575", "3,085", "3,190", "2,890", "3,409 "],
            ["GG1868", "R-422A，HFC-125/HFC-134a/HC-600a (85.1/11.5/3.4)", "2,532", "3,043", "3,143", "2,847", "3,359 "],
            ["GG1869", "R-422B，HFC-125/HFC-134a/HC-600a (55.0/42.0/3.0)", "2,086", "2,416", "2,526", "2,290", "2,700 "],
            ["GG1870", "R-422C，HFC-125/HFC-134a/HC-600a (82.0/15.0/3.0)", "2,491", "2,983", "3,085", "2,794", "3,296 "],
            ["GG1871", "R-500，CFC-12/HFC-152a (73.8/26.2)", "6,014", "7,854", "8,077", "7,564", "8,309 "],
            ["GG1872", "R-501，HCFC-22/CFC-12 (75.0/25.0)", "3,300", "3,925", "4,083", "3,870", "4,270 "],
            ["GG1873", "R-502，HCFC-22/CFC-115 (48.8/51.2)", "4,516", "4,516", "4,657", "4,786", "5,872 "],
            ["GG1874", "R-503，HFC-23/CFC-13 (40.1/59.9)", "13,078", "13,198", "14,560", "13,299", "15,558 "],
            ["GG1875", "R-504，HFC-32/CFC-115 (48.2/51.8)", "4,043", "3,995", "4,143", "4,299", "5,344 "],
            ["GG1876", "R-505，CFC-12/HCFC-31 (78.0/22.0)", "8,809", "8,268", "8,502", "7,956", "8,753 "],
            ["GG1877", "R-506，CFC-31/CFC-114 (55.1/44.9)", "6,891", "4,400", "4,490", "3,857", "4,234 "],
            ["GG1878", "R-507A，HFC-125/HFC-143a (50.0/50.0)", "3,300", "3,850", "3,985", "3,985", "4,775 "],
            ["GG1879", "R-508A，HFC-23/PFC-116 (39.0/61.0)", "10,175", "11,939", "13,214", "11,607", "13,258 "],
            ["GG1880", "R-508B，HFC-23/PFC-116 (46.0/54.0)", "10,350", "11,946", "13,396", "11,698", "13,412 "],
            ["GG1881", "R-509A，HCFC-22/PFC-218 (44.0/56.0)", "4,668", "4,668", "5,741", "5,758", "6,065 "],
            ["GG1882", "R-600A，異丁烷(CH3)CHCH3", "─", "─", "─", "─", ""],
            ["GG1883", "FC-77，全氟混合物", "─", "─", "─", "─", ""],
            ["GG1885", "C4F6，六氟丁二烯", "─", "─", "─", "─", ""],
            ["GG1886", "C5F8，八氟環戊烯", "─", "─", "─", "─", ""],
            ["GG1887", "C4F8O", "─", "─", "─", "─", ""],
            ["GG1888", "COF2", "─", "─", "─", "─", ""],
            ["GG1889", "F2", "─", "─", "─", "─", ""],
        ]

        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+1]}{i+110}", process_list[i][j], cf8)

        worksheet.merge_range("H110:H159", "根據\n2006年IPCC國家溫室氣體清冊指引第三\n冊第七章表7.8之混合冷媒比例", cf22)
        worksheet.merge_range("H160:H161", "IPCC尚未公告GWP值", cf22)
        worksheet.merge_range("H162:H166", "IPCC尚未公告GWP值，目前僅提供代碼\n以便光電半導體業計算其所產生之副產\n物。", cf22)


        for i in range(110, 167):
            for j in range(3):
                worksheet.write(f"{chr_ord[j+9]}{i}", "", cf13)

    # insert images to cell
    workbook = load_workbook(filename)
    worksheet = workbook["3.3-冷媒設備清單(冷氣、飲水機、冰箱)2022"]

    img = Image("image.png")

    worksheet.add_image(img, 'J3')

    img_width = img.width
    img_height = img.height

    worksheet.column_dimensions['J'].width = img_width / 7 
    worksheet.row_dimensions[3].height = img_height / 1.3 

    workbook.save(filename)