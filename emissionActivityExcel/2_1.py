import time
import sys
import os
rootPath = os.getcwd()
sys.path.append(rootPath)
import configparser
import logging
from utils import util, myLog, globals
from connection.mongo_connection import MongoConn
from bson.objectid import ObjectId
import datetime
import pandas as pd

if __name__ == '__main__':    
    startRunTime = time.time()
    env = sys.argv[1]
    source = sys.argv[2]
    language = sys.argv[3]

    config = configparser.ConfigParser()
    config.read(rootPath+"/config.ini")

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", level=logging.INFO)

    if env == "production":
        client = MongoConn(config["mongo_production_nxmap"])
    elif env == "dev":
        client = MongoConn(config["mongo_dev_v1_nxmap"])
    elif env == "staging":
        client = MongoConn(config["mongo_staging"])

    client.connect()
    db = client.get_database()
    chr_ord = globals.chr_ord
   
    sheet1 = globals.sheet1
    sheet2 = globals.sheet2
    category = "Purchased Electricity"

    sourceTitle = ["Data Source", "Energy Type", "Method Of Calculation", "Source", "Emission Type"]
    EnergyType = [category]
    tableTitle = ["Energy Supplier Name", "Energy Type", "Method of Calculation", "Source", "Emission Type"]

    # 製作一個空的DataFrame，之後的sheet都以它為基礎資料來源
    df = pd.DataFrame({})

    max_length = [10 for i in range(78)]
    max_length_addition = 8
    version = "V1.4"

    if language.lower() != "en":
        if source == "EPA Taiwan" or source == "EPA(Taiwan)":
            source_tran = globals.get_EPATaiwan_trans(language)
        else:
            source_tran = source
        if language.lower() == "tw":
            filename = f"輸入電力 - {source_tran}{version}.xlsx"
        elif language.lower() == "de":
            filename = f"Eingangs-Strom - {source_tran}{version}.xlsx"
        elif language.lower() == "jp":
            filename = f"入力電力 - {source_tran}{version}.xlsx"
        elif language.lower() == "it":
            filename = f"Energia in ingresso - {source_tran}{version}.xlsx"
        elif language.lower() == "th":
            filename = f"การรับ-ส่ง ไฟฟ้า - {source_tran}{version}.xlsx"
        elif language.lower() == "zh":
            filename = f"输入电力 - {source_tran}{version}.xlsx"
        else:
            logger.error(f"Language {language} is not supported.")
            raise Exception(f"Language {language} is not supported. Only allow 'tw', 'de', 'jp', 'it', 'th', 'zh'.")
        
        # 開始執行資料寫入
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            logger.info(f"生成 {sheet1}")
            # ------------------------- Source -----------------------------
            df.to_excel(writer, sheet_name=sheet1)
            workbook = writer.book
            worksheet = writer.sheets[sheet1]

            cfSourceTitle = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#4472C4",  # RGB顏色
                "border": 1,         # 格子邊框
                "font_color": "white"
            })
            cfSourceEvenRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#B4C6E7",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })
            cfSourceOddRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#D9E1F2",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })

            emission_view_translations = db.translations.find_one({"url": "emission-view"})
            cal_approaches_translations = db.translations.find_one({"url": "cal_approaches"})

            if language.lower() == "tw":
                translationsTitle = emission_view_translations["tran"][5]["data"]
                translationsEF = cal_approaches_translations["tran"][5]["data"]
            elif language.lower() == "de":
                translationsTitle = emission_view_translations["tran"][0]["data"]
                translationsEF = cal_approaches_translations["tran"][0]["data"]
            elif language.lower() == "jp":
                translationsTitle = emission_view_translations["tran"][3]["data"]
                translationsEF = cal_approaches_translations["tran"][3]["data"]
            elif language.lower() == "it":
                translationsTitle = emission_view_translations["tran"][2]["data"]
                translationsEF = cal_approaches_translations["tran"][2]["data"]
            elif language.lower() == "th":
                translationsTitle = emission_view_translations["tran"][4]["data"]
                translationsEF = cal_approaches_translations["tran"][4]["data"]
            elif language.lower() == "zh":
                translationsTitle = emission_view_translations["tran"][6]["data"]
                translationsEF = cal_approaches_translations["tran"][6]["data"]

            new_sourceTitle = []
            for i in range(len(sourceTitle)):
                new_sourceTitle.append(translationsTitle.get(sourceTitle[i], sourceTitle[i]))
                new_sourceTitle.append(sourceTitle[i])
            sourceTitle = new_sourceTitle

            # 插入標題
            for i in range(len(sourceTitle)):
                worksheet.write(f"{chr_ord[i+1]}1", sourceTitle[i], cfSourceTitle)
                if len(sourceTitle[i]) + max_length_addition > max_length[i]:
                    max_length[i] = len(sourceTitle[i]) + max_length_addition
            
            # 塞入奇數列和偶數列的格子顏色
            for i in range(2, 102, 2):
                worksheet.conditional_format(f"A{i}:J{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceEvenRow})
            for i in range(3, 102, 2):
                worksheet.conditional_format(f"A{i}:J{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceOddRow})

            # 塞入資料
            worksheet.write(f"A2", translationsTitle.get("Standard Data Source", "Standard Data Source"))
            if len(translationsTitle.get("Standard Data Source", "Standard Data Source")) + max_length_addition > max_length[0]:
                max_length[0] = len(translationsTitle.get("Standard Data Source", "Standard Data Source")) + max_length_addition
            worksheet.write(f"B2", "Standard Data Source")
            if len("Standard Data Source") + max_length_addition > max_length[1]:
                max_length[1] = len("Standard Data Source") + max_length_addition

            for i in range(len(EnergyType)):
                worksheet.write(f"D{i+2}", EnergyType[i])
                if len(EnergyType[i]) + max_length_addition > max_length[3]:
                    max_length[3] = len(EnergyType[i]) + max_length_addition

            # Activity Name 其他語言版
            EnergyTypeTrans = []
            for i in range(len(EnergyType)):
                EnergyTypeTrans.append(translationsTitle.get(EnergyType[i], EnergyType[i]))

            numOfEnergyType = len(EnergyTypeTrans)

            workbook.define_name("EnergyType", f"='{sheet1}'!$C$2:$C${numOfEnergyType+1}")
            
            for i in range(len(EnergyTypeTrans)):
                worksheet.write(f"C{i+2}", EnergyTypeTrans[i])
                if len(EnergyTypeTrans[i]) + max_length_addition > max_length[2]:
                    max_length[2] = len(EnergyTypeTrans[i]) + max_length_addition

            worksheet.write(f"G2", source_tran)
            if len(source_tran) + max_length_addition > max_length[6]:
                max_length[6] = len(source_tran) + max_length_addition
            worksheet.write(f"H2", source)
            if len(source) + max_length_addition > max_length[7]:
                max_length[7] = len(source) + max_length_addition
            numOfSource = 1

            sourceType_cal_approaches = list(db.cal_approaches.distinct("sourceType", {"category": category, "methods.source": source}))
            methodOfCalculationEnglish = []
            methodOfCalculationTrans = []
            for sourceType in sourceType_cal_approaches:
                methodOfCalculationEnglish.append(sourceType)
                methodOfCalculationTrans.append(translationsTitle.get(sourceType, sourceType))

            for i in range(len(methodOfCalculationEnglish)):
                # Emission Source 英文版
                worksheet.write(f"F{i+2}", methodOfCalculationEnglish[i])
                if len(methodOfCalculationEnglish[i]) + max_length_addition > max_length[5]:
                    max_length[5] = len(methodOfCalculationEnglish[i]) + max_length_addition
                # Emission Source 其他語言版
                worksheet.write(f"E{i+2}", methodOfCalculationTrans[i])
                if len(methodOfCalculationTrans[i]) + max_length_addition > max_length[4]:
                    max_length[4] = len(methodOfCalculationTrans[i]) + max_length_addition

            numOfMethodOfCalculation = len(methodOfCalculationEnglish)

            workbook.define_name("emissionSource", f"='{sheet1}'!$E$2:$E${numOfMethodOfCalculation+1}")

            cal_approaches = list(db.cal_approaches.find({"category": category, "methods.source": source}))

            numOfemissionType = 0
            for cal_approach in cal_approaches:
                # index = emissionSourceEnglish.index(cal_approach.get("typeOfEmission"))
                # if cal_approach.get("typeOfEmission") == emissionSourceEnglish[0]:    # Coal and Coke
                methods = cal_approach["methods"]
                for idx, method in enumerate(methods):
                    unit = method.get("baseUnit", "")
                    sourceOfEmission = method.get("sourceOfEmission", "")

                    worksheet.write(f"I{idx+2}", translationsEF.get(sourceOfEmission, sourceOfEmission) + " - " + unit)
                    if len(translationsEF.get(sourceOfEmission, sourceOfEmission) + " - " + unit) + max_length_addition > max_length[8]:
                        max_length[8] = len(translationsEF.get(sourceOfEmission, sourceOfEmission) + " - " + unit) + max_length_addition
                    worksheet.write(f"J{idx+2}", sourceOfEmission + " - " + unit)
                    if len(sourceOfEmission + " - " + unit) + max_length_addition > max_length[9]:
                        max_length[9] = len(sourceOfEmission + " - " + unit) + max_length_addition

                    numOfemissionType += 1

            for i in range(len(max_length)):
                worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", max_length[i])

            worksheet.write("A105", version)
            
            logger.info(f"生成 {sheet2}")
            # ------------------------- Table -----------------------------
            df.to_excel(writer, sheet_name=sheet2)
            worksheet = writer.sheets[sheet2]

            cfTableTitle = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#5B9BD5",  # RGB顏色
                "border": 1,         # 格子邊框
                "font_color": "white"
            })
            cfTableRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#FFFFFF",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })

            process_list = [15, 15, 25, 25, 15, 15, 15, 15, 25, 25, 15, 15]
            for i in range(len(process_list)):
                worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

            new_tableTitle = []
            for i in range(len(tableTitle)):
                new_tableTitle.append(translationsTitle.get(tableTitle[i], tableTitle[i]))
                new_tableTitle.append(tableTitle[i])
            tableTitle = new_tableTitle

            for i in range(len(tableTitle)):
                worksheet.write(f"{chr_ord[i+1]}1", tableTitle[i], cfTableTitle)

            worksheet.conditional_format("A2:J101", {"type": "formula", "criteria": "TRUE", "format":cfTableRow})
            
            for i in range(2, 102):
                worksheet.write(f"B{i}", f"""=IF(A{i}="", "", A{i})""")

                worksheet.write(f"D{i}", f"""=IF(C{i}="","",VLOOKUP(C{i},'{sheet1}'!C2:D{numOfEnergyType+1},2,0))""")
                
                worksheet.write(f"F{i}", f"""=IF(E{i}="","",VLOOKUP(E{i},'{sheet1}'!E2:F{numOfMethodOfCalculation+1},2,0))""")

                worksheet.write(f"H{i}", f"""=IF(G{i}="","",VLOOKUP(G{i},'{sheet1}'!G2:H{numOfSource+1},2,0))""")

                worksheet.write(f"J{i}", f"""=IF(I{i}="","",VLOOKUP(I{i},'{sheet1}'!I2:J{numOfemissionType+1},2,0))""")

            worksheet.data_validation("C2:C101", {"validate": "list", "source": f"='{sheet1}'!$C$2:$C${numOfEnergyType+1}",'ignore_blank': True})
            worksheet.data_validation("E2:E101", {"validate": "list", "source": f"='{sheet1}'!$E$2:$E${numOfMethodOfCalculation+1}",'ignore_blank': True})
            worksheet.data_validation("G2:G101", {"validate": "list", "source": f"='{sheet1}'!$G$2:$G${numOfSource+1}",'ignore_blank': True})
            worksheet.data_validation("I2:I101", {"validate": "list", "source": f"='{sheet1}'!$I$2:$I${numOfemissionType+1}",'ignore_blank': True})
            

            worksheet.activate()

    else:
        filename = f"{category}_{source}.xlsx"

        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            logger.info(f"生成 {sheet1}")
            # ------------------------- Source -----------------------------
            df.to_excel(writer, sheet_name=sheet1)
            workbook = writer.book
            worksheet = writer.sheets[sheet1]
            
            cfSourceTitle = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#4472C4",  # RGB顏色
                "border": 1,         # 格子邊框
                "font_color": "white"
            })
            cfSourceEvenRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#B4C6E7",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })
            cfSourceOddRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#D9E1F2",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })

            for i in range(len(sourceTitle)):
                worksheet.write(f"{chr_ord[i+1]}1", sourceTitle[i], cfSourceTitle)
                if len(sourceTitle[i]) + max_length_addition > max_length[i]:
                    max_length[i] = len(sourceTitle[i]) + max_length_addition

            for i in range(2, 102, 2):
                worksheet.conditional_format(f"A{i}:E{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceEvenRow})

            for i in range(3, 102, 2):
                worksheet.conditional_format(f"A{i}:E{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceOddRow})

            worksheet.write("A2", "Standard Data Source")
            if  len("Standard Data Source") + max_length_addition > max_length[0]:
                max_length[0] = len("Standard Data Source") + max_length_addition

            for i in range(len(EnergyType)):
                worksheet.write(f"B{i+2}", EnergyType[i])
                if len(EnergyType[i]) + max_length_addition > max_length[1]:
                    max_length[1] = len(EnergyType[i]) + max_length_addition
            
            numOfEnergyType = len(EnergyType)
            workbook.define_name("EnergyType", f"='{sheet1}'!B2:B{numOfEnergyType+1}")

            worksheet.write("D2", source)
            if len(source) + max_length_addition > max_length[3]:
                max_length[3] = len(source) + max_length_addition
            numOfSource = 1

            sourceType_cal_approaches = list(db.cal_approaches.distinct("sourceType", {"category": category, "methods.source": source}))
            methodOfCalculationEnglish = []
            for sourceType in sourceType_cal_approaches:
                methodOfCalculationEnglish.append(sourceType)
            
            for i in range(len(methodOfCalculationEnglish)):
                worksheet.write(f"C{i+2}", methodOfCalculationEnglish[i])
                if len(methodOfCalculationEnglish[i]) + max_length_addition > max_length[2]:
                    max_length[2] = len(methodOfCalculationEnglish[i]) + max_length_addition
                
            numOfMethodOfCalculation = len(methodOfCalculationEnglish)

            workbook.define_name("methodOfCalculation", f"='{sheet1}'!D2:D{numOfMethodOfCalculation+1}")

            cal_approaches = list(db.cal_approaches.find({"category": category, "methods.source": source}))

            numOfemissionType = 0
            for cal_approach in cal_approaches:
                methods = cal_approach["methods"]
                for idx, method in enumerate(methods):
                    unit = method.get("baseUnit", "")
                    sourceOfEmission = method.get("sourceOfEmission", "")
                    
                    worksheet.write(f"E{idx+2}", sourceOfEmission + " - " + unit)
                    if len(sourceOfEmission + " - " + unit) + max_length_addition > max_length[4]:
                        max_length[4] = len(sourceOfEmission + " - " + unit) + max_length_addition

                    numOfemissionType += 1

            for i in range(len(max_length)):
                worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", max_length[i])

            worksheet.write("A105", version)
    
            logger.info(f"生成 {sheet2}")
            # ------------------------- Table ----------
            df.to_excel(writer, sheet_name=sheet2)
            worksheet = writer.sheets[sheet2]

            cfTableTitle = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#5B9BD5",  # RGB顏色
                "border": 1,         # 格子邊框
                "font_color": "white"
            })
            cfTableRow = workbook.add_format({
                "font_size": 12,    # 字体大小
                "pattern": 1,       # 填充模式
                "bg_color": "#FFFFFF",  # RGB顏色
                "border": 1,
                # "align": "center",
                # "valign": "vcenter",
            })

            process_list = [15, 15, 25, 25, 15, 15, 15, 15, 25, 25, 15, 15]
            for i in range(len(process_list)):
                worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

            for i in range(len(tableTitle)):
                worksheet.write(f"{chr_ord[i+1]}1", tableTitle[i], cfTableTitle)

            worksheet.conditional_format("A2:E101", {"type": "formula", "criteria": "TRUE", "format":cfTableRow})

            worksheet.data_validation("B2:B101", {"validate": "list", "source": f"='{sheet1}'!$B$2:$B${numOfEnergyType+1}",'ignore_blank': True})
            worksheet.data_validation("C2:C101", {"validate": "list", "source": f"='{sheet1}'!$C$2:$C${numOfMethodOfCalculation+1}",'ignore_blank': True})
            worksheet.data_validation("D2:D101", {"validate": "list", "source": f"='{sheet1}'!$D$2:$D${numOfSource+1}",'ignore_blank': True})
            worksheet.data_validation("E2:E101", {"validate": "list", "source": f"='{sheet1}'!$E$2:$E${numOfemissionType+1}",'ignore_blank': True})

            worksheet.activate()


    endRunTime = time.time()
    client.close()
    logger.info(f"Time used: {util.convert_sec(endRunTime - startRunTime)}.")