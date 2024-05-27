import time
import sys
import os
rootPath = os.getcwd()
sys.path.append(rootPath)
import configparser
import logging
from utils import util, myLog
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
    chr_ord = {}
    for i in range(65, 91):
        chr_ord[i-64] = chr(i)
        chr_ord[i-38] = "A"+chr(i)
        chr_ord[i-12] = "B" + chr(i)
        
    # 創造一個變數作為數字和字母的對照表
    # {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z', 27: 'AA', 28: 'AB', 29: 'AC', 30: 'AD', 31: 'AE', 32: 'AF', 33: 'AG', 34: 'AH', 35: 'AI', 36: 'AJ', 37: 'AK', 38: 'AL', 39: 'AM', 40: 'AN', 41: 'AO', 42: 'AP', 43: 'AQ', 44: 'AR', 45: 'AS', 46: 'AT', 47: 'AU', 48: 'AV', 49: 'AW', 50: 'AX', 51: 'AY', 52: 'AZ'}
    sheet1 = "Source"
    sheet2 = "Table"
    category = "Stationary Combustion"

    sourceTitle = ["Data Source", "Activity Name", "Source", "Emission Source"]
    activityName = ["Backup generator", "Boiler", "Chillers", "Combustion Turbine", "Convector", "Dryer", "Flare Tower", "Gas Stove", "GCB/GIS", "Generator", "Incinerator", "Kiln", "Pulverized-coal fired boiler", "Regenerative Thermal Oxidizer (RTO)", "Others"]
    tableTitle = ["Equipment Name", "Activity Name", "Source", "Emission Source", "Emission Type", "Biomass"] 

    # 製作一個空的DataFrame，之後的sheet都以它為基礎資料來源
    df = pd.DataFrame({})

    max_length = [10 for i in range(78)]
    max_length_addition = 8
    version = "V1.4"

    if language.lower() != "en":
        if source == "EPA Taiwan" or source == "EPA(Taiwan)":
            if language.lower() == "tw":
                source_tran = "台灣環境部"
            elif language.lower() == "de":
                source_tran = "Bundesamt für Umwelt BAFU"
            elif language.lower() == "jp":
                source_tran = "気象庁"
            elif language.lower() == "it":
                source_tran = "Bureau di Meteo"
            elif language.lower() == "th":
                source_tran = "กรมอุตุนิยมวิทยา"
            elif language.lower() == "zh":
                source_tran = "环境部"
            else:
                logger.error(f"Language {language} is not supported.")
        else:
            source_tran = source
        if language.lower() == "tw":
            filename = f"固定式燃燒源 - {source_tran}{version}.xlsx"
        elif language.lower() == "de":
            filename = f"Feststoff-Quellen - {source_tran}{version}.xlsx"
        elif language.lower() == "jp":
            filename = f"固定式燃焼源 - {source_tran}{version}.xlsx"
        elif language.lower() == "it":
            filename = f"Fossili-Fuochi - {source_tran}{version}.xlsx"
        elif language.lower() == "th":
            filename = f"กรดไม้เติม - {source_tran}{version}.xlsx"
        elif language.lower() == "zh":
            filename = f"固定式燃烧源 - {source_tran}{version}.xlsx"
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

            if language.lower() == "tw":
                translationsTitle = emission_view_translations["tran"][5]["data"]
            elif language.lower() == "de":
                translationsTitle = emission_view_translations["tran"][0]["data"]
            elif language.lower() == "jp":
                translationsTitle = emission_view_translations["tran"][3]["data"]
            elif language.lower() == "it":
                translationsTitle = emission_view_translations["tran"][2]["data"]
            elif language.lower() == "th":
                translationsTitle = emission_view_translations["tran"][4]["data"]
            elif language.lower() == "zh":
                translationsTitle = emission_view_translations["tran"][6]["data"]

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
                worksheet.conditional_format(f"A{i}:H{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceEvenRow})
            for i in range(3, 102, 2):
                worksheet.conditional_format(f"A{i}:H{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceOddRow})

            # 塞入資料
            worksheet.write(f"A2", translationsTitle.get("Standard Data Source", "Standard Data Source"))
            if len(translationsTitle.get("Standard Data Source", "Standard Data Source")) + max_length_addition > max_length[0]:
                max_length[0] = len(translationsTitle.get("Standard Data Source", "Standard Data Source")) + max_length_addition
            worksheet.write(f"B2", "Standard Data Source")
            if len("Standard Data Source") + max_length_addition > max_length[1]:
                max_length[1] = len("Standard Data Source") + max_length_addition

            for i in range(len(activityName)):
                worksheet.write(f"D{i+2}", activityName[i])
                if len(activityName[i]) + max_length_addition > max_length[3]:
                    max_length[3] = len(activityName[i]) + max_length_addition

            # Activity Name 其他語言版
            activityNameTrans = []
            for i in range(len(activityName)):
                activityNameTrans.append(translationsTitle.get(activityName[i], activityName[i]))

            numOfActivityName = len(activityNameTrans)

            workbook.define_name("activityName", f"='{sheet1}'!$C$2:$C${numOfActivityName+1}")
            
            for i in range(len(activityNameTrans)):
                worksheet.write(f"C{i+2}", activityNameTrans[i])
                if len(activityNameTrans[i]) + max_length_addition > max_length[2]:
                    max_length[2] = len(activityNameTrans[i]) + max_length_addition

            worksheet.write(f"E2", source_tran)
            if len(source_tran) + max_length_addition > max_length[4]:
                max_length[4] = len(source_tran) + max_length_addition
            worksheet.write(f"F2", source)
            if len(source) + max_length_addition > max_length[5]:
                max_length[5] = len(source) + max_length_addition
            numOfSource = 1

            cal_approaches_translations = db.translations.find_one({"url": "cal_approaches"})
            if language.lower() == "tw":
                translationsEF = cal_approaches_translations["tran"][5]["data"]
            elif language.lower() == "de":
                translationsEF = cal_approaches_translations["tran"][0]["data"]
            elif language.lower() == "jp":
                translationsEF = cal_approaches_translations["tran"][3]["data"]
            elif language.lower() == "it":
                translationsEF = cal_approaches_translations["tran"][2]["data"]
            elif language.lower() == "th":
                translationsEF = cal_approaches_translations["tran"][4]["data"]
            elif language.lower() == "zh":
                translationsEF = cal_approaches_translations["tran"][6]["data"]

            typeOfEmission_cal_approaches = list(db.cal_approaches.distinct("typeOfEmission", {"category": category, "methods.source": source}))
            emissionSourceEnglish = []
            emissionSourceTrans = []
            for typeOfEmission in typeOfEmission_cal_approaches:
                emissionSourceEnglish.append(typeOfEmission)
                emissionSourceTrans.append(translationsTitle.get(typeOfEmission, typeOfEmission))

            for i in range(len(emissionSourceEnglish)):
                # Emission Source 英文版
                worksheet.write(f"H{i+2}", emissionSourceEnglish[i])
                if len(emissionSourceEnglish[i]) + max_length_addition > max_length[7]:
                    max_length[7] = len(emissionSourceEnglish[i]) + max_length_addition
                # Emission Source 其他語言版
                worksheet.write(f"G{i+2}", emissionSourceTrans[i])
                if len(emissionSourceTrans[i]) + max_length_addition > max_length[6]:
                    max_length[6] = len(emissionSourceTrans[i]) + max_length_addition

                worksheet.write(f"{chr_ord[i*2+11]}1", emissionSourceEnglish[i], cfSourceTitle)
                if len(emissionSourceEnglish[i]) + max_length_addition > max_length[i*2+10]:
                    max_length[i*2+10] = len(emissionSourceEnglish[i]) + max_length_addition
                worksheet.write(f"{chr_ord[i*2+10]}1", emissionSourceTrans[i], cfSourceTitle)
                if len(emissionSourceTrans[i]) + max_length_addition > max_length[i*2+9]:
                    max_length[i*2+9] = len(emissionSourceTrans[i]) + max_length_addition

                for j in range(2, 102, 2):
                    worksheet.conditional_format(f"{chr_ord[i*2+10]}{j}:{chr_ord[i*2+11]}{j}", {"type": "formula", "criteria": "TRUE", "format": cfSourceEvenRow})
                
                for j in range(3, 102, 2):
                    worksheet.conditional_format(f"{chr_ord[i*2+10]}{j}:{chr_ord[i*2+11]}{j}", {"type": "formula", "criteria": "TRUE", "format": cfSourceOddRow})

            numOfEmissionSource = len(emissionSourceEnglish)

            workbook.define_name("emissionSource", f"='{sheet1}'!$G$2:$G${numOfEmissionSource+1}")

            cal_approaches = list(db.cal_approaches.find({"category": category, "methods.source": source}))
            numOfSubEmissionSource = [0 for i in range(numOfEmissionSource)]

            for cal_approach in cal_approaches:
                index = emissionSourceEnglish.index(cal_approach.get("typeOfEmission"))
                # if cal_approach.get("typeOfEmission") == emissionSourceEnglish[0]:    # Coal and Coke
                methods = cal_approach["methods"]
                for idx, method in enumerate(methods):
                    unit = method.get("baseUnit", "")
                    sourceOfEmission = method.get("sourceOfEmission", "")

                    worksheet.write(f"{chr_ord[index*2+10]}{idx+2}", translationsEF.get(sourceOfEmission, sourceOfEmission)+" - "+unit)
                    if len(translationsEF.get(sourceOfEmission, sourceOfEmission)+" - "+unit) + max_length_addition > max_length[index*2+9]:
                        max_length[index*2+9] = len(translationsEF.get(sourceOfEmission, sourceOfEmission)+" - "+unit) + max_length_addition
                    worksheet.write(f"{chr_ord[index*2+11]}{idx+2}", sourceOfEmission + " - " + unit)
                    if len(sourceOfEmission + " - " + unit) + max_length_addition > max_length[index*2+10]:
                        max_length[index*2+10] = len(sourceOfEmission + " - " + unit) + max_length_addition
                    numOfSubEmissionSource[index] += 1

            for i in range(len(emissionSourceTrans)):
                workbook.define_name(emissionSourceTrans[i].replace(" ", "").replace("-", "").replace(",","").replace("(","").replace(")","").replace(">","").replace("<","").replace(".",""), f"='{sheet1}'!${chr_ord[i*2+10]}$2:${chr_ord[i*2+10]}${numOfSubEmissionSource[i]+1}")
            
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

            worksheet.conditional_format("A2:L101", {"type": "formula", "criteria": "TRUE", "format":cfTableRow})

            report_config_translations = db.translations.find_one({"url": "report-configure"})
            if language.lower() == "tw":
                translation4Biomass = report_config_translations["tran"][5]["data"]
            elif language.lower() == "de":
                translation4Biomass = report_config_translations["tran"][0]["data"]
            elif language.lower() == "jp":
                translation4Biomass = report_config_translations["tran"][3]["data"]
            elif language.lower() == "it":
                translation4Biomass = report_config_translations["tran"][2]["data"]
            elif language.lower() == "th":
                translation4Biomass = report_config_translations["tran"][4]["data"]
            elif language.lower() == "zh":
                translation4Biomass = report_config_translations["tran"][6]["data"]

            for i in range(2, 102):
                worksheet.write(f"B{i}", f"""=IF(A{i}="", "", A{i})""")

                worksheet.write(f"D{i}", f"""=IF(C{i}="","",VLOOKUP(C{i},'{sheet1}'!C2:D{numOfActivityName+1},2,0))""")
                
                worksheet.write(f"F{i}", f"""=IF(E{i}="","",VLOOKUP(E{i},'{sheet1}'!E2:F{numOfSource+1},2,0))""")

                worksheet.write(f"H{i}", f"""=IF(G{i}="","",VLOOKUP(G{i},'{sheet1}'!G2:H{numOfEmissionSource+1},2,0))""")

                formula = f"""=IF(I{i}="","","""

                for j in range(len(numOfSubEmissionSource)):
                    formula += f"""IF(G{i}='{sheet1}'!G{j+2},VLOOKUP(I{i},'{sheet1}'!{chr_ord[j*2+10]}2:{chr_ord[j*2+11]}{numOfSubEmissionSource[j]+2},2,0),"""
                
                formula += ")"*(len(numOfSubEmissionSource)+1)

                worksheet.write(f"J{i}", formula)

                worksheet.write(f"L{i}", f"""=IF(K{i}="","",IF(K{i}="{translation4Biomass.get("Yes", "Yes")}","Yes","No"))""")

                worksheet.data_validation(f"I{i}:I{i}", {"validate": "list", "source": f"""=INDIRECT(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(G{i}," ",""),"-",""),",",""),"(",""),")",""),">",""),"<",""),".",""))""", "ignore_blank": True})
            
            worksheet.data_validation("C2:C101", {"validate": "list", "source": f"='{sheet1}'!$C$2:$C${numOfActivityName+1}",'ignore_blank': True})
            worksheet.data_validation("E2:E101", {"validate": "list", "source": f"='{sheet1}'!$E$2:$E${numOfSource+1}", "ignore_blank": True})
            worksheet.data_validation("G2:G101", {"validate": "list", "source": f"='{sheet1}'!$G$2:$G${numOfEmissionSource+1}", "ignore_blank": True})
            worksheet.data_validation("K2:K101", {"validate": "list", "source": [translation4Biomass.get("Yes", "Yes"), translation4Biomass.get("No", "No")], "ignore_blank": True})

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
                worksheet.conditional_format(f"A{i}:D{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceEvenRow})

            for i in range(3, 102, 2):
                worksheet.conditional_format(f"A{i}:D{i}", {"type": "formula", "criteria": "TRUE", "format":cfSourceOddRow})

            worksheet.write("A2", "Standard Data Source")
            if  len("Standard Data Source") + max_length_addition > max_length[0]:
                max_length[0] = len("Standard Data Source") + max_length_addition

            for i in range(len(activityName)):
                worksheet.write(f"B{i+2}", activityName[i])
                if len(activityName[i]) + max_length_addition > max_length[1]:
                    max_length[1] = len(activityName[i]) + max_length_addition
            
            numOfActivityName = len(activityName)
            workbook.define_name("activityName", f"='{sheet1}'!B2:B{numOfActivityName+1}")

            worksheet.write("C2", source)
            if len(source) + max_length_addition > max_length[2]:
                max_length[2] = len(source) + max_length_addition
            numOfSource = 1

            type_cal_approaches = list(db.cal_approaches.distinct("typeOfEmission", {"category": category, "methods.source": source}))
            emissionSourceEnglish = []
            for _type in type_cal_approaches:
                emissionSourceEnglish.append(_type)
            
            for i in range(len(emissionSourceEnglish)):
                worksheet.write(f"D{i+2}", emissionSourceEnglish[i])
                if len(emissionSourceEnglish[i]) + max_length_addition > max_length[3]:
                    max_length[3] = len(emissionSourceEnglish[i]) + max_length_addition
                worksheet.write(f"{chr_ord[i+6]}1", emissionSourceEnglish[i], cfSourceTitle)
                if len(emissionSourceEnglish[i]) + max_length_addition > max_length[i+6]:
                    max_length[i+6] = len(emissionSourceEnglish[i]) + max_length_addition

                for j in range(2, 102, 2):
                    worksheet.conditional_format(f"{chr_ord[i+6]}{j}:{chr_ord[i+6]}{j}", {"type": "formula", "criteria": "TRUE", "format": cfSourceEvenRow})
                
                for j in range(3, 102, 2):
                    worksheet.conditional_format(f"{chr_ord[i+6]}{j}:{chr_ord[i+6]}{j}", {"type": "formula", "criteria": "TRUE", "format": cfSourceOddRow})

            numOfEmissionSource = len(emissionSourceEnglish)

            workbook.define_name("emissionSource", f"='{sheet1}'!D2:D{numOfEmissionSource+1}")

            cal_approaches = list(db.cal_approaches.find({"category": category, "methods.source": source}))
            numOfSubEmissionSource = [0 for i in range(len(emissionSourceEnglish))]

            for cal_approach in cal_approaches:
                index = emissionSourceEnglish.index(cal_approach.get("typeOfEmission"))
                methods = cal_approach["methods"]
                for idx, method in enumerate(methods):
                    unit = method.get("baseUnit", "")
                    sourceOfEmission = method.get("sourceOfEmission", "")

                    worksheet.write(f"{chr_ord[index+6]}{idx+2}", sourceOfEmission + " - " + unit)
                    if len(sourceOfEmission + " - " + unit) + max_length_addition > max_length[index+6]:
                        max_length[index+6] = len(sourceOfEmission + " - " + unit) + max_length_addition
                    numOfSubEmissionSource[index] += 1

            for i in range(len(emissionSourceEnglish)):
                workbook.define_name(emissionSourceEnglish[i].replace(" ", "").replace("-", "").replace(",","").replace("(","").replace(")","").replace(">","").replace("<","").replace(".",""), f"='{sheet1}'!${chr_ord[i+6]}$2:${chr_ord[i+6]}${numOfSubEmissionSource[i]+1}")

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

            worksheet.conditional_format("A2:F101", {"type": "formula", "criteria": "TRUE", "format":cfTableRow})

            for i in range(2, 102):
                worksheet.data_validation(f"E{i}:E{i}", {"validate": "list", "source": f"""=INDIRECT(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(D{i}," ",""),"-",""),",",""),"(",""),")",""),">",""),"<",""),".",""))""",'ignore_blank': True})

            worksheet.data_validation("B2:B101", {"validate": "list", "source": f"='{sheet1}'!$B$2:$B${numOfActivityName+1}",'ignore_blank': True})
            worksheet.data_validation("C2:C101", {"validate": "list", "source": f"='{sheet1}'!$C$2:$C${numOfSource+1}",'ignore_blank': True})
            worksheet.data_validation("D2:D101", {"validate": "list", "source": f"='{sheet1}'!$D$2:$D${numOfEmissionSource+1}",'ignore_blank': True})
            worksheet.data_validation("F2:F101", {"validate": "list", "source": ["Yes","No"], "ignore_blank": True})

            worksheet.activate()


    endRunTime = time.time()
    client.close()
    logger.info(f"Time used: {util.convert_sec(endRunTime - startRunTime)}.")