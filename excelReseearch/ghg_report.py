import pandas as pd
import datetime
from openpyxl.drawing.image import Image
from openpyxl import load_workbook
import os
import sys
rootPath = os.getcwd()
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
import pymongo
from utils import myLog, util
from bson import ObjectId
import time
import json
import logging

# 65b9b22e3bb61226a5bc5d4c

if __name__ == "__main__":
    startRunTime = time.time()
    companyId = sys.argv[1]
    inventory_year = int(sys.argv[2])
    env = sys.argv[3]

    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"], level=logging.INFO)

    if env == "production":
        client = MongoConn(config['mongo_production_nxmap'])
    elif env == "dev":
        client = MongoConn(config['mongo_dev_v1_nxmap'])
    elif env == "staging":
        client = MongoConn(config['mongo_staging_nxmap'])
    client.connect()
    db = client.get_database()

    site_modules = db.site_modules.find_one({"_id": ObjectId(companyId)})
    companyName = site_modules['companyName']

    filename = f"溫室氣體盤查清冊v1.2_{companyName}_{inventory_year}.xlsx"

    today = datetime.datetime.now().date()

    chr_ord = {}

    for i in range(65, 91):
        chr_ord[i-64] = chr(i)
        chr_ord[i-38] = "A"+chr(i)
    # 創造一個變數作為數字和字母的對照表
    # {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z', 27: 'AA', 28: 'AB', 29: 'AC', 30: 'AD', 31: 'AE', 32: 'AF', 33: 'AG', 34: 'AH', 35: 'AI', 36: 'AJ', 37: 'AK', 38: 'AL', 39: 'AM', 40: 'AN', 41: 'AO', 42: 'AP', 43: 'AQ', 44: 'AR', 45: 'AS', 46: 'AT', 47: 'AU', 48: 'AV', 49: 'AW', 50: 'AX', 51: 'AY', 52: 'AZ'}

    # 類別名稱分類
    category1_list = ["Stationary Combustion", "Mobile Combustion", "Industrial Processes", "Refrigerants and Fugitives", "Land Use, Land Use Change and Forestry (LULUCF)"]
    category2_list = ["Purchased Electricity", "Imported Energy"]
    category3_list = ["Upstream T&D", "Downstream T&D", "Employee Commuting", "Client and Visitor Transport", "Business Travel"]
    category4_list = ["Purchased Goods","Capital Goods that are Purchased and Amortized by an Organization", "Waste", "Use of Assets (through leased equipment) by an Organization", "Use of Services not Described in the above sub-categories by an Organization"]
    category5_list = ["Use Stage of the Product", "Downstream Leased Assets", "End of Life Stage of the Product", "Investments"]
    category6_list = ["GHG Emissions or Removals from Other Sources"] 
    
    # 以下三行分別是活動數據種類、活動數據可信種類、排放係數種類
    activityDataTypes = {"1-1":"連續量測", "2-2":"定期(間歇)量測", "3-3": "財務會計推估", "4-3": "自行評估"}
    activityTrustedDataTypes = {"1-1": "(1)有進行外部校正或有多組數據茲佐證者", "2-2": "(2)有進行內部校正或經過會計簽證等証明者", "3-3": "(3)未進行儀器校正或未進行紀錄彙整者"}
    emissionFactorTypes = {"1-1": "1自廠發展係數/質量平衡所得係數", "2-1": "2同製程/設備經驗係數", "3-1": "3製造廠提供係數", "4-3": "4區域排放係數", "5-3": "5國家排放係數", "6-3": "6國際排放係數"}

    refrigerants_list = ["HCFC-22/R22", "HFC-134a", "HFC-32/R32", "R-401A", "R-404A", "R-410A", "R600A"]

    # 製作一個空的DataFrame，之後的sheet都以它為基礎資料來源
    df = pd.DataFrame({})
    # 開始執行資料寫入
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        logger.info("生成 1-基本資料")
        # ------------------------- 1-基本資料 -----------------------------
        df.to_excel(writer, sheet_name="1-基本資料", )
        workbook = writer.book
        worksheet = writer.sheets["1-基本資料"]

        # cfX:cell format（單元格格式）的定義
        cf1 = workbook.add_format({
            "bold": True,       # 粗体
            "font_size": 14,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FDE9D9",  # RGB顏色
            "border": 1         # 格子邊框
        })
        cf2 = workbook.add_format({
            "font_size": 14,
            "bg_color": "#FFFF00",
            "border": 1,
            "text_wrap": True,
            "align": "center",
            "valign": "vcenter"
        })
        cf0 = workbook.add_format({
            "font_size": 14,
            "border": 1,
            "text_wrap": True,   # 自動換行
            "align": "center",
            "valign": "vcenter"
        })
        cf0_1 = workbook.add_format({
            "font_size": 14,
            "border": 1,
            "bg_color": "#FFFF00",
            "text_wrap": True,   # 自動換行
            "num_format": '@',
            "align": "center",
            "valign": "vcenter"
        })
        cf3 = workbook.add_format({
            "bold": True,       
            "font_size": 14,    
            "pattern": 1,      
            "bg_color": "#FDE9D9", 
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

        # write: 寫入資料進入對應的單元格子
        worksheet.write("A1", "版次", cf1)
        worksheet.write("B1", "V1.4", cf2)
        worksheet.write("C1", "更新時間", cf1)
        worksheet.write("D1", today.strftime("%m/%d/%Y"), cf2)

        # 設定寬度
        worksheet.set_column("A:A", 9.89)
        worksheet.set_column("B:B", 12.89)
        worksheet.set_column("C:C", 11.67)
        worksheet.set_column("D:D", 14.44)
        worksheet.set_column("G:G", 32.67)
        # 設定高度
        for i in range(10):
            worksheet.set_row(i, 18.75)

        # merge_range: 合并单元格
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
        
        # 取得公司資訊
        companyId = site_modules['_id']
        company_address = site_modules.get('address')
        inventory_start = f"{inventory_year}.01.01"          
        inventory_end = f"{inventory_year}.12.31"           
        industry_num = "請參考附表一的行業代碼"                  
        industry = "=VLOOKUP(C9,附表一、行業代碼!B2:C558,2,)"
        operationalBoundaries = f"{companyName} {inventory_year-1911}年溫室氣體盤查採用「營運控制權法」"

        insert_list = [companyName, company_address, inventory_year, inventory_start, inventory_end, industry_num, industry]

        # 寫資料到C4:G11
        for i in range(len(insert_list)):
            if i == 6:
                worksheet.merge_range(f"C{i+4}:G{i+4}", insert_list[i], cf0)
            elif i == 5:
                worksheet.merge_range(f"C{i+4}:G{i+4}", insert_list[i], cf0_1)
            else:
                worksheet.merge_range(f"C{i+4}:G{i+4}", insert_list[i], cf2)
        worksheet.merge_range("C11:G11", operationalBoundaries, cf2)

        logger.info("生成 2-定性盤查")
        # ------------------------- 2-定性盤查 -----------------------------
        # 取得cal_approaches的翻譯資料並只選取中文
        translations = db.translations.find_one({"url": "cal_approaches"})
        tw_tanslate = translations['tran'][5]['data']

        df.to_excel(writer, sheet_name="2-定性盤查", )
        worksheet = writer.sheets["2-定性盤查"]

        # 取得公司的設定的排放係數
        company_assets_head = db.company_assets.find({"company": ObjectId(companyId)})
        
        # 取得公司底下的子公司
        site_modules_sub = {}
        for site_module in db.site_modules.find({"parentCompanyId": ObjectId(companyId)}):
            site_modules_sub[site_module['companyName']] = site_module['_id']
 
        # 設定寬度
        process_list = [10, 14, 26, 24, 14.5, 14, 14]
        for i in range(1, 8):
            worksheet.set_column(f"{chr_ord[i]}:{chr_ord[i]}", process_list[i-1])
        worksheet.set_column("H:N", 5.89)
        worksheet.set_column("O:O", 8.56)

        # 設定高度
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
        cf24 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#DAEEF3",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
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
        cf14 = workbook.add_format({
            "font_size": 12,
            "pattern": 1,      
            "bg_color": "#D9D9D9",  
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
        cf29 = workbook.add_format({
            "font_size": 12,
            "pattern": 1,      
            "bg_color": "#D9D9D9",  
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
            "num_format": '0.0000%'
        }) 
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
            "font_size": 12,    
            "pattern": 1,       
            "bg_color": "#FFFFFF",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })
        cf25 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "color": "#4BACC6",
            "bg_color": "#FFFFFF",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True
        })
        cf26 = workbook.add_format({
            "font_size": 12,    
            "pattern": 1,       
            "bg_color": "#FFFFFF",  
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            'num_format': '0.0000000000'
        })
        cf27 = workbook.add_format({
            "font_size": 12,    
            "pattern": 1,       
            "bg_color": "#D9D9D9", 
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
            'num_format': '0.0000'
        })
        cf28 = workbook.add_format({
            "font_size": 12,    
            "pattern": 1,       
            "bg_color": "#FFFFFF",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True,
            'num_format': '0.0000'
        })

        process_list = [10.7109375, 20, 30, 25, 15.28515625, 14.7109375, 13.0, 13, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 9.28515625, 0.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定A2:O3的資料
        process_list = ["編號", "廠區歸屬", "名稱", "排放源", "是否屬於生質能源"]
        for i in range(1, 6):
            worksheet.merge_range(f"{chr_ord[i]}2:{chr_ord[i]}3", process_list[i-1], cf6)
        worksheet.write("F2", "類別", cf6)
        worksheet.write("G2", "類別\n排放類型", cf6)
        worksheet.merge_range("H2:N2", "可能產生溫室氣體種類", cf6)
        process_list = ["1~6", "(E,P,T,F)", "CO₂", "CH₄", "N₂O"]
        for i in range(6, 11):
            worksheet.write(f"{chr_ord[i]}3", process_list[i-6], cf6)
        worksheet.write("K3", "HFC\u209b", cf6)
        worksheet.write("L3", "PFC\u209b", cf6)
        worksheet.write("M3", "SF₆", cf6)
        worksheet.write("N3", "NF₃", cf6)
        worksheet.merge_range("O2:O3", "備住", cf6)

        # current_index: 目前的排放係數狀態編號, ef: 排放係數資訊, current_index_upstream: 目前的上游排放資訊編號, upstream_info: 上游排放資訊, current_index_downtream: 目前的下游排放資訊編號, downstream_info: 下游排放資訊, asset_index: 用作記錄上下遊的資訊
        current_index = 1
        ef = {}
        current_index_upstream = 1
        upstream_info = {}
        current_index_downtream = 1
        downstream_info = {}
        asset_index = 1

        # 海（空）運和陸運的分類
        ModeOfTransport = [[tw_tanslate.get("Air", "Air"), tw_tanslate.get("Ship", "Ship")], [tw_tanslate.get("Bus", "Bus"), tw_tanslate.get("Car", "Car"), tw_tanslate.get("Garbage truck", "Garbage truck"), tw_tanslate.get("Motorcycle", "Motorcycle"), tw_tanslate.get("Rail", "Rail"), tw_tanslate.get("Refrigerated trucks", "Refrigerated trucks"), tw_tanslate.get("Tour bus", "Tour bus"), tw_tanslate.get("truck", "truck")]]

        # 到母公司的資產獲取資料並放進ef, upstream_info, downstream_info
        for globalAsset in company_assets_head:
            # 取得公司排放係數的資料
            companyAssets = globalAsset['companyAsset']
            # 根據各個類別去處理
            for companyAsset in companyAssets:
                scope = companyAsset['scope']
                scopeName = companyAsset['scopeName']       # 類別名稱（並非范疇名稱）
                assets = companyAsset['assets']             # 各個類別的資料

                for asset in assets:
                    if asset['category'] == "Emission Removal & Mitigation":        # 這個類別跳過
                        continue

                    # 根據判斷式去設定設備名稱（因為每個類別都有可能有一樣的設備名稱）
                    if "assetName" in asset:
                        assetName = asset['assetName']
                    elif "supplierName" in asset:
                        assetName = asset["supplierName"]
                    elif 'transportationName' in asset:
                        assetName = asset['transportationName']
                    else:
                        assetName = asset['name']
                    
                    # 根據判斷式去設定排放源（因為每個類別都有可能有一樣的排放源）
                    if "fuelType" in asset:
                        fuelType = tw_tanslate.get(asset['fuelType'], asset['fuelType'])
                    elif "calculationApproach" in asset:
                        fuelType = tw_tanslate.get(asset["calculationApproach"], asset["calculationApproach"])
                    elif "transportMode" in asset:
                        fuelType = tw_tanslate.get(asset["transportMode"], asset["transportMode"])
                    elif "capitalGood" in asset:
                        fuelType = tw_tanslate.get(asset["capitalGood"], asset["capitalGood"])
                    elif "Waste Treatment Method" in asset: 
                        fuelType = tw_tanslate.get(asset["Waste Treatment Method"], asset["Waste Treatment Method"])
                    elif "wasteTreatmentMethod" in asset:
                        fuelType = tw_tanslate.get(asset["wasteTreatmentMethod"], asset["wasteTreatmentMethod"])
                    else:
                        fuelType = ""

                    # 判斷是否為生質能源
                    biomass = asset.get('isBiomass')
                    if biomass == True:
                        biomass = "是"
                    else:
                        biomass = "否"

                    # 判斷是類別1~6
                    category = asset['category']
                    if category in category1_list:
                        append_category = "1"
                    elif category in category2_list:
                        append_category = "2"
                    elif category in category3_list:
                        append_category = "3"
                    elif category in category4_list:
                        append_category = "4"
                    elif category in category5_list:
                        append_category = "5"
                    elif category in category6_list:
                        append_category = "6"
                    else:
                        append_category = "Wrong Category"

                    # 以上若是類別1，則設定"類別1排放型式"
                    if category == category1_list[0]:
                       category1_type = "E,固定"
                    elif category == category1_list[1]:
                        category1_type = "T,移動"
                    elif category == category1_list[2]:
                       category1_type = "P,製程"
                    elif category == category1_list[3]:
                        category1_type = "F,逸散"
                    else:
                        category1_type = ""
                    
                    # 收集各個溫室氣體的資訊
                    co2 = asset.get('baseEmissionFactor')
                    co2Unit = asset.get('emissionUnit') + "/" + asset.get("baseUnit")
                    ch4 = asset.get('ch4EmissionValue')
                    ch4Unit = asset.get('ch4Unit') + "/" + asset.get("baseUnit")
                    n2o = asset.get('n2oEmissonValue')
                    n2oUnit = asset.get('n2oUnit') + "/" + asset.get("baseUnit")
                    hfc = asset.get('hfcsEmissonValue')
                    pfc = asset.get('pfcsEmissonValue')
                    sf6 = asset.get('sf6EmissonValue')
                    nf3 = asset.get('nf3EmissonValue')
                    source = asset.get('source')
                    if source == "EPA Taiwan" or source == "EPA(Taiwan)":
                        source = "台灣環境部"

                    # 取得活動數據種類、活動數據可信種類以及排放係數種類的編號
                    activityDataType = asset.get('activityDataType')
                    activityDataTypeValue = asset.get('activityDataTypeValue')
                    activityTrustedDataType = asset.get('activityTrustedDataType')
                    activityTrustedDataTypeValue = asset.get('activityTrustedDataTypeValue')
                    emissionFactorType = asset.get('emissionFactorType')
                    emissionFactorTypeValue = asset.get('emissionFactorTypeValue')

                    # 取得活動數據種類、活動數據可信種類以及排放係數種類的名稱
                    if activityDataType:
                        final_ADT = activityDataTypes.get(f"{activityDataType}-{activityDataTypeValue}", "")
                    else:
                        final_ADT = ""

                    if activityTrustedDataType:
                        final_ATDT = activityTrustedDataTypes.get(f"{activityTrustedDataType}-{activityTrustedDataTypeValue}", "")
                    else:
                        final_ATDT = ""

                    if emissionFactorType:
                        final_EFT = emissionFactorTypes.get(f"{emissionFactorType}-{emissionFactorTypeValue}", "")
                    else:
                        final_EFT = ""
            
                    # 取得活動數據、單位、總排放量等資訊
                    asset_datas = db.assetdatas.find({"company": ObjectId(companyId), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                    activityData = 0            # 初始活動數據
                    unit = asset['baseUnit']    
                    for asset_data in asset_datas:
                        consumption_datas = asset_data['consumption_data']
                        for consumption_data in consumption_datas:
                            consumption_yearly_data = consumption_data["consumption_yearly_data"]
                            for yearly_data in consumption_yearly_data:
                                if yearly_data["assetId"] == asset["_id"]:
                                    consumptionValues = yearly_data["consumptionValue"]
                                    for consumptionValue in consumptionValues:
                                        activityData += consumptionValue["consumptionValue"]

                    # 增加ef資訊
                    if fuelType in refrigerants_list:
                        ef[current_index] = {
                            "companyName": companyName,
                            "assetName": assetName,
                            "fuelType": fuelType,
                            "biomass": biomass,
                            "category": category,
                            "categoryNum": append_category,
                            "category1_type": category1_type,
                            "CO2": {"co2": "", "Unit": co2Unit, "source": source},
                            "CH4": {"ch4": "", "Unit": ch4Unit if ch4 else "", "source": source if ch4 else ""},
                            "N2O": {"n2o": "", "Unit": n2oUnit if n2o else "", "source": source if n2o else ""},
                            "HFC": {"hfc": co2, "source": source if co2 else ""},
                            "PFC": {"pfc": "", "source": source if pfc else ""},
                            "SF6": {"sf6": "", "source": source if sf6 else ""},
                            "NF3": {"nf3": "", "source": source if nf3 else ""},
                            "co2Label": "V" if hfc else "",
                            "ch4Label": "V" if ch4 else "",
                            "n2oLabel": "V" if n2o else "",
                            "hfcLabel": "V" if co2 else "",
                            "pfcLabel": "V" if pfc else "",
                            "sf6Label": "V" if sf6 else "",
                            "nf3Label": "V" if nf3 else "",
                            "activityDataType": final_ADT,
                            "activityTrustedDataType": final_ATDT,
                            "emissionFactorType": final_EFT,
                            "activityData": activityData,
                            "unit": unit
                        }
                    else:
                        ef[current_index] = {
                            "companyName": companyName,
                            "assetName": assetName,
                            "fuelType": fuelType,
                            "biomass": biomass,
                            "category": category,
                            "categoryNum": append_category,
                            "category1_type": category1_type,
                            "CO2": {"co2": co2, "Unit": co2Unit, "source": source},
                            "CH4": {"ch4": ch4, "Unit": ch4Unit if ch4 else "", "source": source if ch4 else ""},
                            "N2O": {"n2o": n2o, "Unit": n2oUnit if n2o else "", "source": source if n2o else ""},
                            "HFC": {"hfc": hfc, "source": source if hfc else ""},
                            "PFC": {"pfc": pfc, "source": source if pfc else ""},
                            "SF6": {"sf6": sf6, "source": source if sf6 else ""},
                            "NF3": {"nf3": nf3, "source": source if nf3 else ""},
                            "co2Label": "V" if co2 else "",
                            "ch4Label": "V" if ch4 else "",
                            "n2oLabel": "V" if n2o else "",
                            "hfcLabel": "V" if hfc else "",
                            "pfcLabel": "V" if pfc else "",
                            "sf6Label": "V" if sf6 else "",
                            "nf3Label": "V" if nf3 else "",
                            "activityDataType": final_ADT,
                            "activityTrustedDataType": final_ATDT,
                            "emissionFactorType": final_EFT,
                            "activityData": activityData,
                            "unit": unit
                        }
                        if ef.get(current_index-1) and scopeName in ("Upstream T&D", "Downstream T&D"):
                            if ef[current_index-1]["companyName"] == companyName and ef[current_index-1]["assetName"] == assetName:
                                if ef[current_index-1].get("asset_index") is None:
                                    ef[current_index-1]['asset_index'] = asset_index
                                    ef[current_index]["asset_index"] = asset_index
                                    ef[current_index-1]["scopeName"] = scopeName
                                    ef[current_index]["scopeName"] = scopeName
                                    ef[current_index-1]["stream_index"] = current_index_upstream if scopeName == "Upstream T&D" else current_index_downtream
                                    ef[current_index]["stream_index"] = current_index_upstream if scopeName == "Upstream T&D" else current_index_downtream
                                    asset_index += 1
                                else:
                                    ef[current_index]["asset_index"] = ef[current_index-1]["asset_index"]

                    current_index += 1                      # 編號+1

                    # 處理上游資料以便寫入3.2 上游運輸
                    if scopeName == "Upstream T&D":
                        if companyName+assetName not in upstream_info:      # 以公司名稱+設備名稱作為key值，避免重複（由於母公司和子公司可能同時存在某一個一模一樣的設備名稱）
                            upstream_info[companyName+assetName] = {
                                "serialNumber": current_index_upstream,
                                "companyName": companyName,
                                "assetName": assetName,
                                "activityData1": "",        # 海運/空運 的活動數據
                                "unit1": "",                # 海運/空運 的單位
                                "ghg1": "",                 # 海運/空運 的GHG
                                "emissionFactor1": "",      # 海運/空運 的排放因子
                                "emissionUnit1": "",        # 海運/空運 的排放單位
                                "emission1": f"""=IF(D{current_index_upstream+3}="", "", D{current_index_upstream+3}*G{current_index_upstream+3})""",            # 海運/空運 的排放量
                                "GWP1": "",                  # 海運/空運 的GWP
                                "totalemission1": f"""=IF(D{current_index_upstream+3}="", "", I{current_index_upstream+3}*J{current_index_upstream+3})""",            # 海運/空運 的總排放量   
                                "activityData2": "",        # 陸運 的活動數據
                                "unit2": "",                # 陸運 的單位emissionFactorValue
                                "ghg2": "",                 # 陸運 的GHG
                                "emissionFactor2": "",      # 陸運 的排放因子
                                "emissionUnit2": "",        # 陸運 的排放單位
                                "emission2": f"""=IF(L{current_index_upstream+3}="", "", L{current_index_upstream+3}*O{current_index_upstream+3})""",            # 陸運 的排放當量
                                "GWP2": "",                  # 陸運 的GWP
                                "totalemission2": f"""=IF(L{current_index_upstream+3}="", "", Q{current_index_upstream+3}*R{current_index_upstream+3})""",            # 陸運 的總排放量
                            }
                            current_index_upstream += 1     # 上游編號+1
                    
                        # 取得上游活動數據、單位、GHG、排放因子、GWP、總排放量等資訊
                        asset_datas = db.assetdatas.find({"company": ObjectId(companyId), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                        activityData = 0
                        unit = ""
                        for asset_data in asset_datas:
                            consumption_datas = asset_data['consumption_data']
                            for consumption_data in consumption_datas:
                                consumption_yearly_data = consumption_data["consumption_yearly_data"]
                                for yearly_data in consumption_yearly_data:
                                    if yearly_data["assetId"] == asset["_id"]:
                                        consumptionValues = yearly_data["consumptionValue"]
                                        for consumptionValue in consumptionValues:
                                            activityData += consumptionValue["consumptionValue"]
                                            unit = consumptionValue["unit"]
                                            ghg = "CO₂"
                                            emissionFactorValue = consumptionValue["emissionFactorValue"]
                                            GWP = 1
                                            emissionUnit = asset["emissionUnit"]

                                            if fuelType in ModeOfTransport[0]:
                                                upstream_info[companyName+assetName]["activityData1"] = activityData
                                                upstream_info[companyName+assetName]["unit1"] = unit
                                                upstream_info[companyName+assetName]["ghg1"] = ghg
                                                upstream_info[companyName+assetName]["emissionFactor1"] = emissionFactorValue
                                                upstream_info[companyName+assetName]["emissionUnit1"] = emissionUnit
                                                upstream_info[companyName+assetName]["GWP1"] = GWP
                                            elif fuelType in ModeOfTransport[1]:
                                                upstream_info[companyName+assetName]["activityData2"] = activityData
                                                upstream_info[companyName+assetName]["unit2"] = unit
                                                upstream_info[companyName+assetName]["ghg2"] = ghg
                                                upstream_info[companyName+assetName]["emissionFactor2"] = emissionFactorValue
                                                upstream_info[companyName+assetName]["emissionUnit2"] = emissionUnit
                                                upstream_info[companyName+assetName]["GWP2"] = GWP
                                            else:
                                                logger.error(f"Error fuelType: {fuelType}")

                    # 處理上游資料以便寫入3.3 下游運輸
                    if scopeName == "Downstream T&D":
                        if companyName+assetName not in downstream_info:
                            downstream_info[companyName+assetName] = {
                                "serialNumber": current_index_downtream,
                                "companyName": companyName, 
                                "assetName": assetName,
                                "activityData1": "",        # 海運/空運 的活動數據
                                "unit1": "",                # 海運/空運 的單位
                                "ghg1": "",                 # 海運/空運 的GHG
                                "emissionFactor1": "",      # 海運/空運 的排放因子
                                "emissionUnit1": "",        # 海運/空運 的排放單位
                                "emission1": f"""=IF(D{current_index_downtream+3}="", "", D{current_index_downtream+3}*G{current_index_downtream+3})""",            # 海運/空運 的排放量
                                "GWP1": "",                  # 海運/空運 的GWP
                                "totalemission1": f"""=IF(D{current_index_downtream+3}="", "", I{current_index_downtream+3}*J{current_index_downtream+3})""",            # 海運/空運 的總排放量   
                                "activityData2": "",        # 陸運 的活動數據
                                "unit2": "",                # 陸運 的單位emissionFactorValue
                                "ghg2": "",                 # 陸運 的GHG
                                "emissionFactor2": "",      # 陸運 的排放因子
                                "emissionUnit2": "",        # 陸運 的排放單位
                                "emission2": f"""=IF(L{current_index_downtream+3}="", "", L{current_index_downtream+3}*O{current_index_downtream+3})""",            # 陸運 的排放當量
                                "GWP2": "",                  # 陸運 的GWP
                                "totalemission2": f"""=IF(L{current_index_downtream+3}="", "", Q{current_index_downtream+3}*R{current_index_downtream+3})""",            # 陸運 的總排放量
                            }
                            current_index_downtream += 1
                        asset_datas = db.assetdatas.find({"company": ObjectId(companyId), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                        activityData = 0
                        unit = ""
                        for asset_data in asset_datas:
                            consumption_datas = asset_data['consumption_data']
                            for consumption_data in consumption_datas:
                                consumption_yearly_data = consumption_data["consumption_yearly_data"]
                                for yearly_data in consumption_yearly_data:
                                    if yearly_data["assetId"] == asset["_id"]:
                                        consumptionValues = yearly_data["consumptionValue"]
                                        for consumptionValue in consumptionValues:
                                            activityData += consumptionValue["consumptionValue"]
                                            unit = consumptionValue["unit"]
                                            ghg = "CO₂"
                                            emissionFactorValue = consumptionValue["emissionFactorValue"]
                                            GWP = 1
                                            emissionUnit = asset["emissionUnit"]

                                            if fuelType in ModeOfTransport[0]:
                                                downstream_info[companyName+assetName]["activityData1"] = activityData
                                                downstream_info[companyName+assetName]["unit1"] = unit
                                                downstream_info[companyName+assetName]["ghg1"] = ghg
                                                downstream_info[companyName+assetName]["emissionFactor1"] = emissionFactorValue
                                                downstream_info[companyName+assetName]["emissionUnit1"] = emissionUnit
                                                downstream_info[companyName+assetName]["GWP1"] = GWP
                                            elif fuelType in ModeOfTransport[1]:
                                                downstream_info[companyName+assetName]["activityData2"] = activityData
                                                downstream_info[companyName+assetName]["unit2"] = unit
                                                downstream_info[companyName+assetName]["ghg2"] = ghg
                                                downstream_info[companyName+assetName]["emissionFactor2"] = emissionFactorValue
                                                downstream_info[companyName+assetName]["emissionUnit2"] = emissionUnit
                                                downstream_info[companyName+assetName]["GWP2"] = GWP
                                            else:
                                                logger.error(f"Error fuelType: {fuelType}")

        # 到子公司的資產獲取資料並放進ef, upstream_info, downstream_info（跟母公司邏輯一樣）
        for subName, _id in site_modules_sub.items():
            sub_company_assets = db.company_assets.find({"company": ObjectId(_id)})
            for sub_globalAsset in sub_company_assets:
                sub_companyAssets = sub_globalAsset['companyAsset']
                for sub_companyAsset in sub_companyAssets:
                    sub_scope = sub_companyAsset['scope']
                    sub_scopeName = sub_companyAsset['scopeName']
                    sub_assets = sub_companyAsset['assets']

                    for sub_asset in sub_assets:
                        if sub_asset['category'] == "Emission Removal & Mitigation":
                            continue
                        if "assetName" in sub_asset:
                            sub_assetName = sub_asset['assetName']
                        elif "supplierName" in sub_asset:
                            sub_assetName = sub_asset["supplierName"]
                        elif 'transportationName' in sub_asset:
                            sub_assetName = sub_asset['transportationName']
                        else:
                            sub_assetName = sub_asset['name']
                        
                        if "fuelType" in sub_asset:
                            sub_fuelType = tw_tanslate.get(sub_asset['fuelType'], sub_asset['fuelType'])
                        elif "calculationApproach" in sub_asset:
                            sub_fuelType = tw_tanslate.get(sub_asset["calculationApproach"], sub_asset["calculationApproach"])
                        elif "transportMode" in sub_asset:
                            sub_fuelType = tw_tanslate.get(sub_asset["transportMode"], sub_asset["transportMode"])
                        elif "capitalGood" in sub_asset:
                            sub_fuelType = tw_tanslate.get(sub_asset["capitalGood"], sub_asset["capitalGood"])
                        elif "Waste Treatment Method" in sub_asset: 
                            sub_fuelType = tw_tanslate.get(sub_asset["Waste Treatment Method"], sub_asset["Waste Treatment Method"])
                        else:
                            sub_fuelType = tw_tanslate.get(sub_asset["wasteTreatmentMethod"], sub_asset["wasteTreatmentMethod"])

                        sub_biomass = sub_asset.get('isBiomass')
                        if sub_biomass == True:
                            sub_biomass = "是"
                        else:
                            sub_biomass = "否"

                        sub_category = sub_asset['category']
                        if sub_category in category1_list:
                            sub_append_category = "1"
                        elif sub_category in category2_list:
                            sub_append_category = "2"
                        elif sub_category in category3_list:
                            sub_append_category = "3"
                        elif sub_category in category4_list:
                            sub_append_category = "4"
                        elif sub_category in category5_list:
                            sub_append_category = "5"
                        elif sub_category in category6_list:
                            sub_append_category = "6"
                        else:
                            sub_append_category = "Wrong Category"
                        
                        if sub_category == category1_list[0]:
                            sub_category1_type = "E,固定"
                        elif sub_category == category1_list[1]:
                            sub_category1_type = "T,移動"
                        elif sub_category == category1_list[2]:
                            sub_category1_type = "P,製程"
                        elif sub_category == category1_list[3]:
                            sub_category1_type = "F,逸散"
                        else:
                            sub_category1_type = ""
                        
                        sub_co2 = sub_asset.get('baseEmissionFactor')
                        sub_co2Unit = sub_asset.get('emissionUnit') + "/" + sub_asset.get("baseUnit")
                        sub_ch4 = sub_asset.get('ch4EmissionValue')
                        sub_ch4Unit = sub_asset.get('ch4Unit') + "/" + sub_asset.get("baseUnit")
                        sub_n2o = sub_asset.get('n2oEmissonValue')
                        sub_n2oUnit = sub_asset.get('n2oUnit') + "/" + sub_asset.get("baseUnit")
                        sub_hfc = sub_asset.get('hfcsEmissonValue')
                        sub_pfc = sub_asset.get('pfcsEmissonValue')
                        sub_sf6 = sub_asset.get('sf6EmissonValue')
                        sub_nf3 = sub_asset.get('nf3EmissonValue')
                        sub_source = sub_asset.get('source')
                        if sub_source == "EPA Taiwan" or sub_source == "EPA(Taiwan)":
                            sub_source = "台灣環境部"
                        
                        activityDataType = sub_asset.get('activityDataType')
                        activityDataTypeValue = sub_asset.get('activityDataTypeValue')
                        activityTrustedDataType = sub_asset.get('activityTrustedDataType')
                        activityTrustedDataTypeValue = sub_asset.get('activityTrustedDataTypeValue')
                        emissionFactorType = sub_asset.get('emissionFactorType')
                        emissionFactorTypeValue = sub_asset.get('emissionFactorTypeValue')

                        if activityDataType:
                            final_ADT = activityDataTypes[f"{activityDataType}-{activityDataTypeValue}"]
                        else:
                            final_ADT = ""

                        if activityTrustedDataType:
                            final_ATDT = activityTrustedDataTypes[f"{activityTrustedDataType}-{activityTrustedDataTypeValue}"]
                        else:
                            final_ATDT = ""

                        if emissionFactorType:
                            final_EFT = emissionFactorTypes[f"{emissionFactorType}-{emissionFactorTypeValue}"]
                        else:
                            final_EFT = ""

                        sub_asset_datas = db.assetdatas.find({"company": ObjectId(_id), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                        sub_activityData = 0
                        sub_unit = ""
                        for sub_asset_data in sub_asset_datas:
                            sub_consumption_datas = sub_asset_data['consumption_data']
                            for sub_consumption_data in sub_consumption_datas:
                                sub_consumption_yearly_data = sub_consumption_data["consumption_yearly_data"]
                                for sub_yearly_data in sub_consumption_yearly_data:
                                    if sub_yearly_data["assetId"] == sub_asset["_id"]:
                                        sub_consumptionValues = sub_yearly_data["consumptionValue"]
                                        for sub_consumptionValue in sub_consumptionValues:
                                            sub_activityData += sub_consumptionValue["consumptionValue"]
                                            sub_unit = sub_consumptionValue["unit"]

                        if sub_fuelType in refrigerants_list:
                            ef[current_index] = {
                                "companyName": subName,
                                "assetName": sub_assetName,
                                "fuelType": sub_fuelType,
                                "biomass": sub_biomass,
                                "category": sub_category,
                                "categoryNum": sub_append_category,
                                "category1_type": sub_category1_type,
                                "CO2": {"co2": "", "Unit": sub_co2Unit if sub_co2 else "", "source": sub_source if sub_co2 else ""},
                                "CH4": {"ch4": "", "Unit": sub_ch4Unit if sub_ch4 else "", "source": sub_source if sub_ch4 else ""},
                                "N2O": {"n2o": "sub_n2o", "Unit": sub_n2oUnit if sub_n2o else "", "source": sub_source if sub_n2o else ""},
                                "HFC": {"hfc": sub_co2, "source": sub_source if sub_co2 else ""},
                                "PFC": {"pfc": "", "source": sub_source if sub_pfc else ""},
                                "SF6": {"sf6": "", "source": sub_source if sub_sf6 else ""},
                                "NF3": {"nf3": "", "source": sub_source if sub_nf3 else ""},
                                "co2Label": "V" if sub_hfc else "",
                                "ch4Label": "V" if sub_ch4 else "",
                                "n2oLabel": "V" if sub_n2o else "",
                                "hfcLabel": "V" if sub_co2 else "",
                                "pfcLabel": "V" if sub_pfc else "",
                                "sf6Label": "V" if sub_sf6 else "",
                                "nf3Label": "V" if sub_nf3 else "",
                                "activityDataType": final_ADT,
                                "activityTrustedDataType": final_ATDT,
                                "emissionFactorType": final_EFT,
                                "activityData": activityData,
                                "unit": sub_unit
                            }
                        else:
                            ef[current_index] = {
                                "companyName": subName,
                                "assetName": sub_assetName,
                                "fuelType": sub_fuelType,
                                "biomass": sub_biomass,
                                "category": sub_category,
                                "categoryNum": sub_append_category,
                                "category1_type": sub_category1_type,
                                "CO2": {"co2": sub_co2, "Unit": sub_co2Unit if sub_co2 else "", "source": sub_source if sub_co2 else ""},
                                "CH4": {"ch4": sub_ch4, "Unit": sub_ch4Unit if sub_ch4 else "", "source": sub_source if sub_ch4 else ""},
                                "N2O": {"n2o": sub_n2o, "Unit": sub_n2oUnit if sub_n2o else "", "source": sub_source if sub_n2o else ""},
                                "HFC": {"hfc": sub_hfc, "source": sub_source if sub_hfc else ""},
                                "PFC": {"pfc": sub_pfc, "source": sub_source if sub_pfc else ""},
                                "SF6": {"sf6": sub_sf6, "source": sub_source if sub_sf6 else ""},
                                "NF3": {"nf3": sub_nf3, "source": sub_source if sub_nf3 else ""},
                                "co2Label": "V" if sub_co2 else "",
                                "ch4Label": "V" if sub_ch4 else "",
                                "n2oLabel": "V" if sub_n2o else "",
                                "hfcLabel": "V" if sub_hfc else "",
                                "pfcLabel": "V" if sub_pfc else "",
                                "sf6Label": "V" if sub_sf6 else "",
                                "nf3Label": "V" if sub_nf3 else "",
                                "activityDataType": final_ADT,
                                "activityTrustedDataType": final_ATDT,
                                "emissionFactorType": final_EFT,
                                "activityData": activityData,
                                "unit": sub_unit
                            }
                        if ef.get(current_index-1) and sub_scopeName in ("Upstream T&D", "Downstream T&D"):
                            if ef[current_index-1]["companyName"] == subName and ef[current_index-1]["assetName"] == sub_assetName:
                                if ef[current_index-1].get("asset_index") is None:
                                    ef[current_index-1]['asset_index'] = asset_index
                                    ef[current_index]["asset_index"] = asset_index
                                    ef[current_index-1]["scopeName"] = sub_scopeName
                                    ef[current_index]["scopeName"] = sub_scopeName
                                    ef[current_index-1]["stream_index"] = current_index_upstream if scopeName == "Upstream T&D" else current_index_downtream
                                    ef[current_index]["stream_index"] = current_index_upstream if scopeName == "Upstream T&D" else current_index_downtream
                                    asset_index += 1
                                else:
                                    ef[current_index]["asset_index"] = ef[current_index-1]["asset_index"]

                        current_index += 1
                        if sub_scopeName == "Upstream T&D":
                            if subName+sub_assetName not in upstream_info:
                                upstream_info[subName+sub_assetName] = {
                                    "serialNumber": current_index_upstream,
                                    "companyName": subName, 
                                    "assetName": sub_assetName,
                                    "activityData1": "",        # 海運/空運 的活動數據
                                    "unit1": "",                # 海運/空運 的單位
                                    "ghg1": "",                 # 海運/空運 的GHG
                                    "emissionFactor1": "",      # 海運/空運 的排放因子
                                    "emissionUnit1": "",        # 海運/空運 的排放單位
                                    "emission1": f"""=IF(D{current_index_upstream+3}="", "", D{current_index_upstream+3}*G{current_index_upstream+3})""",            # 海運/空運 的排放量
                                    "GWP1": "",                  # 海運/空運 的GWP
                                    "totalemission1": f"""=IF(D{current_index_upstream+3}="", "", I{current_index_upstream+3}*J{current_index_upstream+3})""",        # 海運/空運 的總排放量   
                                    "activityData2": "",        # 陸運 的活動數據
                                    "unit2": "",                # 陸運 的單位emissionFactorValue
                                    "ghg2": "",                 # 陸運 的GHG
                                    "emissionFactor2": "",      # 陸運 的排放因子
                                    "emissionUnit2": "",        # 陸運 的排放單位
                                    "emission2": f"""=IF(L{current_index_upstream+3}="", "", L{current_index_upstream+3}*O{current_index_upstream+3})""",            # 陸運 的排放當量
                                    "GWP2": "",                  # 陸運 的GWP
                                    "totalemission2": f"""=IF(L{current_index_upstream+3}="", "", Q{current_index_upstream+3}*R{current_index_upstream+3})""",        # 陸運 的總排放量
                                }
                                current_index_upstream += 1
                            asset_datas = db.assetdatas.find({"company": ObjectId(_id), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                            activityData = 0
                            unit = ""
                            for asset_data in asset_datas:    
                                consumption_datas = asset_data['consumption_data']                                
                                for consumption_data in consumption_datas:
                                    consumption_yearly_data = consumption_data["consumption_yearly_data"]
                                    for yearly_data in consumption_yearly_data:
                                        if yearly_data["assetId"] == sub_asset["_id"]:
                                            consumptionValues = yearly_data["consumptionValue"]
                                            for consumptionValue in consumptionValues:
                                                activityData += consumptionValue["consumptionValue"]
                                                unit = consumptionValue["unit"]
                                                ghg = "CO₂"
                                                emissionFactorValue = consumptionValue["emissionFactorValue"] 
                                                GWP = 1
                                                emissionUnit = sub_asset["emissionUnit"]

                                                if sub_fuelType in ModeOfTransport[0]:
                                                    upstream_info[subName+sub_assetName]["activityData1"] = activityData
                                                    upstream_info[subName+sub_assetName]["unit1"] = unit
                                                    upstream_info[subName+sub_assetName]["ghg1"] = ghg
                                                    upstream_info[subName+sub_assetName]["emissionFactor1"] = emissionFactorValue
                                                    upstream_info[subName+sub_assetName]["emissionUnit1"] = emissionUnit
                                                    upstream_info[subName+sub_assetName]["GWP1"] = GWP
                                                elif sub_fuelType in ModeOfTransport[1]:
                                                    upstream_info[subName+sub_assetName]["activityData2"] = activityData
                                                    upstream_info[subName+sub_assetName]["unit2"] = unit
                                                    upstream_info[subName+sub_assetName]["ghg2"] = ghg
                                                    upstream_info[subName+sub_assetName]["emissionFactor2"] = emissionFactorValue
                                                    upstream_info[subName+sub_assetName]["emissionUnit2"] = emissionUnit
                                                    upstream_info[subName+sub_assetName]["GWP2"] = GWP
                                                else:
                                                    logger.error(f"Error fuelType: {sub_fuelType}")
                
                        if sub_scopeName == "Downstream T&D":
                            if subName+sub_assetName not in downstream_info:
                                downstream_info[subName+sub_assetName] = {
                                    "serialNumber": current_index_downtream,
                                    "companyName": subName, 
                                    "assetName": sub_assetName,
                                    "activityData1": "",        # 海運/空運 的活動數據
                                    "unit1": "",                # 海運/空運 的單位
                                    "ghg1": "",                 # 海運/空運 的GHG
                                    "emissionFactor1": "",      # 海運/空運 的排放因子
                                    "emissionUnit1": "",        # 海運/空運 的排放單位
                                    "emission1": f"""=IF(D{current_index_downtream+3}="", "", D{current_index_downtream+3}*G{current_index_downtream+3})""",            # 海運/空運 的排放量
                                    "GWP1": "",                  # 海運/空運 的GWP
                                    "totalemission1": f"""=IF(D{current_index_downtream+3}="", "", I{current_index_downtream+3}*J{current_index_downtream+3})""",        # 海運/空運 的總排放量   
                                    "activityData2": "",        # 陸運 的活動數據
                                    "unit2": "",                # 陸運 的單位emissionFactorValue
                                    "ghg2": "",                 # 陸運 的GHG
                                    "emissionFactor2": "",      # 陸運 的排放因子
                                    "emissionUnit2": "",        # 陸運 的排放單位
                                    "emission2": f"""=IF(L{current_index_downtream+3}="", "", L{current_index_downtream+3}*O{current_index_downtream+3})""",            # 陸運 的排放當量
                                    "GWP2": "",                  # 陸運 的GWP
                                    "totalemission2": f"""=IF(L{current_index_downtream+3}="", "", Q{current_index_downtream+3}*R{current_index_downtream+3})""",        # 陸運 的總排放量陸運
                                }
                                current_index_downtream += 1
                            asset_datas = db.assetdatas.find({"company": ObjectId(_id), "consumption_data.year": inventory_year}, {"_id": 0, "consumption_data.$": 1})
                            for asset_data in asset_datas:
                                activityData = 0
                                consumption_datas = asset_data['consumption_data']
                                for consumption_data in consumption_datas:
                                    consumption_yearly_data = consumption_data["consumption_yearly_data"]
                                    for yearly_data in consumption_yearly_data:
                                        if yearly_data["assetId"] == sub_asset["_id"]:
                                            consumptionValues = yearly_data["consumptionValue"]
                                            for consumptionValue in consumptionValues:
                                                activityData += consumptionValue["consumptionValue"]
                                                unit = consumptionValue["unit"]
                                                ghg = "CO₂"
                                                emissionFactorValue = consumptionValue["emissionFactorValue"]
                                                GWP = 1
                                                emissionUnit = sub_asset["emissionUnit"]

                                                if sub_fuelType in ModeOfTransport[0]:
                                                    downstream_info[subName+sub_assetName]["activityData1"] = activityData
                                                    downstream_info[subName+sub_assetName]["unit1"] = unit
                                                    downstream_info[subName+sub_assetName]["ghg1"] = ghg
                                                    downstream_info[subName+sub_assetName]["emissionFactor1"] = emissionFactorValue
                                                    downstream_info[subName+sub_assetName]["emissionUnit1"] = emissionUnit
                                                    downstream_info[subName+sub_assetName]["GWP1"] = GWP
                                                elif sub_fuelType in ModeOfTransport[1]:
                                                    downstream_info[subName+sub_assetName]["activityData2"] = activityData
                                                    downstream_info[subName+sub_assetName]["unit2"] = unit
                                                    downstream_info[subName+sub_assetName]["ghg2"] = ghg
                                                    downstream_info[subName+sub_assetName]["emissionFactor2"] = emissionFactorValue
                                                    downstream_info[subName+sub_assetName]["emissionUnit2"] = emissionUnit
                                                    downstream_info[subName+sub_assetName]["GWP2"] = GWP
                                                else:
                                                    logger.error(f"Error fuelType: {sub_fuelType}")
        
        index_process = []
        skip_index = 0
        for index, item in ef.items():
            if index == skip_index:
                continue
            asset_index = item.get("asset_index")
            if asset_index is None:
                index_process.append(index)
            else:
                index_process.append(index)
                skip_index = index + 1
        new_index = [i+1 for i in range(len(index_process))]
        new_ef = {}
        categories = {}
        for i in range(len(index_process)):
            if ef[index_process[i]].get("asset_index"):
                # 49: {'companyName': 'Taiwan Aaron', 'assetName': '產品D', 'fuelType': '航空', 'biomass': '否', 'category': 'Downstream T&D', 'categoryNum': '3', 'category1_type': '', 'CO2': {'co2': 0.281, 'Unit': 'kgCO₂/passenger-km', 'source': '台灣環境部'}, 'CH4': {'ch4': None, 'Unit': 'kgCH₄/passenger-km', 'source': '台灣環境部'}, 'N2O': {'n2o': None, 'Unit': 'kgN₂O/passenger-km', 'source': '台灣環境部'}, 'HFC': {'hfc': 0, 'source': '台灣環境部'}, 'PFC': {'pfc': 0, 'source': '台灣環境部'}, 'SF6': {'sf6': 0, 'source': '台灣環境部'}, 'NF3': {'nf3': 0, 'source': '台灣環境部'}, 'co2Label': 'V', 'ch4Label': '', 'n2oLabel': '', 'hfcLabel': '', 'pfcLabel': '', 'sf6Label': '', 'nf3Label': '', 'activityDataType': '', 'activityTrustedDataType': '', 'emissionFactorType': '', 'activityData': 66, 'unit': 'passenger-km', 'asset_index': 1}, 
                # 50: {'companyName': 'Taiwan Aaron', 'assetName': '產品D', 'fuelType': '小客車', 'biomass': '否', 'category': 'Downstream T&D', 'categoryNum': '3', 'category1_type': '', 'CO2': {'co2': 0.133, 'Unit': 'kgCO₂/passenger-km', 'source': '台灣環境部'}, 'CH4': {'ch4': None, 'Unit': 'kgCH₄/passenger-km', 'source': '台灣環境部'}, 'N2O': {'n2o': None, 'Unit': 'kgN₂O/passenger-km', 'source': '台灣環境部'}, 'HFC': {'hfc': 0, 'source': '台灣環境部'}, 'PFC': {'pfc': 0, 'source': '台灣環境部'}, 'SF6': {'sf6': 0, 'source': '台灣環境部'}, 'NF3': {'nf3': 0, 'source': '台灣環境部'}, 'co2Label': 'V', 'ch4Label': '', 'n2oLabel': '', 'hfcLabel': '', 'pfcLabel': '', 'sf6Label': '', 'nf3Label': '', 'activityDataType': '', 'activityTrustedDataType': '', 'emissionFactorType': '', 'activityData': 77, 'unit': 'passenger-km', 'asset_index': 1},
                current_ef = ef[index_process[i]]
                next_ef = ef[index_process[i]+1]

                scopeName = current_ef["scopeName"]

                new_ef[new_index[i]] = {
                    "companyName": current_ef["companyName"],
                    "assetName": current_ef["assetName"],
                    "fuelType": "上游" if scopeName == "Upstream T&D" else "下游",
                    "biomass": current_ef["biomass"],
                    "category": current_ef["category"],
                    "categoryNum": current_ef["categoryNum"],
                    "category1_type": current_ef["category1_type"],
                    "CO2": {"co2": 1, "Unit": "總排放量", "source": current_ef["CO2"]["source"] + '/' + next_ef["CO2"]["source"] if current_ef["CO2"]["source"] != next_ef["CO2"]["source"] else current_ef["CO2"]["source"]},
                    "CH4": current_ef["CH4"],
                    "N2O": current_ef["N2O"],
                    "HFC": current_ef["HFC"],
                    "PFC": current_ef["PFC"],
                    "SF6": current_ef["SF6"],
                    "NF3": current_ef["NF3"],
                    "co2Label": current_ef["co2Label"],
                    "ch4Label": current_ef["ch4Label"],
                    "n2oLabel": current_ef["n2oLabel"],
                    "hfcLabel": current_ef["hfcLabel"],
                    "pfcLabel": current_ef["pfcLabel"],
                    "sf6Label": current_ef["sf6Label"],
                    "nf3Label": current_ef["nf3Label"],
                    "activityDataType": current_ef["activityDataType"],
                    "activityTrustedDataType": current_ef["activityTrustedDataType"],
                    "emissionFactorType": current_ef["emissionFactorType"],
                    "activityData": current_ef["activityData"],
                    "unit": current_ef["unit"],
                    "scopeName" : scopeName,
                    "stream_index": current_ef["stream_index"],
                }
                # categories[new_index[i]] = current_ef["category"]
                if current_ef["category"] not in categories:
                    categories[current_ef["category"]] = [new_index[i]]
                else:
                    categories[current_ef["category"]].append(new_index[i])
            else:
                new_ef[new_index[i]] = ef[index_process[i]]
                if ef[index_process[i]]["category"] not in categories:
                    categories[ef[index_process[i]]["category"]] = [new_index[i]]
                else:
                    categories[ef[index_process[i]]["category"]].append(new_index[i])
        ef = new_ef
        ef_sort = {}
        new_index = 1
        for category in category1_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue
        for category in category2_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue
        for category in category3_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue
        for category in category4_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue
        for category in category5_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue
        for category in category6_list:
            current_process_category = categories.get(category)
            if current_process_category:
                for index in current_process_category:
                    ef_sort[new_index] = ef[index]
                    new_index += 1
            else:
                continue

        ef = ef_sort

        # 設定變數ef的個數、上游的個數以及下游的個數
        numOfEF = len(ef)
        numOfUpstream = len(upstream_info.values())
        numOfDownstream = len(downstream_info.values())

        process_list = [10, 30, 18] + [30 for _ in range(len(ef))]
        for i in range(len(process_list)):
            worksheet.set_row(i, process_list[i])
        # 將ef的資料裡面的溫室氣體判斷寫入P:AD的隱藏單元格子
        for index, item in ef.items():
            worksheet.write(f"A{index+3}", index, cf17)
            worksheet.write(f"B{index+3}", item["companyName"], cf17)
            worksheet.write(f"C{index+3}", item["assetName"], cf17)
            worksheet.write(f"D{index+3}", item["fuelType"], cf17)
            worksheet.write(f"E{index+3}", item["biomass"], cf17)
            worksheet.write(f"F{index+3}", item["categoryNum"], cf17)
            worksheet.write(f"G{index+3}", item["category1_type"], cf17)
            worksheet.write(f"H{index+3}", item['co2Label'], cf17)
            worksheet.write(f"I{index+3}", item['ch4Label'], cf17)
            worksheet.write(f"J{index+3}", item['n2oLabel'], cf17)
            worksheet.write(f"K{index+3}", item['hfcLabel'], cf17)  
            worksheet.write(f"L{index+3}", item['pfcLabel'], cf17)
            worksheet.write(f"M{index+3}", item['sf6Label'], cf17)
            worksheet.write(f"N{index+3}", item['nf3Label'], cf17)
            worksheet.write(f"O{index+3}", "", cf17)

            worksheet.write(f"P{index+3}", f"""=IF(H{index+3}<>"","a","")""", cf25)
            worksheet.write(f"Q{index+3}", f"""=IF(I{index+3}<>"","b","")""", cf25)
            worksheet.write(f"R{index+3}", f"""=IF(J{index+3}<>"","c","")""", cf25)
            worksheet.write(f"S{index+3}", f"""=IF(K{index+3}<>"","d","")""", cf25)
            worksheet.write(f"T{index+3}", f"""=IF(L{index+3}<>"","e","")""", cf25)
            worksheet.write(f"U{index+3}", f"""=IF(M{index+3}<>"","f","")""", cf25)
            worksheet.write(f"V{index+3}", f"""=IF(N{index+3}<>"","g","")""", cf25)
            worksheet.write(f"W{index+3}", f"=P{index+3}&Q{index+3}&R{index+3}&S{index+3}&T{index+3}&U{index+3}&V{index+3}", cf25)
            worksheet.write(f"X{index+3}", f"""=IF(W{index+3}="","",VLOOKUP(W{index+3},AA2:AD58,2,0))""", cf25)
            worksheet.write(f"Y{index+3}", f"""=IF(X{index+3}="","",VLOOKUP(W{index+3},AA2:AD58,3,0))""", cf25)
            worksheet.write(f"Z{index+3}", f"""=IF(Y{index+3}="","",VLOOKUP(W{index+3},AA2:AD58,4,0))""", cf25)
        
        # 完成表格，作為上面vloopup的值
        process_list = ["a", "b", "c", "d", "e", "f", "g", "ab", "f", "af", "ag", "bc", "bd", "ce", "cf", "cg", "de", "df", "dg", "ef", "eg", "fg", "abc", "abd", "abe", "abf", "abg", "acd", "ace", "acf", "acg", "ade", "adf", "adg", "aef", "aeg", "bcd", "bce", "bcf", "bcg", "bde", "bdf", "bdg", "bef", "beg", "cde", "cdf", "cdg", "cef", "ceg", "def", "deg", "efg"]
        for i in range(len(process_list)):
            worksheet.write(f"AA{i+2}", process_list[i], cf14)
        process_list = ["CO₂", "CH₄","N₂O","HFC\u209b","PFC\u209b","SF₆","NF₃","CO₂","SF₆","CO₂","CO₂","CH₄","CH₄","N₂O","N₂O","N₂O","HFC\u209b","HFC\u209b","HFC\u209b","PFC\u209b","PFC\u209b","SF₆","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CO₂","CH₄","CH₄","CH₄","CH₄","CH₄","CH₄","CH₄","CH₄","CH₄","N₂O","N₂O","N₂O","N₂O","N₂O","HFC\u209b","HFC\u209b","PFC\u209b"]
        for i in range(len(process_list)):
            worksheet.write(f"AB{i+2}", process_list[i], cf14) 
        process_list = ["", "", "", "", "", "", "", "CH₄", "", "SF₆","NF₃","N₂O","HFC\u209b","PFC\u209b","SF₆","NF₃","PFC\u209b","SF₆","NF₃","SF₆","NF₃","NF₃","CH₄","CH₄","CH₄","CH₄","CH₄","N₂O","N₂O","N₂O","N₂O","HFC\u209b","HFC\u209b","HFC\u209b","PFC\u209b","PFC\u209b","N₂O","N₂O","N₂O","N₂O","HFC\u209b","HFC\u209b","HFC\u209b","PFC\u209b","PFC\u209b","HFC\u209b","HFC\u209b","HFC\u209b","PFC\u209b","PFC\u209b","PFC\u209b","PFC\u209b","SF₆"]
        for i in range(len(process_list)):
            worksheet.write(f"AC{i+2}", process_list[i], cf14)
        process_list = ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "N₂O", "HFC\u209b", "PFC\u209b", "SF₆", "NF₃", "HFC\u209b", "PFC\u209b", "SF₆", "NF₃", "PFC\u209b", "SF₆", "NF₃", "SF₆", "NF₃", "HFC\u209b", "PFC\u209b", "SF₆", "NF₃", "PFC\u209b", "SF₆", "NF₃", "SF₆", "NF₃", "PFC\u209b", "SF₆", "NF₃", "SF₆", "NF₃", "SF₆", "NF₃", "NF₃"]
        for i in range(len(process_list)):
            worksheet.write(f"AD{i+2}", process_list[i], cf14)
        
        # 將上面的表格資料隱藏
        worksheet.set_column("P:AD", None, None, {"hidden": True})

        logger.info("生成 2.1-重大性準則")
        # ------------------------- 2.1-重大性準則 -----------------------------
        # 之後用來儲存邊界設定資料
        on_boarding_info = {}
        
        # 從MongoDB取得邊界設定資料
        onboardingRating = db.get_collection('onbording-ratings').find({"companyId":ObjectId(companyId)})

        # 取得邊界設定的翻譯
        findTranslate = db.translations.find_one({"url": "on-boarding-information"})
        tw_translate = findTranslate['tran'][5]['data']
        
        # 完成邊界設定資料的儲存
        for OR in onboardingRating:
            threshold = OR['threshold']         # 重大間接溫室氣體排放源之評估門檻的閥值
            rating = OR['rating']               # 各個類別的rating
            # on_boarding_info["threshold"] = threshold
            for rate in rating:
                findCategory = db.get_collection('system-sub-categories').find_one({"_id":ObjectId(rate['subCategory'])})
                category = findCategory['label']
                tw = tw_translate[category]

                rating1 = rate.get('rating1')       # 數據可信度
                rating2 = rate.get('rating2')       # 排放因子來源
                rating3 = rate.get('rating3')       # 減量措施推行可信度
                rating4 = rate.get('rating4')       # 發生頻率
                rating5 = rate.get('rating5')       # 排放量

                # 如果存在rating1，表示客戶在報告邊界的法律或客戶的要求選擇否，反之則選擇是
                if rating1:
                    yesORno = "否"
                    total = rating1 + rating2 + rating3 + rating4 + rating5
                    if total >= threshold:      # 大於設定的閥值
                        disclosure_req = 'V'
                    else: 
                        disclosure_req = 'X'
                else:
                    yesORno = "是"
                    total = ""
                    disclosure_req = "V"
                # 設定各個類別的description（NXMap-邊界設定-報告邊界的各個類別的說明）
                if '1.1' in tw:
                    desc = "固定式設備之燃料燃燒，如鍋爐、加熱爐、緊急發電機等設備。固定式設備中燃燒任何類型的燃料（煤炭、天然氣、重油等）所產生的排放。電力、熱、蒸汽\n或其他化石燃料衍生能源產生之溫室氣體排放。"
                elif '1.2' in tw:
                    desc = "組織範圍內之交通(移動)運輸設備之燃料燃燒所產生的溫室氣體排放，如車輛(柴油、汽油)、堆高機(柴油)等，交通設備中燃燒燃料。不在組織範圍內的車輛所產生的排放應為“間接排放”，包括商務出差、員工通勤、客戶或訪客交通、上游租賃資產等。"
                elif '1.3' in tw:
                    desc = "工業產品製造過程所釋放的溫室氣體排放，包含水泥製程、半導體/LCD/PV製程、電焊(焊條)、乙炔(金屬切割器)等。"
                elif '1.4' in tw:
                    desc = "人為系統所釋放的溫室氣體產生的直接逸散性排放，包含化糞池(CH₄)、滅火器(CO₂)、氣體斷路器(SF₆)、噴霧劑與冷媒等逸散(HFCs)（冰水主機、冷氣機、飲水機冰水冷媒、冰箱、車輛冷媒、冷凍冷藏設備、冷凍式乾燥機）等。"
                elif '1.5' in tw:
                    desc = "涵蓋由活生質體至土壤內有機物質之所有溫室氣體。"
                elif '2.1' in tw:
                    desc = "輸入能源產生之間接溫室氣體排放，如外購電力。"
                elif '2.2' in tw:
                    desc = "來自於熱、蒸氣或其他化石燃料衍生能源，間接產生之溫室氣體排放，如蒸氣、熱能、冷能和高壓空氣(CDA)。"
                elif '3.1' in tw:
                    desc = "組織購買之原物料運輸與配送產生的排放。供應商使用車輛或設施，運送原物料至組織產生的溫室氣體排放，交通運具非組織所有。"
                elif '3.2' in tw:
                    desc = "組織售出產品運輸至零售商或倉儲產生的排放。運輸、物流與零售業者運輸產品過程產生的溫室氣體排放，交通運具非組織所有。"
                elif '3.3' in tw:
                    desc = "組織員工從家裡至辦公場址的通勤排放。員工通勤使用交通運具，包含搭乘大眾交通、汽車、機車等工具，交通運具非組織所有。"
                elif '3.4' in tw:
                    desc = "客戶與訪客至組織辦公場所產生之運輸排放。客戶與訪客至組織辦公場所使用交通運具，包含大眾交通工具、汽車、機車等，交通過程所產生的溫室氣體排放。"
                elif '3.5' in tw:
                    desc = "組織員工的公務差旅運輸排放。員工公務差旅使用交通運具(汽車、機車等)、搭乘大眾交通工具過程產生的溫室氣體排放。"
                elif '4.1' in tw:
                    desc = "組織購買原料開採、製造與加工過程所產生溫室氣體排放。供應商之產品、燃料、能源或服務之碳足跡，或是供應商之類別一、類別二排放；或引用單位產品重量/距離/費用之平均排放。"
                elif '4.2' in tw:
                    desc = "組織購買資本物品(資本財) 製造與加工過程所產生溫室氣體排放。如設備、機械、建築物、交通。供應商之產品、燃料、能源或服務之碳足跡，或是供應商之類別一、類別二排放；或引用單位產品重量/距離/費用之平均排放。"
                elif '4.3' in tw:
                    desc = "組織營運衍生的廢棄物處理排放。廢棄物處理商在處理廢棄物過程產生的類別一、類別二排放；另可再加上廢棄物運輸產生的排放；或引用單位廢棄物之平均排放。"
                elif '4.4' in tw:
                    desc = "組織（承租者）租賃使用之溫室氣體排放。資產的排放（未列入類別一、類別二），由承租者報告。租賃資產於報告期間的類別一、類別二排放；另可再加上製造租賃資產的生命週期排放量。"
                elif '4.5' in tw:
                    desc = "組織使用服務如：顧問諮詢、清潔、維護、郵件投遞、銀行等造成之排放。"
                elif '5.1' in tw:
                    desc = "使用組織售出產品產生的溫室氣體排放。"
                elif '5.2' in tw:
                    desc = "組織（出租者）出租資產的排放（未列入類別一、類別二），由出租者報告。"
                elif '5.3' in tw:
                    desc = "組織售出產品的廢棄處理排放。廢棄物處理商在廢棄處理過程產生的類別一、類別二排放。"
                elif '5.4' in tw:
                    desc = "報告期間投資（股權、債務、融資）產生的排放（未列入類別一、類別二）。"
                elif '6.1' in tw:
                    desc = "其他類別（即類別一～五）中，無法報告的組織特定排放量（或移除量）。"

                cat = tw[:3]
                catName = tw[4:]
                on_boarding_info[category] = [cat, catName, desc, yesORno, rating1 if rating1 else "", rating2 if rating2 else "", rating3 if rating3 else "", rating4 if rating4 else "", rating5 if rating5 else "", total, disclosure_req]

        df.to_excel(writer, sheet_name="2.1-重大性準則", )
        worksheet = writer.sheets["2.1-重大性準則"]

        # 設定寬度
        process_list = [17.7890625, 25.7890625, 10.5234375, 8.68359375, 13.0, 10.5234375, 19.5234375, 35.1015625, 10.89453125, 12.5234375, 10.1015625, 12.89453125, 12.5234375, 8.68359375, 8.68359375, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]
        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定重大性評估因子及評估門檻的說明
        content = f"""重大性評估因子及評估門檻
· 重大間接溫室氣體排放源之評估門檻：{threshold}
· 法規或客戶要求即為該年度重大間接排放源，須進行量化
· 未來評估門檻得依營運情形滾動式修正，惟須於盤查報告書闡明修正緣由
· 非法規要求之間接排放源重大性評分方式：
S= C+R+P+F+V
S：總分數 (Significance)
C：數據可信度(Credibility)
R：排放因子來源 (Risk)
P：減量措施推行可行度(Practicability)
F：發生頻率 (Frequency)
V：排放量 (Volume)
                """
        # 插入到A2下方
        worksheet.insert_textbox('A2', content, {'width': 520, 'height': 300})

        # 設定標題
        process_list = ["評估因子", "評分項目", "評分"]
        
        # 插入到A21,B21,C21
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+1]}21", process_list[i], cf7)

        # 插入以下的評估因子到對應的單元格子
        worksheet.merge_range("A22:A25", "數據可信度", cf8)
        worksheet.merge_range("A26:A31", "排放來源的量化風險", cf8)
        worksheet.merge_range("A32:A36", "減量措施推行可行度", cf8)
        worksheet.merge_range("A37:A40", "發生頻率", cf8)
        worksheet.merge_range("A41:A44", "排放量", cf8)

        # 評分項目資料
        process_list = ["有第三方提供之佐證","有內部財務或物料系統報表","有內部已簽核之相關操作記錄","有內部已簽核之相關操作記錄","內部測量係數","設備製造商提供的係數","區域排放係數","國家排放因素","國際排放因素","沒有排放因素","1年內可進行減量措施","1~2年內可進行減量措施","3~5年內可進行減量措施","6年以上可進行減量措施","無法採取減量措施","至少每月一次","至少每季度一次","至少每年一次","不考慮此項","佔總排放量的3％以上","佔總排放量的0.5％至3％","佔總排放量的0.5％","不考慮此項"]

        # 插入評分項目資料到B21:B44
        for i in range(22, 45):
            worksheet.write(f"B{i}", process_list[i-22], cf9)

        # 評分 資料
        process_list = [5,3,1,0,5,4,3,2,1,0,5,4,3,2,1,5,3,1,0,5,3,1,0]
        # 插入評分資料到C22:C44
        for i in range(22, 45):
            worksheet.write(f"C{i}", process_list[i-22], cf8)

        # 設定該公司的各個類別報告邊界的信息
        process_list = ["類別", "排放源名稱", "說明", "法規或客戶\n要求", "數據可信度 (C)", "排放因子來源 (R)",	"減量措施\n推行可行度(P)", "發生頻率 (F)" ,"排放量 (V)", "總分數 (S)", "揭露要求"]

        # 插入到F1:P1
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+6]}1", process_list[i], cf10)

        # 將邊界設定的資料轉換成二維list
        process_list = []
        for item in on_boarding_info.values():
            process_list.append(item)
        # 各個column的cell format
        process_list_color = [cf8, cf8, cf13, cf8, cf8, cf8, cf8, cf8, cf8, cf8, cf8]
            
        # 將第一個list寫到F2:P2，第二個list寫到F3:P3，依此類推
        for i in range(len(process_list)):
            for j in range(len(process_list[i])):
                worksheet.write(f"{chr_ord[j+6]}{i+2}", process_list[i][j], process_list_color[j])

        logger.info("生成 3-定量盤查")
        # ------------------------- 3-定量盤查 -----------------------------
        df.to_excel(writer, sheet_name="3-定量盤查", )
        worksheet = writer.sheets["3-定量盤查"]

        # 設定高度
        process_list = [15, 15,] + [30 for _ in range(len(ef))]
        for i in range(len(process_list)):
            worksheet.set_row(i, process_list[i])

        # 設定寬度
        process_list = [14.68359375, 13.0, 20.20703125, 14.68359375, 13.0, 13.0, 21.68359375, 21.68359375, 14.68359375, 16.89453125, 14.68359375, 13.0, 13.0, 8.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]
        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i] + 7)
        
        # 設定定量盤查標題。方式：有一些標題是merge第一列和第二列（存成string），另一些是將第一列和第二列分開（存成list），所以以下的string即先merge第一列和第二列後寫入，list的話分別寫入第一列和第二列。
        process_list = ['編號', "廠區歸屬", "名稱", "排放源", "是否屬於\n生質能源", ["類別", "(1~6)"], ["類別一\n排放類型", "(E,P,T,F)"], "年活動數據", "單位"] + [None for i in range(18)] + ["單一排放源排放當量小計(CO2e公噸/年)", "單一排放源生質燃料CO2排放當量小計(CO2e公噸/年)", "單一排放源占排放總量比(%)"]

        for i, con in enumerate(process_list):
            if con:
                if type(con) == str:
                    worksheet.merge_range(f"{chr_ord[i+1]}1:{chr_ord[i+1]}2", con, cf10)
                else:
                    for j, c in enumerate(con):
                        worksheet.write(f"{chr_ord[i+1]}{j+1}", c, cf10)

        process_list = ["溫室氣體#1", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        # "溫室氣體#2": ["溫室氣體#2", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"], "溫室氣體#3": ["溫室氣體#3", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]}
        
        worksheet.merge_range("J1:O1", "溫室氣體#1", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+10]}2", process_list[i], cf10)

        process_list = ["溫室氣體#2", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        worksheet.merge_range("P1:U1", "溫室氣體#2", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+16]}2", process_list[i], cf10)

        process_list = ["溫室氣體#3", "排放係數", "係數單位", "排放量\n(公噸/年)", "GWP", "排放當量\n(公噸CO₂e/年)"]
        worksheet.merge_range("V1:AA1", "溫室氣體#3", cf10)
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+22]}2", process_list[i], cf10)

        # 資料寫入，大部分都是套公式
        for i in range(3, len(ef) + 3):
            ef_info = ef[i-2]
            category_type1 = ef_info['category1_type']# current_list[i-3][6]
            fuelType = ef_info['fuelType']
            scopeName = ef[i-2].get("scopeName")

            CO2_info = ef_info["CO2"]
            CH4_info = ef_info["CH4"]
            N2O_info = ef_info["N2O"]
            HFC_info = ef_info["HFC"]
            PFC_info = ef_info["PFC"]
            SF6_info = ef_info["SF6"]
            NF3_info = ef_info["NF3"]

            worksheet.write(f"A{i}", f"""=IF('2-定性盤查'!A{i+1}<>"",'2-定性盤查'!A{i+1},"")""", cf14)
            worksheet.write(f"B{i}", f"""=IF('2-定性盤查'!B{i+1}<>"",'2-定性盤查'!B{i+1},"")""", cf14)
            worksheet.write(f"C{i}", f"""=IF('2-定性盤查'!C{i+1}<>"",'2-定性盤查'!C{i+1},"")""", cf14)
            worksheet.write(f"D{i}", f"""=IF('2-定性盤查'!D{i+1}<>"",'2-定性盤查'!D{i+1},"")""", cf14)
            worksheet.write(f"E{i}", f"""=IF('2-定性盤查'!E{i+1}<>"",'2-定性盤查'!E{i+1},"")""", cf14)
            worksheet.write(f"F{i}", f"""=IF('2-定性盤查'!F{i+1}<>"",'2-定性盤查'!F{i+1},"")""", cf14)
            worksheet.write(f"G{i}", f"""=IF('2-定性盤查'!G{i+1}<>"",'2-定性盤查'!G{i+1},"")""", cf14)
            if scopeName:
                stream_index = ef[i-2]["stream_index"]
                if scopeName == "Upstream T&D":
                    worksheet.write(f"H{i}", f"""='3.2-上游運輸'!T{stream_index+2} """, cf8)
                else:
                    worksheet.write(f"H{i}", f"""='3.3-下游運輸'!T{stream_index+2} """, cf8)
                
                worksheet.write(f"I{i}", "總排放單位", cf8)
            else:
                worksheet.write(f"H{i}", f"{ef[i-2]['activityData']}", cf8)
                worksheet.write(f"I{i}", f"{ef[i-2]['unit']}", cf8)
            worksheet.write(f"J{i}", f"""=IF('2-定性盤查'!X{i+1}<>"",IF('2-定性盤查'!X{i+1}<>0,'2-定性盤查'!X{i+1},""),"")""", cf14)
            worksheet.write(f"K{i}", f"""='3.1-排放係數'!D{i}""", cf26)
            worksheet.write(f"L{i}", f"""='3.1-排放係數'!E{i}""", cf8)
            worksheet.write(f"M{i}", f"""=IF(J{i}="","",H{i}*K{i})""", cf27)
            if category_type1 == "F,逸散" and "化糞池" not in fuelType:
                worksheet.write(f"N{i}", HFC_info["hfc"], cf8)
                worksheet.write(f"Q{i}", "", cf26)
                worksheet.write(f"Z{i}", "", cf8)
            else:
                worksheet.write(f"N{i}", f"""=附表二、含氟氣體之GWP值!G3""", cf8)
                worksheet.write(f"Q{i}", f"""=IF('3.1-排放係數'!H{i}="", "", '3.1-排放係數'!H{i})""", cf26)
                worksheet.write(f"Z{i}", f"""=IF(Y{i}="", "", 附表二、含氟氣體之GWP值!G5)""", cf8)

            worksheet.write(f"O{i}", f"""=IF(M{i}="","",M{i}*N{i})""", cf27)
            worksheet.write(f"P{i}", f"""=IF('2-定性盤查'!Y{i+1}<>"",IF('2-定性盤查'!Y{i+1}<>0,'2-定性盤查'!Y{i+1},""),"")""", cf14)
            
            worksheet.write(f"R{i}", f"""=IF(Q{i}="","",'3.1-排放係數'!I{i})""", cf8)
            worksheet.write(f"S{i}", f"""=IF(P{i}="","",H{i}*Q{i})""", cf27)
            worksheet.write(f"T{i}", f"""=IF(S{i}="", "", 附表二、含氟氣體之GWP值!G4)""", cf8)
            worksheet.write(f"U{i}", f"""=IF(S{i}="","",S{i}*T{i})""", cf27)
            worksheet.write(f"V{i}", f"""=IF('2-定性盤查'!Z{i+1}<>"",IF('2-定性盤查'!Z{i+1}<>0,'2-定性盤查'!Z{i+1},""),"")""", cf14)
            worksheet.write(f"W{i}", f"""=IF('3.1-排放係數'!L{i} ="", "", '3.1-排放係數'!L{i})""", cf26)
            worksheet.write(f"X{i}", f"""=IF(W{i}="","",'3.1-排放係數'!M{i})""", cf8)
            worksheet.write(f"Y{i}", f"""=IF(V{i}="","",H{i}*W{i})""", cf27)
            
            worksheet.write(f"AA{i}", f"""=IF(Y{i}="","",Y{i}*Z{i})""", cf27)
            worksheet.write(f"AB{i}", f"""=IF('2-定性盤查'!E{i+1}="是",IF(J{i}="CO2",SUM(U{i},AA{i}),SUM(O{i},U{i},AA{i})),IF(SUM(O{i},U{i},AA{i})<>0,SUM(O{i},U{i},AA{i}),0))""", cf27)
            worksheet.write(f"AC{i}", f"""=IF('2-定性盤查'!E{i+1}="是",IF(J{i}="CO2",O{i},""),"")""", cf27)
            worksheet.write(f"AD{i}", f"""=IF(AB{i}<>"",AB{i}/'6-彙總表'!$J$5,"")""", cf29)
        
        worksheet.merge_range("AE8:AL8", "統計用變數", cf25)
        worksheet.write("AM8", "單一排放源排放當量絕對值小計(CO2e公噸/年)", cf25)

        for i in range(9, len(ef)+9):
            worksheet.write(f"AE{i}", f"=F{i-6}&J{i-6}&E{i-6}", cf25)
            worksheet.write(f"AF{i}", f"=F{i-6}&J{i-6}", cf25)
            worksheet.write(f"AG{i}", f"=F{i-6}&P{i-6}", cf25)
            worksheet.write(f"AH{i}", f"=F{i-6}&V{i-6}", cf25)
            worksheet.write(f"AI{i}", f"=F{i-6}&G{i-6}", cf25)
            worksheet.write(f"AJ{i}", f"=F{i-6}&G{i-6}", cf25)
            worksheet.write(f"AK{i}", f"=F{i-6}&G{i-6}", cf25)
            worksheet.write(f"AL{i}", f"=F{i-6}&J{i-6}&G{i-6}&E{i-6}", cf25)
            worksheet.write(f"AM{i}", f"""=IFERROR(ABS(AB{i-6}),"")""", cf25)

        worksheet.set_column("AE:AM", None, None, {"hidden": True})

        logger.info("生成 3.1-排放係數")
        # ------------------------- 3.1-排放係數 -----------------------------

        df.to_excel(writer, sheet_name="3.1-排放係數", )
        worksheet = writer.sheets["3.1-排放係數"]

        # 設定寬度
        process_list = [8.33203125, 23.77734375, 20.0, 17.33203125, 16.109375, 18.109375, 18.33203125, 34.109375, 13.33203125, 15.6640625, 16.33203125, 14.0, 14.5546875, 15.33203125, 17.0, 10.44140625, 10.44140625, 11.109375, 17.44140625, 9.6640625, 9.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定標題
        worksheet.merge_range("B1:B2", "排放源", cf6)
        worksheet.merge_range("C1:C2", "排放源", cf6)
        
        worksheet.merge_range("D1:G1", "CO2排放係數", cf6)
        worksheet.merge_range("H1:K1", "CH4排放係數", cf6)
        worksheet.merge_range("L1:O1", "N2O排放係數", cf6)
        worksheet.merge_range("P1:T1", "其他排放係數", cf6)

        process_list = ["數值", "單位", "來源分類", "來源說明", "數值", "單位", "係數種類", "來源說明", "數值", "單位", "係數種類", "來源說明", "排放源", "數值", "單位", "係數種類", "來源說明"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+4]}2", process_list[i], cf6)

        # 將ef資料寫入
        for i in range(3,numOfEF+3):
            ef_info = ef[i-2]
            category_type1 = ef_info['category1_type']
            fuelType = ef_info['fuelType']

            formula = f"""='2-定性盤查'!C{i+1}"""
            worksheet.write(f"B{i}", formula, cf6)

            formula = f"""='2-定性盤查'!D{i+1}"""
            worksheet.write(f"C{i}", formula, cf6)

            
            CO2_info = ef_info["CO2"]
            CH4_info = ef_info["CH4"]
            N2O_info = ef_info["N2O"]
            HFC_info = ef_info["HFC"]
            PFC_info = ef_info["PFC"]
            SF6_info = ef_info["SF6"]
            NF3_info = ef_info["NF3"]
            
            if category_type1 == "F,逸散" and "化糞池" not in fuelType:
                worksheet.write(f"D{i}", 1, cf26)
                worksheet.write(f"H{i}", "", cf26)
                worksheet.write(f"L{i}", "", cf26)

                worksheet.write(f"E{i}", CO2_info["Unit"], cf17)
                worksheet.write(f"F{i}", ef_info["activityTrustedDataType"], cf17)
                worksheet.write(f"G{i}", CO2_info["source"], cf17)
                
                worksheet.write(f"I{i}", CH4_info["Unit"], cf17)
                worksheet.write(f"J{i}", "", cf17)
                worksheet.write(f"K{i}", CH4_info["source"], cf17)
                
                worksheet.write(f"M{i}", N2O_info["Unit"], cf17)
                worksheet.write(f"N{i}", "", cf17)
                worksheet.write(f"O{i}", N2O_info["source"], cf17)
            elif category_type1 == "F,逸散" and "化糞池" in fuelType:
                worksheet.write(f"D{i}", CH4_info["ch4"], cf26)

                worksheet.write(f"E{i}", CH4_info["Unit"], cf17)
                worksheet.write(f"F{i}", ef_info["activityTrustedDataType"], cf17)
                worksheet.write(f"G{i}", CH4_info["source"], cf17)

                worksheet.write(f"H{i}", "", cf26)
                worksheet.write(f"I{i}", "", cf26)
                worksheet.write(f"J{i}", "", cf26)
                worksheet.write(f"K{i}", "", cf26)
                worksheet.write(f"L{i}", "", cf26)
                worksheet.write(f"M{i}", "", cf26)
                worksheet.write(f"N{i}", "", cf26)
                worksheet.write(f"O{i}", "", cf26)
            else:
                worksheet.write(f"D{i}", CO2_info["co2"], cf26)
                if CH4_info["ch4"]:
                    worksheet.write(f"H{i}", CH4_info["ch4"] if float(CH4_info["ch4"]) != 0 else "", cf26)
                else:
                    worksheet.write(f"H{i}", "", cf26)
                if N2O_info["n2o"]:
                    worksheet.write(f"L{i}", N2O_info["n2o"] if float(N2O_info["n2o"]) != 0 else "", cf26)
                else:
                    worksheet.write(f"L{i}", "", cf26)

                worksheet.write(f"E{i}", CO2_info["Unit"], cf17)
                worksheet.write(f"F{i}", ef_info["activityTrustedDataType"], cf17)
                worksheet.write(f"G{i}", CO2_info["source"], cf17)
                
                worksheet.write(f"I{i}", f"""=IF(H{i}="", "", "{CH4_info["Unit"]}")""", cf17)
                worksheet.write(f"J{i}", f"""=IF(H{i}="", "", "{ef_info["activityTrustedDataType"]}")""", cf17)
                worksheet.write(f"K{i}", f"""=IF(H{i}="", "", "{CH4_info["source"]}")""", cf17)
                
                worksheet.write(f"M{i}", f"""=IF(L{i}="", "", "{N2O_info["Unit"]}")""", cf17)
                worksheet.write(f"N{i}", f"""=IF(L{i}="", "", "{ef_info["activityTrustedDataType"]}")""", cf17)
                worksheet.write(f"O{i}", f"""=IF(L{i}="", "", "{N2O_info["source"]}")""", cf17)

            worksheet.write(f"P{i}", "", cf17)
            worksheet.write(f"R{i}", "", cf17)
            worksheet.write(f"S{i}", "", cf17)
            if HFC_info["hfc"]:
                worksheet.write(f"Q{i}", HFC_info["hfc"], cf28)
                worksheet.write(f"T{i}", CO2_info["source"])
            elif PFC_info["pfc"]:
                worksheet.write(f"Q{i}", PFC_info["pfc"], cf28)
                worksheet.write(f"T{i}", CO2_info["source"])
            elif SF6_info["sf6"]:
                worksheet.write(f"Q{i}", SF6_info["sf6"], cf28)
                worksheet.write(f"T{i}", CO2_info["source"])
            elif NF3_info["nf3"]:
                worksheet.write(f"Q{i}", NF3_info["nf3"], cf28)
                worksheet.write(f"T{i}", CO2_info["source"])
            else:
                worksheet.write(f"Q{i}", "", cf28)
                worksheet.write(f"T{i}", "", cf17)

        worksheet.protect('eco4ever')

        logger.info("生成 3.2-上游運輸")
        # ------------------------- 3.2-上游運輸 -----------------------------

        df.to_excel(writer, sheet_name="3.2-上游運輸", )
        worksheet = writer.sheets["3.2-上游運輸"]

        # 設定寬度
        process_list = [13.0, 19.88671875, 13.0, 15, 20, 18.6640625, 20, 15.77734375, 20, 8.44140625, 20, 15.0, 20, 13.0, 20, 13.0, 20, 13.0, 20, 20, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定標題
        process_list = ["序號", "廠區歸屬", "產品名稱"]

        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+1]}1:{chr_ord[i+1]}3", process_list[i], cf24)
        
        worksheet.merge_range("D1:K1", "國際海/空運", cf24)
        worksheet.merge_range("L1:S1", "國內陸運（港口/機場到國內工廠）", cf24)
        worksheet.merge_range("T1:T3", "總排放當量\n(公噸CO2e/年)", cf14)

        process_list = ["年活動數據", "單位", "溫室氣體", "排放係數", "係數單位", "排放量(公噸/年)", "GWP", "排放當量\n(公噸CO2e/年)"]
        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+4]}2:{chr_ord[i+4]}3", process_list[i], cf6)
            worksheet.merge_range(f"{chr_ord[i+12]}2:{chr_ord[i+12]}3", process_list[i], cf6)

        # 寫入資料
        for i, item in enumerate(upstream_info.values()):
            worksheet.write(f"A{i+4}", item["serialNumber"], cf6)
            worksheet.write(f"B{i+4}", item["companyName"], cf6)
            worksheet.write(f"C{i+4}", item["assetName"], cf6)
            worksheet.write(f"D{i+4}", item["activityData1"], cf17)
            worksheet.write(f"E{i+4}", item["unit1"], cf17)
            worksheet.write(f"F{i+4}", item["ghg1"], cf17)
            worksheet.write(f"G{i+4}", item["emissionFactor1"], cf26)
            worksheet.write(f"H{i+4}", item["emissionUnit1"], cf17)
            worksheet.write(f"I{i+4}", item["emission1"], cf28)
            worksheet.write(f"J{i+4}", item["GWP1"], cf17)
            worksheet.write(f"K{i+4}", item["totalemission1"], cf28)
            worksheet.write(f"L{i+4}", item["activityData2"], cf17)
            worksheet.write(f"M{i+4}", item["unit2"], cf17)
            worksheet.write(f"N{i+4}", item["ghg2"], cf17)
            worksheet.write(f"O{i+4}", item["emissionFactor2"], cf26)
            worksheet.write(f"P{i+4}", item["emissionUnit2"], cf17)
            worksheet.write(f"Q{i+4}", item["emission2"], cf28)
            worksheet.write(f"R{i+4}", item["GWP2"], cf17)
            worksheet.write(f"S{i+4}", item["totalemission2"], cf28)
            worksheet.write(f"T{i+4}", f"""=IF(K{i+4}="", 0, K{i+4})+IF(S{i+4}="", 0, S{i+4})""", cf27)
        if numOfUpstream > 0:
            for i in range(1, 21):
                if i == 1:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", numOfUpstream+1, cf14)
                elif i == 2:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", "總排放當量", cf14)
                elif i == 9:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", f"""=IF(SUM(I4:I{numOfUpstream+3})=0, "", SUM(I4:I{numOfUpstream+3}))""", cf27)
                elif i == 11:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", f"""=IF(SUM(K4:K{numOfUpstream+3})=0, "", SUM(K4:K{numOfUpstream+3}))""", cf27)
                elif i == 17:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", f"""=IF(SUM(Q4:Q{numOfUpstream+3})=0, "", SUM(Q4:Q{numOfUpstream+3}))""", cf27)
                elif i == 19:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", f"""=IF(SUM(S4:S{numOfUpstream+3})=0, "", SUM(S4:S{numOfUpstream+3}))""", cf27)
                elif i == 20:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", f"=SUM(T4:T{numOfUpstream+3})", cf27)
                else:
                    worksheet.write(f"{chr_ord[i]}{numOfUpstream+4}", "", cf14)

        logger.info("生成 3.3-下游運輸")
        # ------------------------- 3.3-下游運輸 -----------------------------
        df.to_excel(writer, sheet_name="3.3-下游運輸", )
        worksheet = writer.sheets["3.3-下游運輸"]

        # 設定寬度
        process_list = [13.0, 19.88671875, 13.0, 15, 20, 18.6640625, 20, 15.77734375, 20, 8.44140625, 20, 15.0, 20, 13.0, 20, 13.0, 20, 13.0, 20, 20, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定標題
        process_list = ["序號", "廠區歸屬", "產品名稱"]

        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+1]}1:{chr_ord[i+1]}3", process_list[i], cf24)
        
        worksheet.merge_range("D1:K1", "國際海/空運", cf24)
        worksheet.merge_range("L1:S1", "國內陸運（港口/機場到國內工廠）", cf24)
        worksheet.merge_range("T1:T3", "總排放當量\n(公噸CO2e/年)", cf14)

        process_list = ["年活動數據", "單位", "溫室氣體", "排放係數", "係數單位", "排放量(公噸/年)", "GWP", "排放當量\n(公噸CO2e/年)"]
        for i in range(len(process_list)):
            worksheet.merge_range(f"{chr_ord[i+4]}2:{chr_ord[i+4]}3", process_list[i], cf6)
            worksheet.merge_range(f"{chr_ord[i+12]}2:{chr_ord[i+12]}3", process_list[i], cf6)

        # 資料寫入
        for i, item in enumerate(downstream_info.values()):
            worksheet.write(f"A{i+4}", item["serialNumber"], cf6)
            worksheet.write(f"B{i+4}", item["companyName"], cf6)
            worksheet.write(f"C{i+4}", item["assetName"], cf6)
            worksheet.write(f"D{i+4}", item["activityData1"], cf17)
            worksheet.write(f"E{i+4}", item["unit1"], cf17)
            worksheet.write(f"F{i+4}", item["ghg1"], cf17)
            worksheet.write(f"G{i+4}", item["emissionFactor1"], cf26)
            worksheet.write(f"H{i+4}", item["emissionUnit1"], cf17)
            worksheet.write(f"I{i+4}", item["emission1"], cf28)
            worksheet.write(f"J{i+4}", item["GWP1"], cf17)
            worksheet.write(f"K{i+4}", item["totalemission1"], cf28)
            worksheet.write(f"L{i+4}", item["activityData2"], cf17)
            worksheet.write(f"M{i+4}", item["unit2"], cf17)
            worksheet.write(f"N{i+4}", item["ghg2"], cf17)
            worksheet.write(f"O{i+4}", item["emissionFactor2"], cf26)
            worksheet.write(f"P{i+4}", item["emissionUnit2"], cf17)
            worksheet.write(f"Q{i+4}", item["emission2"], cf28)
            worksheet.write(f"R{i+4}", item["GWP2"], cf17)
            worksheet.write(f"S{i+4}", item["totalemission2"], cf28)
            worksheet.write(f"T{i+4}", f"""=IF(K{i+4}="", 0, K{i+4})+IF(S{i+4}="", 0, S{i+4})""", cf27)

        if numOfDownstream > 0:
            for i in range(1, 21):
                if i == 1:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", numOfDownstream+1, cf14)
                elif i == 2:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", "總排放當量", cf14)
                elif i == 9:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", f"""=IF(SUM(I4:I{numOfDownstream+3})=0, "", SUM(I4:I{numOfDownstream+3}))""", cf27)
                elif i == 11:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", f"""=IF(SUM(K4:K{numOfDownstream+3})=0, "", SUM(K4:K{numOfDownstream+3}))""", cf27)
                elif i == 17:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", f"""=IF(SUM(Q4:Q{numOfDownstream+3})=0, "", SUM(Q4:Q{numOfDownstream+3}))""", cf27)
                elif i == 19:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", f"""=IF(SUM(S4:S{numOfDownstream+3})=0, "", SUM(S4:S{numOfDownstream+3}))""", cf27)
                elif i == 20:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", f"=SUM(T4:T{numOfDownstream+3})", cf27)
                else:
                    worksheet.write(f"{chr_ord[i]}{numOfDownstream+4}", "", cf14)

        logger.info("生成 4-數據品質管理")
        # ------------------------- 4-數據品質管理 -----------------------------

        df.to_excel(writer, sheet_name="4-數據品質管理", )
        worksheet = writer.sheets["4-數據品質管理"]

        # 設定寬度
        process_list = [4.68359375, 13.0, 23.1015625, 18.20703125, 18.68359375, 17.0, 23.41796875, 10.68359375, 25.5234375, 7.41796875, 13.1015625, 13.68359375, 7.89453125, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)): 
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])
        
        # 設定標題
        worksheet.merge_range("B2:B3", "編號", cf6)
        worksheet.merge_range("C2:C3", "製程名稱", cf6)
        worksheet.merge_range("D2:D3", "排放源", cf6)
        worksheet.merge_range("E2:H2", "原燃物料或產品", cf6)
        worksheet.merge_range("I2:J2", "排放係數", cf6)
        worksheet.merge_range("K2:N2", "原燃物料或產品", cf6)
        process_list = ["活動數據種類", "活動數據種類等級", "活動數據可信種類(儀器校正誤差等級)", "活動數據可信等級", "排放係數種類", "係數種類等級", "單一排放源數據誤差等級", "單一排放源占排放總量比(%)", "評分區間範圍", "排放量佔比加權平均"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+5]}3", process_list[i], cf6)

        # 資料寫入
        for i in range(4, numOfEF + 4):
            formula = f"""=IF('2-定性盤查'!A{i}<>"",'2-定性盤查'!A{i},"")"""
            worksheet.write(f"B{i}", formula, cf14)

            formula = f"""=IF('2-定性盤查'!C{i}<>"",'2-定性盤查'!C{i},"")"""
            worksheet.write(f"C{i}", formula, cf14)
            
            formula = f"""=IF('2-定性盤查'!D{i}<>"",'2-定性盤查'!D{i},"")"""
            worksheet.write(f"D{i}", formula, cf14)

            formula = f"""=IF(E{i}<>"",IF(E{i}="連續量測",1,IF(E{i}="定期(間歇)量測",2,IF(E{i}="財務會計推估",3,IF(E{i}="自行評估",3,"0")))),"")"""
            worksheet.write(f"F{i}", formula, cf14)

            formula = f"""=IF(G{i}<>"",IF(G{i}="(1)有進行外部校正或有多組數據茲佐證者",1,IF(G{i}="(2)有進行內部校正或經過會計簽證等証明者",2,IF(G{i}="(3)未進行儀器校正或未進行紀錄彙整者",3,"0"))),"")"""
            worksheet.write(f"H{i}", formula, cf14)

            formula = f"""=IF(I{i}="1自廠發展係數/質量平衡所得係數",1,IF(I{i}="2同製程/設備經驗係數",1,IF(I{i}="3製造廠提供係數",2,IF(I{i}="4區域排放係數",2,IF(I{i}="5國家排放係數",3,IF(I{i}="6國際排放係數",3,""))))))"""
            worksheet.write(f"J{i}", formula, cf14)

            formula = f"""=IF(OR(F{i}="", H{i}="", J{i}=""), "系統未選擇", F{i}*H{i}*J{i})"""
            worksheet.write(f"K{i}", formula, cf14)

            formula = f"""=IF('3-定量盤查'!AD{i-1}<>"",ROUND('3-定量盤查'!AD{i-1},4),"")"""
            worksheet.write(f"L{i}", formula, cf14)

            formula = f"""=IF(K{i}="系統未選擇","系統未選擇",IF(K{i}<10,"1",IF(19>K{i},"2",IF(K{i}>=27,"3","-"))))"""
            worksheet.write(f"M{i}", formula, cf14)

            formula = f"""=IF(K{i}="系統未選擇","系統未選擇",IF(L{i}="","",ROUND(K{i}*L{i},2)))"""
            worksheet.write(f"N{i}", formula, cf14)

            worksheet.write(f"E{i}", ef[i-3]["activityDataType"], cf17)
            worksheet.write(f"G{i}", ef[i-3]["activityTrustedDataType"], cf17)
            worksheet.write(f"I{i}", ef[i-3]["emissionFactorType"], cf17)

        logger.info("生成 5-不確定性之評估")
        # ------------------------- 5-不確定性之評估 -----------------------------
        df.to_excel(writer, sheet_name="5-不確定性之評估", )
        worksheet = writer.sheets["5-不確定性之評估"]

        # 設定寬度
        process_list = [9.0, 8.3125, 20.41796875, 13.1015625, 11.0, 10.41796875, 13.89453125, 10.7890625, 11.7890625, 9.41796875, 10.41796875, 14.0, 13.41796875, 13.68359375, 10.41796875, 11.1015625, 13.1015625, 13.0, 13.7890625, 14.5234375, 14.20703125, 9.0, 12.5234375, 14.5234375, 13.5234375, 13.68359375, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i])

        # 設定標題
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
        
        # 設定單元格子格式
        worksheet.conditional_format(f"B5:D{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format(f"H5:I{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format(f"M5:P{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format(f"T5:V{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format(f"AA5:AD{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf14})
        worksheet.conditional_format(f"E5:G{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf17})
        worksheet.conditional_format(f"J5:L{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf17})
        worksheet.conditional_format(f"Q5:S{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf17})
        worksheet.conditional_format(f"X5:Z{numOfEF+5}", {"type": "formula", "criteria": "TRUE", "format":cf17})
        
        # 資料寫入
        for i in range(1, numOfEF+1):
            worksheet.write(f"B{i+4}", i)
            worksheet.write(f"C{i+4}", ef[i]["assetName"])
            worksheet.write(f"D{i+4}", ef[i]["fuelType"])

            formula = f"""=IF('3-定量盤查'!J{i+2}<>"",'3-定量盤查'!J{i+2},"")"""
            worksheet.write_formula(f"H{i+4}", formula)

            formula = f"""=IF(E{i+4}<>"",IF(J{i+4}<>"",IF('3-定量盤查'!O{i+2}<>"",'3-定量盤查'!O{i+2},0),""),"")"""
            worksheet.write_formula(f"I{i+4}", formula)

            formula = f"""=ROUND(IF($E{i+4}="",IF(J{i+4}="",0,0),IF(I{i+4}="",0,($E{i+4}^2+J{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"M{i+4}", formula)

            formula = f"""=ROUND(IF($F{i+4}="",IF(K{i+4}="",0,0),IF(K{i+4}="",0,($F{i+4}^2+K{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"N{i+4}", formula)

            formula = f"""=IF('3-定量盤查'!P{i+2}<>"",'3-定量盤查'!P{i+2},"")"""
            worksheet.write_formula(f"O{i+4}", formula)

            formula = f"""=ROUND(IF($E{i+4}="",IF(Q{i+4}="",0,0),IF(Q{i+4}="",0,($E{i+4}^2+Q{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"T{i+4}", formula)

            formula = f"""=ROUND(IF($F{i+4}="",IF(R{i+4}="",0,0),IF(R{i+4}="",0,($F{i+4}^2+R{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"U{i+4}", formula)

            formula = f"""=IF('3-定量盤查'!V{i+2}<>"",'3-定量盤查'!V{i+2},"")"""
            worksheet.write_formula(f"V{i+4}", formula)

            formula = f"""=ROUND(IF($E{i+4}="",IF(X{i+4}="",0,0),IF(X{i+4}="",0,($E{i+4}^2+X{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"AA{i+4}", formula)

            formula = f"""=ROUND(IF($F{i+4}="",IF(Y{i+4}="",0,0),IF(Y{i+4}="",0,($F{i+4}^2+Y{i+4}^2)^0.5)),5)"""
            worksheet.write_formula(f"AB{i+4}", formula)

            formula = f"""=IF(SUM($I{i+4},$P{i+4}),IF($I{i+4}<>"",IF($P{i+4}<>"",IF($W{i+4}<>"",(($I{i+4}*M{i+4})^2+($P{i+4}*T{i+4})^2+($W{i+4}*AA{i+4})^2)^0.5/SUM($I{i+4},$P{i+4},$W{i+4}),(($I{i+4}*M{i+4})^2+($P{i+4}*T{i+4})^2)^0.5/SUM($I{i+4},$P{i+4})),M{i+4}),""),0)"""
            worksheet.write_formula(f"AC{i+4}", formula)

            formula = f"""=IF(SUM($I{i+4},$P{i+4}),IF($I{i+4}<>"",IF($P{i+4}<>"",IF($W{i+4}<>"",(($I{i+4}*N{i+4})^2+($P{i+4}*U{i+4})^2+($W{i+4}*AB{i+4})^2)^0.5/SUM($I{i+4},$P{i+4},$W{i+4}),(($I{i+4}*N{i+4})^2+($P{i+4}*U{i+4})^2)^0.5/SUM($I{i+4},$P{i+4})),N{i+4}),""),0)"""
            worksheet.write_formula(f"AD{i+4}", formula)

        worksheet.merge_range("AE2:AI2", "統計用計算數值", cf25)
        worksheet.write("AG3", "I欄絕對值", cf25)
        worksheet.write("AH3", "P欄絕對值", cf25)
        worksheet.write("AI3", "W欄絕對值", cf25)

        for i in range(5, numOfEF + 5):
            worksheet.write(f"AE{i}", f"""=IF(AC{i}<>"",(AC{i}*SUM($I{i},$P{i},$W{i}))^2,"")""", cf25)
            worksheet.write(f"AF{i}", f"""=IF(AD{i}<>"",(AD{i}*SUM($I{i},$P{i},$W{i}))^2,"")""", cf25)
            worksheet.write(f"AG{i}", f"""=IFERROR(ABS(I{i}),"")""", cf25)
            worksheet.write(f"AH{i}", f"""=IFERROR(ABS(P{i}),"")""", cf25)
            worksheet.write(f"AI{i}", f"""=IFERROR(ABS(W{i}),"")""", cf25)
        
        worksheet.set_column("AE:AI", None, None, {"hidden": True})

        logger.info("生成 6-彙總表")
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
        cf23 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#C0C0C0",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True,
            'num_format': '0.000'
        })
        cf26 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#C0C0C0",
            "border": 1,
            "valign": "vcenter",
            "align": "center",
            "text_wrap": True,
            'num_format': '0.000%'
        })

        process_list = [14.68359375, 13.0, 20.20703125, 14.68359375, 13.0, 13.0, 21.68359375, 21.68359375, 14.68359375, 16.89453125, 14.68359375, 13.0, 13.0, 8.7890625, 8.7890625, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0, 13.0]

        for i in range(len(process_list)):
            worksheet.set_column(f"{chr_ord[i+1]}:{chr_ord[i+1]}", process_list[i]+3)

        worksheet.merge_range("A3:K3", "彙整表一、全廠七大溫室氣體排放量統計表", cf8)
        worksheet.merge_range("A4:B4", "", cf8)

        process_list = ["CO₂", "CH₄", "N₂O", "HFC\u209b", "PFC\u209b", "SF₆", "NF₃", "七種溫室氣體年總排放當量", "生質排放當量"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}4", process_list[i], cf19)

        worksheet.merge_range("A5:B5", "排放當量\n(公噸CO2e/年)", cf17)
        worksheet.merge_range("A6:B6", "氣體別占比\n(％)", cf17)

        process_list = [f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=CO₂",'3-定量盤查'!$O$3:$O${numOfEF+3})-K5""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=CH₄",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$P$3:$P${numOfEF+3},"=CH₄",'3-定量盤查'!$U$3:$U${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=N₂O",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$P$3:$P${numOfEF+3},"=N₂O",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$V$3:$V${numOfEF+3},"=N₂O",'3-定量盤查'!$AA$3:$AA${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=HFC\u209b",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=PFC\u209b",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=SF₆",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$J$3:$J${numOfEF+3},"=NF₃",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=ROUND(SUM(C5:I5),3)""", f"""=SUM('3-定量盤查'!AC3:AC{numOfEF+3})"""]

        for i in range(len(process_list)): 
            worksheet.write(f"{chr_ord[i+3]}5", process_list[i], cf23)

        process_list = ["""=C5/$J$5""", """=D5/$J$5""", """=E5/$J$5""", """=F5/$J$5""", """=G5/$J$5""", """=H5/$J$5""", """=I5/$J$5""", """=J5/$J$5""", """-"""]
        
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}6", process_list[i], cf26)

        worksheet.merge_range("A9:J9", "彙整表二、類別一七大溫室氣體排放量統計表", cf8)
        worksheet.merge_range("A10:B10", "", cf8)

        process_list = ["CO₂", "CH₄", "N₂O", "HFC\u209b", "PFC\u209b", "SF₆", "NF₃", "類別一七種溫室氣體\n年總排放當量"]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}10", process_list[i], cf19)

        worksheet.merge_range("A11:B11", "排放當量\n(公噸CO2e/年)", cf17)
        worksheet.merge_range("A12:B12", "氣體別占比\n(％)", cf17)

        process_list = [f"""=SUMIF('3-定量盤查'!$AE$9:$AE${numOfEF+9},"=1CO₂否",'3-定量盤查'!$O$3:$O${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1CH₄",'3-定量盤查'!$O$3:$O${numOfEF+9})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1CH₄",'3-定量盤查'!$U$3:$U${numOfEF+9})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1CH₄",'3-定量盤查'!$AA$3:$AA${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1N₂O",'3-定量盤查'!$O$3:$O${numOfEF+9})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1N₂O",'3-定量盤查'!$U$3:$U${numOfEF+9})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1N₂O",'3-定量盤查'!$AA$3:$AA${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1HFC\u209b",'3-定量盤查'!$O$3:$O${numOfEF+9})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1HFC\u209b",'3-定量盤查'!$U$3:$U${numOfEF+9})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1HFC\u209b",'3-定量盤查'!$AA$3:$AA${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1PFC\u209b",'3-定量盤查'!$O$3:$O${numOfEF+9})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1PFC\u209b",'3-定量盤查'!$U$3:$U${numOfEF+9})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1PFC\u209b",'3-定量盤查'!$AA$3:$AA${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1SF₆",'3-定量盤查'!$O$3:$O${numOfEF+9})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1SF₆",'3-定量盤查'!$U$3:$U${numOfEF+9})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1SF₆",'3-定量盤查'!$AA$3:$AA${numOfEF+9})""", f"""=SUMIF('3-定量盤查'!$AF$9:$AF${numOfEF+9},"=1NF₃",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$AG$9:$AG${numOfEF+9},"=1NF₃",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$AH$9:$AH${numOfEF+9},"=1NF₃",'3-定量盤查'!$AA$3:$AA${numOfEF+3})""", """=SUM(C11:I11)"""]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}11", process_list[i], cf23)
        
        process_list = ["""=C11/$J$11""", """=D11/$J$11""", """=E11/$J$11""", """=F11/$J$11""", """=G11/$J$11""", """=H11/$J$11""", """=I11/$J$11""", """=J11/C17"""]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}12", process_list[i], cf26)


        worksheet.merge_range("A14:L14", "彙整表三、全廠溫室氣體類別及類別一排放型式排放量統計表", cf8)
        worksheet.merge_range("A15:B16", "", cf8)
        worksheet.merge_range("C15:F15", "類別一", cf8)

        process_list = ["類別2", "類別3", "類別4", "類別5", "類別6", "總排放當量"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+7]}15", process_list[i], cf8)

        process_list = ["固定排放", "製程排放", "移動排放", "逸散排放", "能源間接排放", "運輸之間接排放", "上游之間接排放", "下游之間接排放", "其他間接排放", ""]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}16", process_list[i], cf8)

        worksheet.merge_range("A17:B18", "排放當量\n(公噸CO2e/年)", cf17)
        worksheet.merge_range("A19:B20", "排放占比\n(％)", cf17)

        worksheet.merge_range("C17:F17", f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=1",'3-定量盤查'!$AB$3:$AB$28)""", cf23)
        worksheet.merge_range("C19:F19", """=C17/L17""", cf26)

        process_list = [f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=2",'3-定量盤查'!$AB$3:$AB${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=3",'3-定量盤查'!$AB$3:$AB${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=4",'3-定量盤查'!$AB$3:$AB${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=5",'3-定量盤查'!$AB$3:$AB${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$F$3:$F${numOfEF+3},"=6",'3-定量盤查'!$AB$3:$AB${numOfEF+3})""", """=J5"""]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+7]}17", process_list[i], cf23)

        process_list = [f"""=SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1E,固定",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$AJ$9:$AJ${numOfEF+9},"=1E,固定",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$AK$9:$AK${numOfEF+9},"=1E,固定",'3-定量盤查'!$AA$3:$AA${numOfEF+3})-SUMIF('3-定量盤查'!$AL$9:$AL${numOfEF+9},"=1CO2E,固定是",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1P,製程",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$AJ$9:$AJ${numOfEF+9},"=1P,製程",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$AK$9:$AK${numOfEF+9},"=1P,製程",'3-定量盤查'!$AA$3:$AA${numOfEF+3})-SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1CO2P,製程是",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1T,移動",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$AJ$9:$AJ${numOfEF+9},"=1T,移動",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$AK$9:$AK${numOfEF+9},"=1T,移動",'3-定量盤查'!$AA$3:$AA${numOfEF+3})-SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1CO2T,移動是",'3-定量盤查'!$O$3:$O${numOfEF+3})""", f"""=SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1F,逸散",'3-定量盤查'!$O$3:$O${numOfEF+3})+SUMIF('3-定量盤查'!$AJ$9:$AJ${numOfEF+9},"=1F,逸散",'3-定量盤查'!$U$3:$U${numOfEF+3})+SUMIF('3-定量盤查'!$AK$9:$AK${numOfEF+9},"=1F,逸散",'3-定量盤查'!$AA$3:$AA${numOfEF+3})-SUMIF('3-定量盤查'!$AI$9:$AI${numOfEF+9},"=1CO2F,逸散是",'3-定量盤查'!$O$3:$O${numOfEF+3})"""] + ["" for i in range(6)]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}18", process_list[i], cf23)


        process_list = ["=G17/$L$17", "=H17/$L$17", "=I17/$L$17", "=J17/$L$17", "=K17/$L$17", "=L17/$L$17"]
        
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+7]}19", process_list[i], cf26)

        process_list = ["=C18/$L$17", "=D18/$L$17", "=E18/$L$17", "=F18/$L$17"] + ["" for i in range(6)]

        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+3]}20", process_list[i], cf26)


        worksheet.merge_range("B22:E22", "彙整表四、全廠溫室氣體數據等級評分結果", cf8)
        
        process_list = ["等級", "第一級", "第二級", "第三級"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+2]}23", process_list[i], cf8)

        process_list = ["評分範圍", "X<10分", "10分≦X<19分", "19≦X≦27分"]
        for i in range(len(process_list)):
            worksheet.write(f"{chr_ord[i+2]}24", process_list[i], cf8)

        worksheet.write("B25", "個數", cf17)
        worksheet.write("B26", "清冊等級總平均\n分數", cf17)
        worksheet.write("D26", "清冊登記", cf17)
        worksheet.write("C25", f"""=COUNTIF('4-數據品質管理'!M4:M{numOfEF+4},"=1")""", cf23)
        worksheet.write("D25", f"""=COUNTIF('4-數據品質管理'!M4:M{numOfEF+4},"=2")""", cf23)
        worksheet.write("E25", f"""=COUNTIF('4-數據品質管理'!M4:M{numOfEF+4},"=3")""", cf23)
        worksheet.write("C26", f"""=SUM('4-數據品質管理'!N4:N{numOfEF+4})""", cf23)
        worksheet.write("E26", f"""=IF(C26<10,"第一級",IF(C26<19,"第二級",IF(C26<=27,"第三級","-")))""", cf23)

        worksheet.merge_range("G22:L22", "彙整表五、溫室氣體不確定性量化評估結果", cf8)
        worksheet.write("G23", "進行不確定性評估之排放量絕對值加總", cf19)
        worksheet.write("H23", "排放總量絕對值加總", cf19)
        worksheet.merge_range("I23:L24", "本清冊之總不確定性", cf19)
        worksheet.merge_range("G25:H25", "進行不確定性評估之排放量佔總排放量之比例", cf19)
        worksheet.merge_range("I25:J25", "95%信賴區間下限", cf19)
        worksheet.merge_range("K25:L25", "95%信賴區間上限", cf19)

        worksheet.write("G24", f"""=SUM('5-不確定性之評估'!AG5:AG{len(ef)+5})+SUM('5-不確定性之評估'!AH5:AH{len(ef)+5})+SUM('5-不確定性之評估'!AI5:AI{len(ef)+5})""", cf23)
        worksheet.write("H24", f"""=SUM('3-定量盤查'!AM9:AM{len(ef)+9})""", cf23)
        worksheet.merge_range("G26:H26", """=G24/H24""", cf23)
        worksheet.merge_range("I26:J26", f"""=(SUM('5-不確定性之評估'!AE5:AE{numOfEF+5}))^0.5/SUM('5-不確定性之評估'!AG5:AG{numOfEF+5},'5-不確定性之評估'!AH5:AH{numOfEF+5},'5-不確定性之評估'!AI5:AI{numOfEF+5})""", cf23)
        worksheet.merge_range("K26:L26", f"""=(SUM('5-不確定性之評估'!AF5:AF{numOfEF+5}))^0.5/SUM('5-不確定性之評估'!AG5:AG{numOfEF+5},'5-不確定性之評估'!AH5:AH{numOfEF+5},'5-不確定性之評估'!AI5:AI{numOfEF+5})""", cf23)

        logger.info("生成 附表一")
        # ------------------------- 附表一 -----------------------------
        df.to_excel(writer, sheet_name="附表一、行業代碼", )
        worksheet = writer.sheets["附表一、行業代碼"]

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

        process_list = [[1, '0111', '稻作栽培業', '1. 0111 稻作栽培業'], [2, '0112', '雜糧栽培業', '2. 0112 雜糧栽培業'], [3, '0113', '特用作物栽培業', '3. 0113 特用作物栽培業'], [4, '0114', '蔬菜栽培業', '4. 0114 蔬菜栽培業'], [5, '0115', '果樹栽培業', '5. 0115 果樹栽培業'], [6, '0116', '食用菌菇類栽培業', '6. 0116 食用菌菇類栽培業'], [7, '0117', '花卉栽培業', '7. 0117 花卉栽培業'], [8, '0119', '其他農作物栽培業', '8. 0119 其他農作物栽培業'], [9, '0121', '牛飼育業', '9. 0121 牛飼育業'], [10, '0122', '豬飼育業', '10. 0122 豬飼育業'], [11, '0123', '雞飼育業', '11. 0123 雞飼育業'], [12, '0124', '鴨飼育業', '12. 0124 鴨飼育業'], [13, '0129', '其他畜牧業', '13. 0129 其他畜牧業'], [14, '0131', '作物栽培服務業', '14. 0131 作物栽培服務業'], [15, '0132', '農產品整理業', '15. 0132 農產品整理業'], [16, '0133', '畜牧服務業', '16. 0133 畜牧服務業'], [17, '0139', '其他農事服務業', '17. 0139 其他農事服務業'], [18, '0210', '造林業', '18. 0210 造林業'], [19, '0221', '伐木業', '19. 0221 伐木業'], [20, '0222', '野生物採捕業', '20. 0222 野生物採捕業'], [21, '0311', '海洋漁業', '21. 0311 海洋漁業'], [22, '0312', '內陸漁撈業', '22. 0312 內陸漁撈業'], [23, '0321', '海面養殖業', '23. 0321 海面養殖業'], [24, '0322', '內陸養殖業', '24. 0322 內陸養殖業'], [25, '0500', '石油及天然氣礦業', '25. 0500 石油及天然氣礦業'], [26, '0600', '砂、石及黏土採取業', '26. 0600 砂、石及黏土採取業'], [27, '0700', '其他礦業及土石採取業', '27. 0700 其他礦業及土石採取業'], [28, '0811', '屠宰業', '28. 0811 屠宰業'], [29, '0812', '冷凍冷藏肉類製造業', '29. 0812 冷凍冷藏肉類製造業'], [30, '0813', '肉品製造業', '30. 0813 肉品製造業'], [31, '0821', '冷凍冷藏水產製造業', '31. 0821 冷凍冷藏水產製造業'], [32, '0822', '水產品製造業', '32. 0822 水產品製造業'], [33, '0831', '冷凍冷藏蔬果製造業', '33. 0831 冷凍冷藏蔬果製造業'], [34, '0832', '蔬果製品製造業', '34. 0832 蔬果製品製造業'], [35, '0840', '食用油脂製造業', '35. 0840 食用油脂製造業'], [36, '0850', '乳品製造業', '36. 0850 乳品製造業'], [37, '0861', '碾榖業', '37. 0861 碾榖業'], [38, '0862', '磨粉製品製造業', '38. 0862 磨粉製品製造業'], [39, '0863', '澱粉及其製品製造業', '39. 0863 澱粉及其製品製造業'], [40, '0870', '動物飼料配製業', '40. 0870 動物飼料配製業'], [41, '0891', '烘焙炊蒸食品製造業', '41. 0891 烘焙炊蒸食品製造業'], [42, '0892', '麵條、粉條類食品製造業', '42. 0892 麵條、粉條類食品製造業'], [43, '0893', '製糖業', '43. 0893 製糖業'], [44, '0894', '糖果製造業', '44. 0894 糖果製造業'], [45, '0895', '製茶業', '45. 0895 製茶業'], [46, '0896', '調味品製造業', '46. 0896 調味品製造業'], [47, '0897', '調理食品製造業', '47. 0897 調理食品製造業'], [48, '0899', '未分類其他食品製造業', '48. 0899 未分類其他食品製造業'], [49, '0911', '啤酒製造業', '49. 0911 啤酒製造業'], [50, '0919', '其他酒精飲料製造業', '50. 0919 其他酒精飲料製造業'], [51, '0920', '非酒精飲料製造業', '51. 0920 非酒精飲料製造業'], [52, 1000, '菸草製造業', '52. 1000 菸草製造業'], [53, 1111, '棉紡紗業', '53. 1111 棉紡紗業'], [54, 1112, '毛紡紗業', '54. 1112 毛紡紗業'], [55, 1113, '人造纖維紡紗業', '55. 1113 人造纖維紡紗業'], [56, 1114, '人造纖維加工絲業', '56. 1114 人造纖維加工絲業'], [57, 1119, '其他紡紗業', '57. 1119 其他紡紗業'], [58, 1121, '棉梭織布業', '58. 1121 棉梭織布業'], [59, 1122, '毛梭織布業', '59. 1122 毛梭織布業'], [60, 1123, '人造纖維梭織布業', '60. 1123 人造纖維梭織布業'], [61, 1124, '玻璃纖維梭織布業', '61. 1124 玻璃纖維梭織布業'], [62, 1125, '針織布業', '62. 1125 針織布業'], [63, 1129, '其他織布業', '63. 1129 其他織布業'], [64, 1130, '不織布業', '64. 1130 不織布業'], [65, 1140, '印染整理業', '65. 1140 印染整理業'], [66, 1151, '紡織製成品製造業', '66. 1151 紡織製成品製造業'], [67, 1152, '繩、纜、網製造業', '67. 1152 繩、纜、網製造業'], [68, 1159, '其他紡織品製造業', '68. 1159 其他紡織品製造業'], [69, 1211, '梭織外衣製造業', '69. 1211 梭織外衣製造業'], [70, 1212, '梭織內衣及睡衣製造業', '70. 1212 梭織內衣及睡衣製造業'], [71, 1221, '針織外衣製造業', '71. 1221 針織外衣製造業'], [72, 1222, '針織內衣及睡衣製造業', '72. 1222 針織內衣及睡衣製造業'], [73, 1231, '襪類製造業', '73. 1231 襪類製造業'], [74, 1232, '紡織手套製造業', '74. 1232 紡織手套製造業'], [75, 1233, '紡織帽製造業', '75. 1233 紡織帽製造業'], [76, 1239, '其他服飾品製造業', '76. 1239 其他服飾品製造業'], [77, 1301, '皮革、毛皮整製業', '77. 1301 皮革、毛皮整製業'], [78, 1302, '鞋類製造業', '78. 1302 鞋類製造業'], [79, 1303, '行李箱及手提袋製造業', '79. 1303 行李箱及手提袋製造業'], [80, 1309, '其他皮革、毛皮製品製造業', '80. 1309 其他皮革、毛皮製品製造業'], [81, 1401, '製材業', '81. 1401 製材業'], [82, 1402, '合板及組合木材製造業', '82. 1402 合板及組合木材製造業'], [83, 1403, '建築用木製品製造業', '83. 1403 建築用木製品製造業'], [84, 1404, '木質容器製造業', '84. 1404 木質容器製造業'], [85, 1409, '其他木竹製品製造業', '85. 1409 其他木竹製品製造業'], [86, 1511, '紙漿製造業', '86. 1511 紙漿製造業'], [87, 1512, '紙張製造業', '87. 1512 紙張製造業'], [88, 1513, '紙板製造業', '88. 1513 紙板製造業'], [89, 1520, '紙容器製造業', '89. 1520 紙容器製造業'], [90, 1591, '家庭及衛生用紙製造業', '90. 1591 家庭及衛生用紙製造業'], [91, 1599, '未分類其他紙製品製造業', '91. 1599 未分類其他紙製品製造業'], [92, 1611, '印刷業', '92. 1611 印刷業'], [93, 1612, '印刷輔助業', '93. 1612 印刷輔助業'], [94, 1620, '資料儲存媒體複製業', '94. 1620 資料儲存媒體複製業'], [95, 1700, '石油及煤製品製造業', '95. 1700 石油及煤製品製造業'], [96, 1810, '基本化學材料製造業', '96. 1810 基本化學材料製造業'], [97, 1820, '石油化工原料製造業', '97. 1820 石油化工原料製造業'], [98, 1830, '肥料製造業', '98. 1830 肥料製造業'], [99, 1841, '合成樹脂及塑膠製造業', '99. 1841 合成樹脂及塑膠製造業'], [100, 1842, '合成橡膠製造業', '100. 1842 合成橡膠製造業'], [101, 1850, '人造纖維製造業', '101. 1850 人造纖維製造業'], [102, 1910, '農藥及環境衛生用藥製造業', '102. 1910 農藥及環境衛生用藥製造業'], [103, 1920, '塗料、染料及顏料製造業', '103. 1920 塗料、染料及顏料製造業'], [104, 1930, '清潔用品製造業', '104. 1930 清潔用品製造業'], [105, 1940, '化粧品製造業', '105. 1940 化粧品製造業'], [106, 1990, '其他化學製品製造業', '106. 1990 其他化學製品製造業'], [107, 2001, '原料藥製造業', '107. 2001 原料藥製造業'], [108, 2002, '西藥製造業', '108. 2002 西藥製造業'], [109, 2003, '生物藥品製造業', '109. 2003 生物藥品製造業'], [110, 2004, '中藥製造業', '110. 2004 中藥製造業'], [111, 2005, '體外檢驗試劑製造業', '111. 2005 體外檢驗試劑製造業'], [112, 2101, '輪胎製造業', '112. 2101 輪胎製造業'], [113, 2102, '工業用橡膠製品製造業', '113. 2102 工業用橡膠製品製造業'], [114, 2109, '其他橡膠製品製造業', '114. 2109 其他橡膠製品製造業'], [115, 2201, '塑膠皮、板、管材製造業', '115. 2201 塑膠皮、板、管材製造業'], [116, 2202, '塑膠膜袋製造業', '116. 2202 塑膠膜袋製造業'], [117, 2203, '塑膠日用品製造業', '117. 2203 塑膠日用品製造業'], [118, 2204, '工業用塑膠製品製造業', '118. 2204 工業用塑膠製品製造業'], [119, 2209, '其他塑膠製品製造業', '119. 2209 其他塑膠製品製造業'], [120, 2311, '平板玻璃及其製品製造業', '120. 2311 平板玻璃及其製品製造業'], [121, 2312, '玻璃容器製造業', '121. 2312 玻璃容器製造業'], [122, 2313, '玻璃纖維製造業', '122. 2313 玻璃纖維製造業'], [123, 2319, '其他玻璃及其製品製造業', '123. 2319 其他玻璃及其製品製造業'], [124, 2321, '耐火材料製造業', '124. 2321 耐火材料製造業'], [125, 2322, '黏土建築材料製造業', '125. 2322 黏土建築材料製造業'], [126, 2323, '陶瓷衛浴設備製造業', '126. 2323 陶瓷衛浴設備製造業'], [127, 2329, '其他陶瓷製品製造業', '127. 2329 其他陶瓷製品製造業'], [128, 2331, '水泥製造業', '128. 2331 水泥製造業'], [129, 2332, '預拌混凝土製造業', '129. 2332 預拌混凝土製造業'], [130, 2333, '水泥製品製造業', '130. 2333 水泥製品製造業'], [131, 2340, '石材製品製造業', '131. 2340 石材製品製造業'], [132, 2391, '工業及研磨材料製造業', '132. 2391 工業及研磨材料製造業'], [133, 2392, '石灰製造業', '133. 2392 石灰製造業'], [134, 2393, '石膏製品製造業', '134. 2393 石膏製品製造業'], [135, 2399, '未分類其他非金屬礦物製品製造業', '135. 2399 未分類其他非金屬礦物製品製造業'], [136, 2411, '鋼鐵冶鍊業', '136. 2411 鋼鐵冶鍊業'], [137, 2412, '鋼鐵鑄造業', '137. 2412 鋼鐵鑄造業'], [138, 2413, '鋼鐵軋延及擠型業', '138. 2413 鋼鐵軋延及擠型業'], [139, 2414, '鋼鐵伸線業', '139. 2414 鋼鐵伸線業'], [140, 2421, '鍊鋁業', '140. 2421 鍊鋁業'], [141, 2422, '鋁鑄造業', '141. 2422 鋁鑄造業'], [142, 2423, '鋁材軋延、擠型、伸線業', '142. 2423 鋁材軋延、擠型、伸線業'], [143, 2431, '鍊銅業', '143. 2431 鍊銅業'], [144, 2432, '銅鑄造業', '144. 2432 銅鑄造業'], [145, 2433, '銅材軋延、擠型、伸線業', '145. 2433 銅材軋延、擠型、伸線業'], [146, 2491, '其他基本金屬鑄造業', '146. 2491 其他基本金屬鑄造業'], [147, 2499, '未分類其他基本金屬製造業', '147. 2499 未分類其他基本金屬製造業'], [148, 2511, '金屬手工具製造業', '148. 2511 金屬手工具製造業'], [149, 2512, '金屬模具製造業', '149. 2512 金屬模具製造業'], [150, 2521, '金屬結構製造業', '150. 2521 金屬結構製造業'], [151, 2522, '金屬建築組件製造業', '151. 2522 金屬建築組件製造業'], [152, 2531, '鍋爐、金屬貯槽及壓力容器製造業', '152. 2531 鍋爐、金屬貯槽及壓力容器製造業'], [153, 2539, '其他金屬容器製造業', '153. 2539 其他金屬容器製造業'], [154, 2541, '金屬鍛造業', '154. 2541 金屬鍛造業'], [155, 2542, '粉末冶金業', '155. 2542 粉末冶金業'], [156, 2543, '金屬熱處理業', '156. 2543 金屬熱處理業'], [157, 2544, '金屬表面處理業', '157. 2544 金屬表面處理業'], [158, 2549, '其他金屬加工處理業', '158. 2549 其他金屬加工處理業'], [159, 2591, '螺絲、螺帽及鉚釘製造業', '159. 2591 螺絲、螺帽及鉚釘製造業'], [160, 2592, '金屬彈簧製造業', '160. 2592 金屬彈簧製造業'], [161, 2593, '金屬線製品製造業', '161. 2593 金屬線製品製造業'], [162, 2599, '未分類其他金屬製品製造業', '162. 2599 未分類其他金屬製品製造業'], [163, 2611, '積體電路製造業', '163. 2611 積體電路製造業'], [164, 2612, '分離式元件製造業', '164. 2612 分離式元件製造業'], [165, 2613, '半導體封裝及測試業', '165. 2613 半導體封裝及測試業'], [166, 2620, '被動電子元件製造業', '166. 2620 被動電子元件製造業'], [167, 2630, '印刷電路板製造業', '167. 2630 印刷電路板製造業'], [168, 2641, '液晶面板及其組件製造業', '168. 2641 液晶面板及其組件製造業'], [169, 2649, '其他光電材料及元件製造業', '169. 2649 其他光電材料及元件製造業'], [170, 2691, '印刷電路板組件製造業', '170. 2691 印刷電路板組件製造業'], [171, 2692, '電子管製造業', '171. 2692 電子管製造業'], [172, 2699, '未分類其他電子零組件製造業', '172. 2699 未分類其他電子零組件製造業'], [173, 2711, '電腦製造業', '173. 2711 電腦製造業'], [174, 2712, '顯示器及終端機製造業', '174. 2712 顯示器及終端機製造業'], [175, 2719, '其他電腦週邊設備製造業', '175. 2719 其他電腦週邊設備製造業'], [176, 2721, '電話及手機製造業', '176. 2721 電話及手機製造業'], [177, 2729, '其他通訊傳播設備製造業', '177. 2729 其他通訊傳播設備製造業'], [178, 2730, '視聽電子產品製造業', '178. 2730 視聽電子產品製造業'], [179, 2740, '資料儲存媒體製造業', '179. 2740 資料儲存媒體製造業'], [180, 2751, '量測、導航及控制設備製造業', '180. 2751 量測、導航及控制設備製造業'], [181, 2752, '鐘錶製造業', '181. 2752 鐘錶製造業'], [182, 2760, '輻射及電子醫學設備製造業', '182. 2760 輻射及電子醫學設備製造業'], [183, 2771, '照相機製造業', '183. 2771 照相機製造業'], [184, 2779, '其他光學儀器及設備製造業', '184. 2779 其他光學儀器及設備製造業'], [185, 2810, '發電、輸電、配電機械製造業', '185. 2810 發電、輸電、配電機械製造業'], [186, 2820, '電池製造業', '186. 2820 電池製造業'], [187, 2831, '電線及電纜製造業', '187. 2831 電線及電纜製造業'], [188, 2832, '配線器材製造業', '188. 2832 配線器材製造業'], [189, 2841, '電燈泡及燈管製造業', '189. 2841 電燈泡及燈管製造業'], [190, 2842, '照明器具製造業', '190. 2842 照明器具製造業'], [191, 2851, '家用空調器具製造業', '191. 2851 家用空調器具製造業'], [192, 2852, '家用電冰箱製造業', '192. 2852 家用電冰箱製造業'], [193, 2853, '家用洗衣設備製造業', '193. 2853 家用洗衣設備製造業'], [194, 2854, '家用電扇製造業', '194. 2854 家用電扇製造業'], [195, 2859, '其他家用電器製造業', '195. 2859 其他家用電器製造業'], [196, 2890, '其他電力設備製造業', '196. 2890 其他電力設備製造業'], [197, 2911, '冶金機械製造業', '197. 2911 冶金機械製造業'], [198, 2912, '金屬切削工具機製造業', '198. 2912 金屬切削工具機製造業'], [199, 2919, '其他金屬加工用機械設備製造業', '199. 2919 其他金屬加工用機械設備製造業'], [200, 2921, '農用及林用機械設備製造業', '200. 2921 農用及林用機械設備製造業'], [201, 2922, '採礦及營造用機械設備製造業', '201. 2922 採礦及營造用機械設備製造業'], [202, 2923, '食品、飲料及菸草製作用機械設備製造業', '202. 2923 食品、飲料及菸草製作用機械設備製造業'], [203, 2924, '紡織、成衣及皮革生產用機械設備製造業', '203. 2924 紡織、成衣及皮革生產用機械設備製造業'], [204, 2925, '木工機械設備製造業', '204. 2925 木工機械設備製造業'], [205, 2926, '化工機械設備製造業', '205. 2926 化工機械設備製造業'], [206, 2927, '橡膠及塑膠加工用機械設備製造業', '206. 2927 橡膠及塑膠加工用機械設備製造業'], [207, 2928, '電子及半導體生產用機械設備製造業', '207. 2928 電子及半導體生產用機械設備製造業'], [208, 2929, '未分類其他專用機械設備製造業', '208. 2929 未分類其他專用機械設備製造業'], [209, 2931, '原動機製造業', '209. 2931 原動機製造業'], [210, 2932, '流體傳動設備製造業', '210. 2932 流體傳動設備製造業'], [211, 2933, '泵、壓縮機、活栓及活閥製造業', '211. 2933 泵、壓縮機、活栓及活閥製造業'], [212, 2934, '機械傳動設備製造業', '212. 2934 機械傳動設備製造業'], [213, 2935, '輸送機械設備製造業', '213. 2935 輸送機械設備製造業'], [214, 2936, '事務機械設備製造業', '214. 2936 事務機械設備製造業'], [215, 2937, '污染防治設備製造業', '215. 2937 污染防治設備製造業'], [216, 2938, '動力手工具製造業', '216. 2938 動力手工具製造業'], [217, 2939, '其他通用機械設備製造業', '217. 2939 其他通用機械設備製造業'], [218, 3010, '汽車製造業', '218. 3010 汽車製造業'], [219, 3020, '車體製造業', '219. 3020 車體製造業'], [220, 3030, '汽車零件製造業', '220. 3030 汽車零件製造業'], [221, 3110, '船舶及其零件製造業', '221. 3110 船舶及其零件製造業'], [222, 3121, '機車製造業', '222. 3121 機車製造業'], [223, 3122, '機車零件製造業', '223. 3122 機車零件製造業'], [224, 3131, '自行車製造業', '224. 3131 自行車製造業'], [225, 3132, '自行車零件製造業', '225. 3132 自行車零件製造業'], [226, 3190, '未分類其他運輸工具及零件製造業', '226. 3190 未分類其他運輸工具及零件製造業'], [227, 3211, '木製家具製造業', '227. 3211 木製家具製造業'], [228, 3219, '其他非金屬家具製造業', '228. 3219 其他非金屬家具製造業'], [229, 3220, '金屬家具製造業', '229. 3220 金屬家具製造業'], [230, 3311, '體育用品製造業', '230. 3311 體育用品製造業'], [231, 3312, '玩具製造業', '231. 3312 玩具製造業'], [232, 3313, '樂器製造業', '232. 3313 樂器製造業'], [233, 3314, '文具製造業', '233. 3314 文具製造業'], [234, 3321, '眼鏡製造業', '234. 3321 眼鏡製造業'], [235, 3329, '其他醫療器材及用品製造業', '235. 3329 其他醫療器材及用品製造業'], [236, 3391, '珠寶及金工製品製造業', '236. 3391 珠寶及金工製品製造業'], [237, 3392, '拉鍊及鈕扣製造業', '237. 3392 拉鍊及鈕扣製造業'], [238, 3399, '其他未分類製造業', '238. 3399 其他未分類製造業'], [239, 3400, '產業用機械設備維修及安裝業', '239. 3400 產業用機械設備維修及安裝業'], [240, 3510, '電力供應業', '240. 3510 電力供應業'], [241, 3520, '氣體燃料供應業', '241. 3520 氣體燃料供應業'], [242, 3530, '蒸汽供應業', '242. 3530 蒸汽供應業'], [243, 3600, '用水供應業', '243. 3600 用水供應業'], [244, 3700, '廢（污）水處理業', '244. 3700 廢（污）水處理業'], [245, 3811, '無害廢棄物清除業', '245. 3811 無害廢棄物清除業'], [246, 3812, '有害廢棄物清除業', '246. 3812 有害廢棄物清除業'], [247, 3821, '無害廢棄物處理業', '247. 3821 無害廢棄物處理業'], [248, 3822, '有害廢棄物處理業', '248. 3822 有害廢棄物處理業'], [249, 3830, '資源回收業', '249. 3830 資源回收業'], [250, 3900, '污染整治業', '250. 3900 污染整治業'], [251, 4100, '建築工程業', '251. 4100 建築工程業'], [252, 4210, '道路工程業', '252. 4210 道路工程業'], [253, 4220, '公用事業設施工程業', '253. 4220 公用事業設施工程業'], [254, 4290, '其他土木工程業', '254. 4290 其他土木工程業'], [255, 4310, '整地、基礎及結構工程業', '255. 4310 整地、基礎及結構工程業'], [256, 4320, '庭園景觀工程業', '256. 4320 庭園景觀工程業'], [257, 4331, '機電、電信及電路設備安裝業', '257. 4331 機電、電信及電路設備安裝業'], [258, 4332, '冷凍、空調及管道工程業', '258. 4332 冷凍、空調及管道工程業'], [259, 4339, '其他建築設備安裝業', '259. 4339 其他建築設備安裝業'], [260, 4340, '最後修整工程業', '260. 4340 最後修整工程業'], [261, 4390, '其他專門營造業', '261. 4390 其他專門營造業'], [262, 4510, '商品經紀業', '262. 4510 商品經紀業'], [263, 4520, '綜合商品批發業', '263. 4520 綜合商品批發業'], [264, 4531, '穀類及豆類批發業', '264. 4531 穀類及豆類批發業'], [265, 4532, '花卉批發業', '265. 4532 花卉批發業'], [266, 4533, '活動物批發業', '266. 4533 活動物批發業'], [267, 4539, '其他農產原料批發業', '267. 4539 其他農產原料批發業'], [268, 4541, '蔬果批發業', '268. 4541 蔬果批發業'], [269, 4542, '肉品批發業', '269. 4542 肉品批發業'], [270, 4543, '水產品批發業', '270. 4543 水產品批發業'], [271, 4544, '冷凍調理食品批發業', '271. 4544 冷凍調理食品批發業'], [272, 4545, '乳製品、蛋及食用油脂批發業', '272. 4545 乳製品、蛋及食用油脂批發業'], [273, 4546, '菸酒批發業', '273. 4546 菸酒批發業'], [274, 4547, '非酒精飲料批發業', '274. 4547 非酒精飲料批發業'], [275, 4548, '咖啡、茶葉及香料批發業', '275. 4548 咖啡、茶葉及香料批發業'], [276, 4549, '其他食品批發業', '276. 4549 其他食品批發業'], [277, 4551, '布疋批發業', '277. 4551 布疋批發業'], [278, 4552, '服裝及其配件批發業', '278. 4552 服裝及其配件批發業'], [279, 4553, '鞋類批發業', '279. 4553 鞋類批發業'], [280, 4559, '其他服飾品批發業', '280. 4559 其他服飾品批發業'], [281, 4561, '家庭電器批發業', '281. 4561 家庭電器批發業'], [282, 4562, '家具批發業', '282. 4562 家具批發業'], [283, 4563, '家飾品批發業', '283. 4563 家飾品批發業'], [284, 4564, '家用攝影器材及光學產品批發業', '284. 4564 家用攝影器材及光學產品批發業'], [285, 4565, '鐘錶及眼鏡批發業', '285. 4565 鐘錶及眼鏡批發業'], [286, 4566, '珠寶及貴金屬製品批發業', '286. 4566 珠寶及貴金屬製品批發業'], [287, 4567, '清潔用品批發業', '287. 4567 清潔用品批發業'], [288, 4569, '其他家庭器具及用品批發業', '288. 4569 其他家庭器具及用品批發業'], [289, 4571, '藥品及醫療用品批發業', '289. 4571 藥品及醫療用品批發業'], [290, 4572, '化粧品批發業', '290. 4572 化粧品批發業'], [291, 4581, '書籍、文具批發業', '291. 4581 書籍、文具批發業'], [292, 4582, '運動用品、器材批發業', '292. 4582 運動用品、器材批發業'], [293, 4583, '玩具、娛樂用品批發業', '293. 4583 玩具、娛樂用品批發業'], [294, 4611, '木製建材批發業', '294. 4611 木製建材批發業'], [295, 4612, '磚瓦、砂石、水泥及其製品批發業', '295. 4612 磚瓦、砂石、水泥及其製品批發業'], [296, 4613, '磁磚、貼面石材、衛浴設備批發業', '296. 4613 磁磚、貼面石材、衛浴設備批發業'], [297, 4614, '漆料、塗料批發業', '297. 4614 漆料、塗料批發業'], [298, 4615, '金屬建材批發業', '298. 4615 金屬建材批發業'], [299, 4619, '其他建材批發業', '299. 4619 其他建材批發業'], [300, 4621, '化學原料批發業', '300. 4621 化學原料批發業'], [301, 4622, '化學製品批發業', '301. 4622 化學製品批發業'], [302, 4631, '石油製品燃料批發業', '302. 4631 石油製品燃料批發業'], [303, 4639, '其他燃料批發業', '303. 4639 其他燃料批發業'], [304, 4641, '電腦及其週邊設備、軟體批發業', '304. 4641 電腦及其週邊設備、軟體批發業'], [305, 4642, '電子設備及其零組件批發業', '305. 4642 電子設備及其零組件批發業'], [306, 4643, '農用及工業用機械設備批發業', '306. 4643 農用及工業用機械設備批發業'], [307, 4644, '辦公用機械器具批發業', '307. 4644 辦公用機械器具批發業'], [308, 4649, '其他機械器具批發業', '308. 4649 其他機械器具批發業'], [309, 4651, '汽車批發業', '309. 4651 汽車批發業'], [310, 4652, '機車批發業', '310. 4652 機車批發業'], [311, 4653, '汽機車零配件、用品批發業', '311. 4653 汽機車零配件、用品批發業'], [312, 4691, '回收物料批發業', '312. 4691 回收物料批發業'], [313, 4699, '未分類其他專賣批發業', '313. 4699 未分類其他專賣批發業'], [314, 4711, '食品飲料為主之綜合商品零售業', '314. 4711 食品飲料為主之綜合商品零售業'], [315, 4719, '其他綜合商品零售業', '315. 4719 其他綜合商品零售業'], [316, 4721, '蔬果零售業', '316. 4721 蔬果零售業'], [317, 4722, '肉品零售業', '317. 4722 肉品零售業'], [318, 4723, '水產品零售業', '318. 4723 水產品零售業'], [319, 4729, '其他食品及飲料、菸草製品零售業', '319. 4729 其他食品及飲料、菸草製品零售業'], [320, 4731, '布疋零售業', '320. 4731 布疋零售業'], [321, 4732, '服裝及其配件零售業', '321. 4732 服裝及其配件零售業'], [322, 4733, '鞋類零售業', '322. 4733 鞋類零售業'], [323, 4739, '其他服飾品零售業', '323. 4739 其他服飾品零售業'], [324, 4741, '家庭電器零售業', '324. 4741 家庭電器零售業'], [325, 4742, '家具零售業', '325. 4742 家具零售業'], [326, 4743, '家飾品零售業', '326. 4743 家飾品零售業'], [327, 4744, '鐘錶及眼鏡零售業', '327. 4744 鐘錶及眼鏡零售業'], [328, 4745, '珠寶及貴金屬製品零售業', '328. 4745 珠寶及貴金屬製品零售業'], [329, 4749, '其他家庭器具及用品零售業', '329. 4749 其他家庭器具及用品零售業'], [330, 4751, '藥品及醫療用品零售業', '330. 4751 藥品及醫療用品零售業'], [331, 4752, '化粧品零售業', '331. 4752 化粧品零售業'], [332, 4761, '書籍、文具零售業', '332. 4761 書籍、文具零售業'], [333, 4762, '運動用品、器材零售業', '333. 4762 運動用品、器材零售業'], [334, 4763, '玩具、娛樂用品零售業', '334. 4763 玩具、娛樂用品零售業'], [335, 4764, '音樂帶及影片零售業', '335. 4764 音樂帶及影片零售業'], [336, 4810, '建材零售業', '336. 4810 建材零售業'], [337, 4821, '加油站業', '337. 4821 加油站業'], [338, 4829, '其他燃料零售業', '338. 4829 其他燃料零售業'], [339, 4831, '電腦及其週邊設備、軟體零售業', '339. 4831 電腦及其週邊設備、軟體零售業'], [340, 4832, '通訊設備零售業', '340. 4832 通訊設備零售業'], [341, 4833, '視聽設備零售業', '341. 4833 視聽設備零售業'], [342, 4841, '汽車零售業', '342. 4841 汽車零售業'], [343, 4842, '機車零售業', '343. 4842 機車零售業'], [344, 4843, '汽機車零配件、用品零售業', '344. 4843 汽機車零配件、用品零售業'], [345, 4851, '花卉零售業', '345. 4851 花卉零售業'], [346, 4852, '其他全新商品零售業', '346. 4852 其他全新商品零售業'], [347, 4853, '中古商品零售業', '347. 4853 中古商品零售業'], [348, 4861, '食品、飲料及菸草製品之零售攤販業', '348. 4861 食品、飲料及菸草製品之零售攤販業'], [349, 4862, '紡織品、服裝及鞋類之零售攤販業', '349. 4862 紡織品、服裝及鞋類之零售攤販業'], [350, 4869, '其他商品之零售攤販業', '350. 4869 其他商品之零售攤販業'], [351, 4871, '電子購物及郵購業', '351. 4871 電子購物及郵購業'], [352, 4872, '直銷業', '352. 4872 直銷業'], [353, 4879, '未分類其他無店面零售業', '353. 4879 未分類其他無店面零售業'], [354, 4910, '鐵路運輸業', '354. 4910 鐵路運輸業'], [355, 4920, '大眾捷運系統運輸業', '355. 4920 大眾捷運系統運輸業'], [356, 4931, '公共汽車客運業', '356. 4931 公共汽車客運業'], [357, 4932, '計程車客運業', '357. 4932 計程車客運業'], [358, 4939, '其他汽車客運業', '358. 4939 其他汽車客運業'], [359, 4940, '汽車貨運業', '359. 4940 汽車貨運業'], [360, 4990, '其他陸上運輸業', '360. 4990 其他陸上運輸業'], [361, 5010, '海洋水運業', '361. 5010 海洋水運業'], [362, 5020, '內河及湖泊水運業', '362. 5020 內河及湖泊水運業'], [363, 5101, '民用航空運輸業', '363. 5101 民用航空運輸業'], [364, 5102, '普通航空業', '364. 5102 普通航空業'], [365, 5210, '報關業', '365. 5210 報關業'], [366, 5220, '船務代理業', '366. 5220 船務代理業'], [367, 5231, '陸上貨運承攬業', '367. 5231 陸上貨運承攬業'], [368, 5232, '海洋貨運承攬業', '368. 5232 海洋貨運承攬業'], [369, 5233, '航空貨運承攬業', '369. 5233 航空貨運承攬業'], [370, 5241, '停車場業', '370. 5241 停車場業'], [371, 5249, '其他陸上運輸輔助業', '371. 5249 其他陸上運輸輔助業'], [372, 5251, '港埠業', '372. 5251 港埠業'], [373, 5259, '其他水上運輸輔助業', '373. 5259 其他水上運輸輔助業'], [374, 5260, '航空運輸輔助業', '374. 5260 航空運輸輔助業'], [375, 5290, '其他運輸輔助業', '375. 5290 其他運輸輔助業'], [376, 5301, '普通倉儲業', '376. 5301 普通倉儲業'], [377, 5302, '冷凍冷藏倉儲業', '377. 5302 冷凍冷藏倉儲業'], [378, 5410, '郵政業', '378. 5410 郵政業'], [379, 5420, '快遞服務業', '379. 5420 快遞服務業'], [380, 5510, '短期住宿服務業', '380. 5510 短期住宿服務業'], [381, 5590, '其他住宿服務業', '381. 5590 其他住宿服務業'], [382, 5610, '餐館業', '382. 5610 餐館業'], [383, 5621, '非酒精飲料店業', '383. 5621 非酒精飲料店業'], [384, 5622, '酒精飲料店業', '384. 5622 酒精飲料店業'], [385, 5631, '餐食攤販業', '385. 5631 餐食攤販業'], [386, 5632, '調理飲料攤販業', '386. 5632 調理飲料攤販業'], [387, 5690, '其他餐飲業', '387. 5690 其他餐飲業'], [388, 5811, '新聞出版業', '388. 5811 新聞出版業'], [389, 5812, '雜誌（期刊）出版業', '389. 5812 雜誌（期刊）出版業'], [390, 5813, '書籍出版業', '390. 5813 書籍出版業'], [391, 5819, '其他出版業', '391. 5819 其他出版業'], [392, 5820, '軟體出版業', '392. 5820 軟體出版業'], [393, 5911, '影片製作業', '393. 5911 影片製作業'], [394, 5912, '影片後製服務業', '394. 5912 影片後製服務業'], [395, 5913, '影片發行業', '395. 5913 影片發行業'], [396, 5914, '影片放映業', '396. 5914 影片放映業'], [397, 5920, '聲音錄製及音樂出版業', '397. 5920 聲音錄製及音樂出版業'], [398, 6010, '廣播業', '398. 6010 廣播業'], [399, 6021, '電視傳播業', '399. 6021 電視傳播業'], [400, 6022, '有線及其他付費節目播送業', '400. 6022 有線及其他付費節目播送業'], [401, 6100, '電信業', '401. 6100 電信業'], [402, 6201, '電腦軟體設計業', '402. 6201 電腦軟體設計業'], [403, 6202, '電腦系統整合服務業', '403. 6202 電腦系統整合服務業'], [404, 6209, '其他電腦系統設計服務業', '404. 6209 其他電腦系統設計服務業'], [405, 6311, '入口網站經營業', '405. 6311 入口網站經營業'], [406, 6312, '資料處理、網站代管及相關服務業', '406. 6312 資料處理、網站代管及相關服務業'], [407, 6391, '新聞供應業', '407. 6391 新聞供應業'], [408, 6399, '未分類其他資訊供應服務業', '408. 6399 未分類其他資訊供應服務業'], [409, 6411, '中央銀行', '409. 6411 中央銀行'], [410, 6412, '銀行業', '410. 6412 銀行業'], [411, 6413, '信用合作社業', '411. 6413 信用合作社業'], [412, 6414, '農會、漁會信用部', '412. 6414 農會、漁會信用部'], [413, 6415, '郵政儲金匯兌業', '413. 6415 郵政儲金匯兌業'], [414, 6419, '其他存款機構', '414. 6419 其他存款機構'], [415, 6420, '金融控股業', '415. 6420 金融控股業'], [416, 6430, '信託、基金及其他金融工具', '416. 6430 信託、基金及其他金融工具'], [417, 6491, '金融租賃業', '417. 6491 金融租賃業'], [418, 6492, '票券金融業', '418. 6492 票券金融業'], [419, 6493, '證券金融業', '419. 6493 證券金融業'], [420, 6494, '信用卡業', '420. 6494 信用卡業'], [421, 6495, '典當業', '421. 6495 典當業'], [422, 6496, '民間融資業', '422. 6496 民間融資業'], [423, 6499, '未分類其他金融中介業', '423. 6499 未分類其他金融中介業'], [424, 6510, '人身保險業', '424. 6510 人身保險業'], [425, 6520, '財產保險業', '425. 6520 財產保險業'], [426, 6530, '再保險業', '426. 6530 再保險業'], [427, 6540, '退休基金', '427. 6540 退休基金'], [428, 6551, '保險代理及經紀業', '428. 6551 保險代理及經紀業'], [429, 6559, '其他保險及退休基金輔助業', '429. 6559 其他保險及退休基金輔助業'], [430, 6611, '證券商', '430. 6611 證券商'], [431, 6619, '其他證券業', '431. 6619 其他證券業'], [432, 6621, '期貨商', '432. 6621 期貨商'], [433, 6629, '其他期貨業', '433. 6629 其他期貨業'], [434, 6631, '投資顧問業', '434. 6631 投資顧問業'], [435, 6632, '信託服務業', '435. 6632 信託服務業'], [436, 6639, '其他金融輔助業', '436. 6639 其他金融輔助業'], [437, 6640, '基金管理業', '437. 6640 基金管理業'], [438, 6700, '不動產開發業', '438. 6700 不動產開發業'], [439, 6811, '不動產租售業', '439. 6811 不動產租售業'], [440, 6812, '不動產經紀業', '440. 6812 不動產經紀業'], [441, 6891, '不動產管理業', '441. 6891 不動產管理業'], [442, 6899, '未分類其他不動產業', '442. 6899 未分類其他不動產業'], [443, 6911, '律師事務服務業', '443. 6911 律師事務服務業'], [444, 6912, '代書事務服務業', '444. 6912 代書事務服務業'], [445, 6919, '其他法律服務業', '445. 6919 其他法律服務業'], [446, 6920, '會計服務業', '446. 6920 會計服務業'], [447, 7010, '企業總管理機構', '447. 7010 企業總管理機構'], [448, 7020, '管理顧問業', '448. 7020 管理顧問業'], [449, 7111, '建築服務業', '449. 7111 建築服務業'], [450, 7112, '工程服務及相關技術顧問業', '450. 7112 工程服務及相關技術顧問業'], [451, 7121, '環境檢測服務業', '451. 7121 環境檢測服務業'], [452, 7129, '其他技術檢測及分析服務業', '452. 7129 其他技術檢測及分析服務業'], [453, 7210, '自然及工程科學研究發展服務業', '453. 7210 自然及工程科學研究發展服務業'], [454, 7220, '社會及人文科學研究發展服務業', '454. 7220 社會及人文科學研究發展服務業'], [455, 7230, '綜合研究發展服務業', '455. 7230 綜合研究發展服務業'], [456, 7311, '一般廣告業', '456. 7311 一般廣告業'], [457, 7312, '戶外廣告業', '457. 7312 戶外廣告業'], [458, 7319, '其他廣告業', '458. 7319 其他廣告業'], [459, 7320, '市場研究及民意調查業', '459. 7320 市場研究及民意調查業'], [460, 7401, '室內設計業', '460. 7401 室內設計業'], [461, 7409, '其他專門設計服務業', '461. 7409 其他專門設計服務業'], [462, 7500, '獸醫服務業', '462. 7500 獸醫服務業'], [463, 7601, '攝影業', '463. 7601 攝影業'], [464, 7602, '翻譯服務業', '464. 7602 翻譯服務業'], [465, 7603, '藝人及模特兒等經紀業', '465. 7603 藝人及模特兒等經紀業'], [466, 7609, '未分類其他專業、科學及技術服務業', '466. 7609 未分類其他專業、科學及技術服務業'], [467, 7711, '營造用機械設備租賃業', '467. 7711 營造用機械設備租賃業'], [468, 7712, '農業及其他工業用機械設備租賃業', '468. 7712 農業及其他工業用機械設備租賃業'], [469, 7713, '辦公用機械設備租賃業', '469. 7713 辦公用機械設備租賃業'], [470, 7719, '其他機械設備租賃業', '470. 7719 其他機械設備租賃業'], [471, 7721, '汽車租賃業', '471. 7721 汽車租賃業'], [472, 7722, '船舶租賃業', '472. 7722 船舶租賃業'], [473, 7723, '貨櫃租賃業', '473. 7723 貨櫃租賃業'], [474, 7729, '其他運輸工具設備租賃業', '474. 7729 其他運輸工具設備租賃業'], [475, 7731, '運動及娛樂用品租賃業', '475. 7731 運動及娛樂用品租賃業'], [476, 7732, '錄影帶及碟片租賃業', '476. 7732 錄影帶及碟片租賃業'], [477, 7739, '其他物品租賃業', '477. 7739 其他物品租賃業'], [478, 7740, '非金融性無形資產租賃業', '478. 7740 非金融性無形資產租賃業'], [479, 7801, '職業介紹服務業', '479. 7801 職業介紹服務業'], [480, 7802, '人力派遣業', '480. 7802 人力派遣業'], [481, 7809, '其他就業服務業', '481. 7809 其他就業服務業'], [482, 7900, '旅行業', '482. 7900 旅行業'], [483, 8001, '保全服務業', '483. 8001 保全服務業'], [484, 8002, '系統保全服務業', '484. 8002 系統保全服務業'], [485, 8003, '私家偵探服務業', '485. 8003 私家偵探服務業'], [486, 8110, '複合支援服務業', '486. 8110 複合支援服務業'], [487, 8120, '清潔服務業', '487. 8120 清潔服務業'], [488, 8130, '綠化服務業', '488. 8130 綠化服務業'], [489, 8201, '代收帳款及信用調查服務業', '489. 8201 代收帳款及信用調查服務業'], [490, 8202, '會議及展覽服務業', '490. 8202 會議及展覽服務業'], [491, 8203, '影印業', '491. 8203 影印業'], [492, 8209, '其他業務及辦公室支援服務業', '492. 8209 其他業務及辦公室支援服務業'], [493, 8311, '政府機關', '493. 8311 政府機關'], [494, 8312, '民意機關', '494. 8312 民意機關'], [495, 8320, '國防事務業', '495. 8320 國防事務業'], [496, 8330, '強制性社會安全', '496. 8330 強制性社會安全'], [497, 8400, '國際組織及外國機構', '497. 8400 國際組織及外國機構'], [498, 8510, '學前教育事業', '498. 8510 學前教育事業'], [499, 8520, '小學', '499. 8520 小學'], [500, 8530, '中學', '500. 8530 中學'], [501, 8540, '職業學校', '501. 8540 職業學校'], [502, 8550, '大專校院', '502. 8550 大專校院'], [503, 8560, '特殊教育事業', '503. 8560 特殊教育事業'], [504, 8571, '外語教育服務業', '504. 8571 外語教育服務業'], [505, 8572, '藝術教育服務業', '505. 8572 藝術教育服務業'], [506, 8573, '運動及休閒教育服務業', '506. 8573 運動及休閒教育服務業'], [507, 8574, '商業、資訊及專業管理教育服務業', '507. 8574 商業、資訊及專業管理教育服務業'], [508, 8579, '未分類其他教育服務業', '508. 8579 未分類其他教育服務業'], [509, 8580, '教育輔助服務業', '509. 8580 教育輔助服務業'], [510, 8610, '醫院', '510. 8610 醫院'], [511, 8620, '診所', '511. 8620 診所'], [512, 8691, '醫學檢驗服務業', '512. 8691 醫學檢驗服務業'], [513, 8699, '未分類其他醫療保健服務業', '513. 8699 未分類其他醫療保健服務業'], [514, 8701, '護理照顧服務業', '514. 8701 護理照顧服務業'], [515, 8702, '心智障礙及藥物濫用者居住照顧服務業', '515. 8702 心智障礙及藥物濫用者居住照顧服務業'], [516, 8703, '老人居住照顧服務業', '516. 8703 老人居住照顧服務業'], [517, 8709, '其他居住照顧服務業', '517. 8709 其他居住照顧服務業'], [518, 8801, '兒童及少年之社會工作服務業', '518. 8801 兒童及少年之社會工作服務業'], [519, 8802, '老人之社會工作服務業', '519. 8802 老人之社會工作服務業'], [520, 8803, '身心障礙者之社會工作服務業', '520. 8803 身心障礙者之社會工作服務業'], [521, 8804, '婦女之社會工作服務業', '521. 8804 婦女之社會工作服務業'], [522, 8809, '未分類其他社會工作服務業', '522. 8809 未分類其他社會工作服務業'], [523, 9010, '創作業', '523. 9010 創作業'], [524, 9020, '藝術表演業', '524. 9020 藝術表演業'], [525, 9031, '藝術表演場所經營業', '525. 9031 藝術表演場所經營業'], [526, 9039, '其他藝術表演輔助服務業', '526. 9039 其他藝術表演輔助服務業'], [527, 9101, '圖書館及檔案保存業', '527. 9101 圖書館及檔案保存業'], [528, 9102, '植物園、動物園及自然生態保護機構', '528. 9102 植物園、動物園及自然生態保護機構'], [529, 9103, '博物館、歷史遺址及其他類似機構', '529. 9103 博物館、歷史遺址及其他類似機構'], [530, 9200, '博弈業', '530. 9200 博弈業'], [531, 9311, '職業運動業', '531. 9311 職業運動業'], [532, 9312, '運動場館業', '532. 9312 運動場館業'], [533, 9319, '其他運動服務業', '533. 9319 其他運動服務業'], [534, 9321, '遊樂園及主題樂園', '534. 9321 遊樂園及主題樂園'], [535, 9322, '視聽及視唱業', '535. 9322 視聽及視唱業'], [536, 9323, '特殊娛樂業', '536. 9323 特殊娛樂業'], [537, 9324, '遊戲場業', '537. 9324 遊戲場業'], [538, 9329, '其他娛樂及休閒服務業', '538. 9329 其他娛樂及休閒服務業'], [539, 9410, '宗教組織', '539. 9410 宗教組織'], [540, 9421, '工商業團體', '540. 9421 工商業團體'], [541, 9422, '專門職業團體', '541. 9422 專門職業團體'], [542, 9423, '勞工團體', '542. 9423 勞工團體'], [543, 9424, '農民團體', '543. 9424 農民團體'], [544, 9491, '政治團體', '544. 9491 政治團體'], [545, 9499, '未分類其他組織', '545. 9499 未分類其他組織'], [546, 9511, '汽車維修業', '546. 9511 汽車維修業'], [547, 9512, '汽車美容業', '547. 9512 汽車美容業'], [548, 9521, '電腦及其週邊設備修理業', '548. 9521 電腦及其週邊設備修理業'], [549, 9522, '通訊傳播設備修理業', '549. 9522 通訊傳播設備修理業'], [550, 9523, '視聽電子產品及家用電器修理業', '550. 9523 視聽電子產品及家用電器修理業'], [551, 9591, '機車維修業', '551. 9591 機車維修業'], [552, 9599, '未分類其他個人及家庭用品維修業', '552. 9599 未分類其他個人及家庭用品維修業'], [553, 9610, '洗衣業', '553. 9610 洗衣業'], [554, 9620, '理髮及美容業', '554. 9620 理髮及美容業'], [555, 9630, '殯葬服務業', '555. 9630 殯葬服務業'], [556, 9640, '家事服務業', '556. 9640 家事服務業'], [557, 9690, '其他個人服務業', '557. 9690 其他個人服務業']]

        for i in range(len(process_list)):
            for j in range(len(process_list[0])):
                worksheet.write(f"{chr_ord[j+1]}{i+2}", process_list[i][j], cf8)
        
        logger.info("生成 附表二、含氟氣體之GWP值")
        # ------------------------- 附表二、含氟氣體之GWP值 -----------------------------
        df.to_excel(writer, sheet_name="附表二、含氟氣體之GWP值", )
        worksheet = writer.sheets["附表二、含氟氣體之GWP值"]

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
    
    # workbook = load_workbook(filename)

    # worksheet = workbook[sheet]
    # worksheet.protection.enable()

    # workbook.save(filename)

    endRunTime = time.time()
    logger.info(f"Time used for generating GHG report: {util.convert_sec(endRunTime - startRunTime)}.")