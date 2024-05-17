import os
import sys
import subprocess
from connection.mongo_connection import MongoConn
import logging
from utils import myLog, util
import time
import shutil
import configparser

if __name__ == "__main__":
    startRunTime = time.time()
    config = configparser.ConfigParser()
    config.read(os.getcwd() + "/config.ini")
    
    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", level=logging.INFO)

    client = MongoConn(config["mongo_production_nxmap"])
    client.connect()
    db = client.get_database()

    site_modules = list(db.site_modules.find())

    python_script = "ghg_report.py"

    report_tmp_folder = "report_tmp"
    try:
        os.makedirs(report_tmp_folder)
    except FileExistsError:
        pass

    for item in site_modules:
        if item.get("usedRegion") != "Taiwan":
            continue
        companyId = item['_id']
        companyName = item['companyName']
        
        inventory_year = "2024"

        filename_tw = f"溫室氣體盤查清冊v1.2_{companyName}_{inventory_year}.xlsx"
        filename_en = f"GHG Report v1.2_{companyName}_{inventory_year}.xlsx"
        filename_de = f"GHG-Bericht v1.2_{companyName}_{inventory_year}.xlsx"
        filename_jp = f"GHGレポートv1.2_{companyName}_{inventory_year}.xlsx"
        filename_it = f"Rapporto sui gas serra v1.2_{companyName}_{inventory_year}.xlsx"
        filename_th = f"รายงานโลหิตสารเคมีv1.2_{companyName}_{inventory_year}.xlsx"
        filename_zh = f"碳排放报告v1.2_{companyName}_{inventory_year}.xlsx"

        success_file = []

        params1 = [str(companyId), inventory_year, "production", "tw"]
        params2 = [str(companyId), inventory_year, "production", "en"]
        params3 = [str(companyId), inventory_year, "production", "de"]
        params4 = [str(companyId), inventory_year, "production", "jp"]
        params5 = [str(companyId), inventory_year, "production", "it"]
        params6 = [str(companyId), inventory_year, "production", "th"]
        params7 = [str(companyId), inventory_year, "production", "zh"]

        errorCode = 0

        try:
            subprocess.run(["python3", python_script] + params1, check=True)
            logger.info(f"{companyId} tw 執行成功！")
        except subprocess.CalledProcessError as e:
            logger.error(f"{companyId}: {companyName} 語言: tw 執行Python腳本時出錯: {e}")
            errorCode = 1
        
        # try:
        #     subprocess.run(["python3", python_script] + params2, check=True)
        #     logger.info(f"{companyId} en 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: en 執行Python腳本時出錯: {e}")
        #     errorCode = 1
        
        # try:
        #     subprocess.run(["python3", python_script] + params3, check=True)
        #     logger.info(f"{companyId} de 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: de 執行Python腳本時出錯: {e}")
        #     errorCode = 1

        # try:
        #     subprocess.run(["python3", python_script] + params4, check=True)
        #     logger.info(f"{companyId} jp 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: jp 執行Python腳本時出錯: {e}")
        #     errorCode = 1

        # try:
        #     subprocess.run(["python3", python_script] + params5, check=True)
        #     logger.info(f"{companyId} it 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: it 執行Python腳本時出錯: {e}")
        #     errorCode = 1

        # try:
        #     subprocess.run(["python3", python_script] + params6, check=True)
        #     logger.info(f"{companyId} th 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: th 執行Python腳本時出錯: {e}")
        #     errorCode = 1

        # try:
        #     subprocess.run(["python3", python_script] + params7, check=True)
        #     logger.info(f"{companyId} zh 執行成功！")
        # except subprocess.CalledProcessError as e:
        #     logger.error(f"{companyId}: {companyName} 語言: zh 執行Python腳本時出錯: {e}")
        #     errorCode = 1

        success_file.append(filename_tw)
        # success_file.append(filename_en)
        # success_file.append(filename_de)
        # success_file.append(filename_jp)
        # success_file.append(filename_it)
        # success_file.append(filename_th)
        # success_file.append(filename_zh)

        companyName_companyId = "failed_" + companyName + "_" + str(companyId) if errorCode == 1 else companyName + "_" + str(companyId)
        company_folder = os.path.join(report_tmp_folder, companyName_companyId)

        try:
            os.makedirs(f"{report_tmp_folder}/{companyName_companyId}")
        except FileExistsError:
            pass
        
        # 移动成功的文件到对应的文件夹W
        for file in success_file:
            try:
                shutil.move(file, os.path.join(company_folder, file))
            except:
                pass

    client.close()

    endRunTime = time.time()
    logger.info(f"執行時間: {endRunTime - startRunTime} 秒")