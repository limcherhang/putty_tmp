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
    companyId = sys.argv[1]
    env = sys.argv[2]

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
        client = MongoConn(config["mongo_staging_nxmap"])

    client.connect()
    db = client.get_database()

    site_modules = db.site_modules.find_one({"_id": ObjectId(companyId)})
    companyName = site_modules["companyName"]

    chr_ord = {}

    for i in range(65, 91):
        chr_ord[i-64] = chr(i)
        chr_ord[i-38] = "A"+chr(i)

    user = db.users.find_one({"companyId": ObjectId(companyId)})
    _id = user["_id"]

    activity_logs = db.activity_logs.find({"updatedBy": str(_id)}).sort("updatedDate", -1)

    company_activity_logs = []
    for idx, activity_log in enumerate(activity_logs):
        action = util.decryption(activity_log["type"])
        field = util.decryption(activity_log["field"])
        description = util.decryption(activity_log["updatedValue"])
        updatedDate = datetime.datetime.fromisoformat(str(activity_log["updatedDate"])).replace(tzinfo=datetime.timezone.utc).timestamp()
        updatedDate = datetime.datetime.fromtimestamp(updatedDate).strftime("%B %d, %Y %I:%M %p %z")

        company_activity_logs.append({"No.": idx+1, "companyName": companyName, "Action": action, "Field": field, "Description": description, "Date": updatedDate})

    df = pd.DataFrame(company_activity_logs, columns=["No.", "companyName", "Action", "Field", "Description", "Date"])

    sheetname = "activitiy_logs"

    with pd.ExcelWriter(f"{companyName}_activity_logs.xlsx", engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheetname, index=False)

        workbook = writer.book
        worksheet = writer.sheets[sheetname]

        cf1 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#D9E1F2",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })
        cf2 = workbook.add_format({
            "font_size": 12,    # 字体大小
            "pattern": 1,       # 填充模式
            "bg_color": "#FFFFFF",  # 橙色的 Accent 6
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True
        })

        for idx, title in enumerate(df.columns):
            worksheet.write(f"{chr(65 + idx)}1", title, cf1)
    
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max(
                series.astype(str).map(len).max(),
                len(str(series.name))
            ) + 5
            worksheet.set_column(idx,idx,max_len)

    endRunTime = time.time()
    client.close()
    logger.info(f"Time used: {util.convert_sec(endRunTime - startRunTime)}.")