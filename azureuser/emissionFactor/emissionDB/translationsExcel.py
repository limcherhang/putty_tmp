import os
import sys
rootPath = os.getcwd() + '/../../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
from utils import myLog
from bson import ObjectId
import time
import pandas as pd

def getTranslation(cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT tr.en, tr.de, tr.it, tr.jp, tr.th, tr.tw, tr.zh, url.urlName FROM mgmtCarbon.translations tr
        JOIN mgmtCarbon.url ON url.urlId = tr.urlId ORDER BY tr.en;"""

    cursor.execute(sqlCommand)
    return cursor.fetchall()

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]+'.log'
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}", config["mysql_azureV2"])

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        translations = getTranslation(cursor)
        datas = {}
        datas_de = {}
        datas_it = {}
        datas_jp = {}
        datas_th = {}
        datas_tw = {}
        datas_zh = {}

        for en, de, it, jp, th, tw, zh, urlName in translations:
            if urlName not in datas:
                datas[urlName] = []
                datas_de[urlName] = []
                datas_it[urlName] = []
                datas_jp[urlName] = []
                datas_th[urlName] = []
                datas_tw[urlName] = []
                datas_zh[urlName] = []
            
            datas[urlName].append([en, de, it, jp, th, tw, zh])
            datas_de[urlName].append([en, de])
            datas_it[urlName].append([en, it])
            datas_jp[urlName].append([en, jp])
            datas_th[urlName].append([en, th])
            datas_tw[urlName].append([en, tw])
            datas_zh[urlName].append([en, zh])
        
    with pd.ExcelWriter('translation.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas.items():
            df = pd.DataFrame(language, columns=['en', 'de', 'it', 'jp', 'th', 'tw', 'zh'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 30
            worksheet.column_dimensions['C'].width = 30
            worksheet.column_dimensions['D'].width = 20
            worksheet.column_dimensions['E'].width = 20
            worksheet.column_dimensions['F'].width = 20
            worksheet.column_dimensions['G'].width = 20

            worksheet.freeze_panes = 'A2'

    with pd.ExcelWriter('translations_de.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_de.items():
            df = pd.DataFrame(language, columns=['en', 'de'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 50
            worksheet.column_dimensions['B'].width = 50

            worksheet.freeze_panes = 'A2'

    with pd.ExcelWriter('translations_it.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_it.items():
            df = pd.DataFrame(language, columns=['en', 'it'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 50
            worksheet.column_dimensions['B'].width = 50

            worksheet.freeze_panes = 'A2'
            
    with pd.ExcelWriter('translations_jp.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_jp.items():
            df = pd.DataFrame(language, columns=['en', 'jp'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 30

            worksheet.freeze_panes = 'A2'

    with pd.ExcelWriter('translations_th.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_th.items():
            df = pd.DataFrame(language, columns=['en', 'th'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 30

            worksheet.freeze_panes = 'A2'

    with pd.ExcelWriter('translations_tw.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_tw.items():
            df = pd.DataFrame(language, columns=['en', 'tw'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 30

            worksheet.freeze_panes = 'A2'

    with pd.ExcelWriter('translations_zh.xlsx', engine='openpyxl') as writer:
        for urlName, language in datas_zh.items():
            df = pd.DataFrame(language, columns=['en', 'zh'])
            df.to_excel(writer, sheet_name=urlName, index=False)

            workbook = writer.book
            worksheet = workbook[urlName]

            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 30

            worksheet.freeze_panes = 'A2'

    conn.close()