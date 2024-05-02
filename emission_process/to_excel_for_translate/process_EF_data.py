import os
import sys
rootPath = os.getcwd() + '/../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
import pymongo
from utils import myLog, util
from bson import ObjectId
import time
import pandas as pd

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"])

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        source = "ThaiWah"
        
        ever_schema = 'ever'
        ever_table = 'EF_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
    
        sqlCommand = f"SELECT * FROM {ever_schema}.{ever_table}"

        cursor.execute(sqlCommand)

        columns = ['Name (translate in English)', 'Name (from original source)', 'CO2', 'CH4', 'N2O', "Unit", "year", "co2Unit", "ch4Unit", "n2oUnit", "everTable", "everId", "scope", "category"]
        final_data = []

        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['ชอื่', 'หนว่ ย', 'ล าดบั ท ี่', 'รายละเอยี ด', 'วนั ทอี่ พั เดท', 'แหลง่ ขอ้ มลู อา้ งองิ', 'คา่ แฟคเตอร ์(kgCO2e/หนว่ ย)'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO O2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2e eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'To otal(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/un nit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq q/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Tota al(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total( (kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(k kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit t)', 'Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2 2O/unit)', 'Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'CH4(k kgCH4/unit)', 'Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล'], 
        # ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'T Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล']

        for id, _type, update, rawData in cursor.fetchall():
            everId = _type + ' - ' + f"{id}"

            rawData = eval(rawData)

            key = [k for k in rawData]

            if key == ['Units', 'ชอื่', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/unit)', 'แหลง่อา้งองิขอ้มูล']:
                
                sourceOfEmission = rawData['ชอื่']
                ch4Factor, ch4Unit = rawData['CH4(kgCH4/unit)'].replace('E E', 'E').replace('5 5', '5'), "kgCH₄"
                co2Factor, co2Unit = rawData['CO2(kgCO2/unit)'].replace('E E', 'E'), "kgCO₂"
                n2oFactor, n2oUnit = rawData['N2O(kgN2O/unit)'].replace('E E', 'E').replace('5 5', '5').replace('6 6', '6'), "kgN₂O"

                if n2oFactor == "NULL L" or n2oFactor == '-' or n2oFactor == 'NULL':
                    n2oFactor = None
                if co2Factor == '-' or co2Factor == 'NULL':
                    co2Factor = None
                if ch4Factor == '-' or ch4Factor == 'NULL':
                    ch4Factor = None


                if co2Factor is not None:
                    co2Factor = float(co2Factor)
                if ch4Factor is not None:
                    ch4Factor = float(ch4Factor)
                if n2oFactor is not None:
                    n2oFactor = float(n2oFactor)

                baseUnit = rawData['Units']


                final_data.append([None, sourceOfEmission, co2Factor, ch4Factor, n2oFactor, baseUnit, 2022, co2Unit, ch4Unit, n2oUnit, ever_table, everId, None, None])

            elif key == ['ชอื่', 'หนว่ ย', 'ล าดบั ท ี่', 'รายละเอยี ด', 'วนั ทอี่ พั เดท', 'แหลง่ ขอ้ มลู อา้ งองิ', 'คา่ แฟคเตอร ์(kgCO2e/หนว่ ย)']:
                
                sourceOfEmission = rawData["ชอื่"]
                baseUnit = rawData["หนว่ ย"]
                date = rawData["วนั ทอี่ พั เดท"]

                try:
                    date = int(date[-4:])
                except:
                    date = 2000 + int(date[-2:])
                
                co2Factor, co2Unit = rawData["คา่ แฟคเตอร ์(kgCO2e/หนว่ ย)"].replace('E E', 'E'), "kgCO₂"

                if co2Factor != "NULL":
                    co2Factor = float(co2Factor)

                final_data.append([None, sourceOfEmission, co2Factor, None, None, baseUnit, date, co2Unit, None, None, ever_table, everId, None, None])

        
        df = pd.DataFrame(final_data, columns=columns)
    
        filename = f'{ever_table}.xlsx'
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='EmissionFactor', index=False)
            worksheet = writer.sheets["EmissionFactor"]

            for idx, col in enumerate(df):
                series = df[col]
                max_len = max(
                    series.astype(str).map(len).max(),
                    len(str(series.name))
                )+3
                worksheet.set_column(idx,idx,max_len)