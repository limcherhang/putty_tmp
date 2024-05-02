import os
import sys
rootPath = os.getcwd() + '/../../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
import pymongo
from utils import myLog, util
from bson import ObjectId
import time

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"])


    # Create mongo connection
    client = MongoConn(config['mongo_production_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])
    
    cal_approches = db.cal_approaches.find()
    total = 0
    total_us = 0
    total_us_epa = 0
    total_us_us = 0
    total_us_kepa = 0
    total_us_k_epa = 0
    total_tw = 0
    total_uk = 0
    total_ghg = 0
    total_ipcc = 0
    total_my = 0
    total_th = 0
    total_unf = 0
    total_unf_ccc = 0
    total_unf_cc = 0
    total_unf_kcc = 0
    total_owid = 0
    total_ktis = 0

    us_combination = ['EPA(US)', 'US EPA-SupplyChain-USD2021', 'Knowles-EPA', 'Knowles - EPA']
    tw_combination = ['EPA Taiwan', 'MOEA Taiwan', 'EPA(Taiwan)']
    uk_combination = ['DEFRA']
    ghg_combination = ['GHG', 'GHG 2017']
    ipcc_combination = ['IPCC', 'IGES']
    my_combination = ['MEIH']
    th_combination = ['ThaiWah']
    unf_combination = ['Knowles-UNFCCC', 'Knowles-UNFCC', 'Knowles - UNFCC']
    owid_combination = ['Knowles-OWID']
    ktis_combination = ['KTIS']

    for cal_approch in cal_approches:
        methods = cal_approch['methods']

        for method in methods:
            total += 1
            
            source = method['source']
            if source in us_combination:
                total_us += 1
                if source == 'EPA(US)':
                    total_us_epa += 1
                if source == 'US EPA-SupplyChain-USD2021':
                    total_us_us += 1
                if source == 'Knowles-EPA':
                    total_us_kepa += 1
                if source == 'Knowles - EPA':
                    total_us_k_epa += 1
            if source in tw_combination:
                total_tw += 1
                
            if source in uk_combination:
                total_uk += 1
            if source in ghg_combination:
                total_ghg += 1
            if source in ipcc_combination:
                total_ipcc += 1
            if source in my_combination:
                total_my += 1
            if source in th_combination:
                total_th += 1
            if source in unf_combination:
                total_unf += 1
                if source == 'Knowles-UNFCCC':
                    total_unf_ccc += 1
                if source == 'Knowles-UNFCC':
                    total_unf_cc += 1
                if source == 'Knowles - UNFCC':
                    total_unf_kcc += 1
            if source in owid_combination:
                total_owid += 1
            if source in ktis_combination:
                total_ktis += 1
    
    logger.debug(f"Total number of EF: {total}")
    logger.debug(f"Total number of EF USA: {total_us}")
    logger.debug(f"Total number of EF USA EPA: {total_us_epa}")
    logger.debug(f"Total number of EF USA US EPA-SupplyChain-USD2021: {total_us_us}")
    logger.debug(f"Total number of EF USA Knowles-EPA: {total_us_kepa}")
    logger.debug(f"Total number of EF USA Knowles - EPA: {total_us_k_epa}")
    logger.debug(f"Total number of EF TW: {total_tw}")
    logger.debug(f"Total number of EF UK: {total_uk}")
    logger.debug(f"Total number of EF GHG: {total_ghg}")
    logger.debug(f"Total number of EF IPCC: {total_ipcc}")
    logger.debug(f"Total number of EF MY: {total_my}")
    logger.debug(f"Total number of EF TH: {total_th}")
    logger.debug(f"Total number of EF UNF: {total_unf}")
    logger.debug(f"Total number of EF UNF Knowles-UNFCCC: {total_unf_ccc}")
    logger.debug(f"Total number of EF UNF Knowles-UNFCC: {total_unf_cc}")
    logger.debug(f"Total number of EF UNF Knowles - UNFCC: {total_unf_kcc}")
    logger.debug(f"Total number of EF OWID: {total_owid}")
    logger.debug(f"Total number of EF KTIS: {total_ktis}")

    logger.debug("-------------------------------- The above from mongodb, below from mysql --------------------------------------")

    del cal_approches
    del total
    del total_us
    del total_us_epa
    del total_us_us
    del total_us_kepa
    del total_us_k_epa
    del total_tw
    del total_uk
    del total_ghg
    del total_ipcc
    del total_my
    del total_th
    del total_unf
    del total_unf_ccc
    del total_unf_cc
    del total_unf_kcc
    del total_owid
    del total_ktis

    del us_combination
    del tw_combination
    del uk_combination
    del ghg_combination 
    del ipcc_combination
    del my_combination
    del th_combination
    del unf_combination
    del owid_combination
    del ktis_combination

    total = 0
    total_us = 0
    total_us_epa = 0
    total_us_us = 0
    total_us_kepa = 0
    total_us_k_epa = 0
    total_tw = 0
    total_uk = 0
    total_ghg = 0
    total_ipcc = 0
    total_my = 0
    total_th = 0
    total_unf = 0
    total_unf_ccc = 0
    total_unf_cc = 0
    total_unf_kcc = 0
    total_owid = 0
    total_ktis = 0

    us_combination = [1, 10, 12, 16]
    tw_combination = [2, 6, 19]
    uk_combination = [3]
    ghg_combination = [4, 9]
    ipcc_combination = [5, 8]
    my_combination = [7]
    th_combination = [11]
    unf_combination = [13, 14, 17]
    owid_combination = [15]
    ktis_combination = [18]

    with conn.cursor() as cursor:
        sqlCommand = f"""
            SELECT sourceId FROM mgmtCarbon.EF;
        """
        cursor.execute(sqlCommand)
        for (sourceId, ) in cursor.fetchall():
            total+=1
            if sourceId in us_combination:
                total_us += 1
                if sourceId == 1:
                    total_us_epa += 1
                if sourceId == 10:
                    total_us_us += 1
                if sourceId == 12:
                    total_us_kepa += 1
                if sourceId == 16:
                    total_us_k_epa += 1
            if sourceId in tw_combination:
                total_tw += 1
                
            if sourceId in uk_combination:
                total_uk += 1
            if sourceId in ghg_combination:
                total_ghg += 1
            if sourceId in ipcc_combination:
                total_ipcc += 1
            if sourceId in my_combination:
                total_my += 1
            if sourceId in th_combination:
                total_th += 1
            if sourceId in unf_combination:
                total_unf += 1
                if sourceId == 13:
                    total_unf_ccc += 1
                if sourceId == 14:
                    total_unf_cc += 1
                if sourceId == 17:
                    total_unf_kcc += 1
            if sourceId in owid_combination:
                total_owid += 1
            if sourceId in ktis_combination:
                total_ktis += 1

    logger.debug(f"Total number of EF: {total}")
    logger.debug(f"Total number of EF USA: {total_us}")
    logger.debug(f"Total number of EF USA EPA: {total_us_epa}")
    logger.debug(f"Total number of EF USA US EPA-SupplyChain-USD2021: {total_us_us}")
    logger.debug(f"Total number of EF USA Knowles-EPA: {total_us_kepa}")
    logger.debug(f"Total number of EF USA Knowles - EPA: {total_us_k_epa}")
    logger.debug(f"Total number of EF TW: {total_tw}")
    logger.debug(f"Total number of EF UK: {total_uk}")
    logger.debug(f"Total number of EF GHG: {total_ghg}")
    logger.debug(f"Total number of EF IPCC: {total_ipcc}")
    logger.debug(f"Total number of EF MY: {total_my}")
    logger.debug(f"Total number of EF TH: {total_th}")
    logger.debug(f"Total number of EF UNF: {total_unf}")
    logger.debug(f"Total number of EF UNF Knowles-UNFCCC: {total_unf_ccc}")
    logger.debug(f"Total number of EF UNF Knowles-UNFCC: {total_unf_cc}")
    logger.debug(f"Total number of EF UNF Knowles - UNFCC: {total_unf_kcc}")
    logger.debug(f"Total number of EF OWID: {total_owid}")
    logger.debug(f"Total number of EF KTIS: {total_ktis}")

    conn.close()
    client.close()