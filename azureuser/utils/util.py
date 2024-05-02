import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
import logging
import datetime
import pymysql
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

def convert_sec(times):
    if times < 60:
        result = f"{round(times)} sec"
    elif times < 3600:      # times < 3600 sec = 60 sec * 60 min = 1 hour
        m = times // 60
        s = round(times - m*60)
        result = f"{m} minutes {s} sec"
    elif times < 86400:     # times < 86400 sec = 3600 sec * 24 hour = 1 day
        h = times // 3600
        s = times - h*3600
        m = s // 60
        s = round(s - m*60)
        result = f"{h} hour {m} minutes {s} sec"
    else:                   # times >= 1 day
        d = times // 86400
        s = times - d*86400
        h = s // 3600
        s = s - h*3600
        m = s // 60
        s = round(s - m*60)
        result = f"{d} day {h} hour {m} minutes {s} sec"
    return result

def hex2dec(value):
    return int(value, 16)

def signed_hex2dec(value):
    logger.debug(value)
    if int(value[0], 16) > 7:
        return int(value, 16) - int('f' * len(value), 16)
    else:
        return int(value, 16)
    
def alert_line(message):
    # 輸入您的 Line Notify 權杖
    token = 'LJxoG1EKMahwhaDkminV3O9crKHDicB9eQi2ETWAtHM'

    # Line Notify 的 API 網址
    url = 'https://notify-api.line.me/api/notify'

    # 設定標頭(header)，包括權杖和訊息
    headers = {
        'Authorization': 'Bearer ' + token,
    }

    # 要傳送的資料
    payload = {
        'message': message,
    }

    # 發送 POST 請求
    requests.post(url, headers=headers, data=payload)

def authenticate(service_file, scopes):
    creds = service_account.Credentials.from_service_account_file(service_file, scopes=scopes)
    return creds

def create_or_get_folder(service, parent_id, folder_path):
    folder_query = f"name='{folder_path}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents"
    folders = service.files().list(q=folder_query, fields='files(id)').execute().get('files', [])

    if not folders:
        # 文件夹不存在，创建它
        folder_metadata = {
            'name': folder_path,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        return folder['id']
    else:
        # 文件夹存在，返回其 ID
        return folders[0]['id']

def upload_file(filename, service_file, scopes, parent_folder_id, filePath='/'):
    creds = authenticate(service_file, scopes)
    service = build('drive', 'v3', credentials=creds)

    # 创建或获取目标文件夹的 ID
    folder_id = create_or_get_folder(service, parent_folder_id, filePath)

    # 打开文件并上传
    try:
        with open(filename, 'rb') as file_content:
            media = MediaIoBaseUpload(file_content, mimetype='application/octet-stream')

            file_metadata = {
                "name": filename,
                "parents": [folder_id]
            }

            file = service.files().create(
                body=file_metadata,
                media_body=media
            ).execute()

        logger.info(f"File '{filename}' uploaded to folder '{filePath}' with ID: {file['id']}")
    except Exception as ex:
        logger.error(f"Upload failed, message: {ex}")

def process_delete(schema:str, module: str, start: datetime.datetime, end: datetime.datetime, cursor: pymysql.cursors.Cursor):
    logger.info("Processing DELETE")
    while True:
        if schema == 'rawData':
            if "API" in module and module != 'RESTAPI':
                delete_sql = f"""
                    DELETE FROM {schema}.{module} WHERE APIts >= '{start}' AND APIts < '{end}' LIMIT 10000;
                """
            elif module == 'milesight':
                delete_sql = f"""
                    DELETE FROM {schema}.{module} WHERE DBts >= '{start}' AND DBts < '{end}' LIMIT 10000;
                """
            else:
                delete_sql = f"""
                    DELETE FROM {schema}.{module} WHERE GWts >= '{start}' AND GWts < '{end}' LIMIT 10000;
                """
        elif schema in ('dataETL', 'dataPlatform', 'iotmgmt'):
            delete_sql = f"""
                DELETE FROM {schema}.{module} WHERE ts >= '{start}' AND ts < '{end}' LIMIT 10000;
            """

        try:
            cursor.execute(delete_sql)
            
        except Exception as ex:
            logger.error(f"Error executing {delete_sql}")
            logger.error(f"Error message: {ex}")
            break
        if cursor.rowcount == 0:
            logger.debug(delete_sql)
            logger.info(f"DELETE {module} from {start} to {end} Succeed!")
            break

        time.sleep(1)

def send_mail(username: str, password: str, from_email: str, to_email: str, subject: str, message: str, html: bool=False):
    logger.info(f"Sending mail to {username}")

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587

    smtp = smtplib.SMTP(smtp_server, smtp_port)
    smtp.starttls()
    smtp.login(username, password)

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['Header-Name'] = "test header name"
    if html == True:
        msg.attach(MIMEText(message, 'html'))
    else:
        msg.attach(MIMEText(message, 'plain'))

    # 發送郵件
    smtp.sendmail(from_email, to_email, msg.as_string())

    # 關閉 SMTP 連接
    smtp.quit()

def getCId(name:str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT cId FROM emissionFactor.mongoCollection
        WHERE collection='{name}'
    """

    try:
        logger.debug(sqlCommand)
        cursor.execute(sqlCommand)

        return cursor.fetchone()[0]
    except Exception as ex:
        logger.error(f"Error executing {sqlCommand}")
        logger.error(f"Error message: {ex}")

        return -10