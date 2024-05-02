import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def connectDB():
	try:
		conn=pymysql.connect(
			host='127.0.0.1',
			#host='192.168.1.62',
			#user='ecoetl',
			#password='ECO4etl'
            read_default_file = '~/.my.cnf'
		)
		print("----- Connection Succeed -----")
		return conn
	except Exception as ex:
		print(f"[ERROR]: {str(ex)}")
	return None

programStartTime = datetime.now()
nowTime = datetime.now()##(2021, 1, 1)
day = nowTime.day
#print(day)
if day==1:
	year=(nowTime-relativedelta(months=1)).year
	mon=(nowTime-relativedelta(months=1)).strftime('%m')
	st_date=(nowTime-relativedelta(months=1)).strftime('%Y-%m-01 00:00:00')
else:
	year=nowTime.year
	mon=nowTime.strftime('%m')
	st_date=nowTime.strftime('%Y-%m-01 00:00:00')

print(f"----- Now: {nowTime.strftime('%Y-%m-%d %H:%M:%S')} -----")

conn=connectDB()
table_cursor=conn.cursor()
sqlCommand="select tableDesc from mgmtETL.NameList where gatewayId>0 and protocol is not NULL group by tableDesc"
table_cursor.execute(sqlCommand)

for rows in table_cursor:
	table=rows[0]
	et_date=nowTime.strftime('%Y-%m-%d 00:00:00')
	print(f"Moving {table} now ...")
	
	move_cursor=conn.cursor()
	move_sql=f"Replace into `dataPlatform{year}`.`{table}_{mon}` select * from dataPlatform.{table} where ts>=\'{st_date}' and ts<\'{et_date}\' "
	print(move_sql)
	move_cursor.execute(move_sql)

conn.commit()
print("----- Replacing Succeed -----")
conn.close()
programEndTime=datetime.now()
print(f"----- Connection closed ----- took: {(programEndTime-programStartTime).seconds}s")
