import pymysql
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def connectDB():
	try:
		conn=pymysql.connect(
			#host='192.168.1.62',
			host='127.0.0.1',
			#user='ecoetl',
			#password='ECO4etl'
            read_default_file = '~/.my.cnf'
		)
		print("----- Connection Succeed -----")
		return conn
	except Exception as ex:
		print(f"[Error]: {str(ex)}")
	return None

def delete(table, ts, sId, name):
	delete_cursor=conn.cursor()
	delete_sql=f" delete from `dataPlatform`.`{table}` where ts='{ts}' and siteId={sId} and name='{name}'"
	#print(delete_sql)
	delete_cursor.execute(delete_sql)
	conn.commit()

programStartTime = datetime.now()
nowTime = datetime.now()

print(f"----- Now: {nowTime} -----")

#parameter
parser = argparse.ArgumentParser()
parser.add_argument('day', type=int)
keptDay = parser.parse_args().day

#date range
date=(nowTime-timedelta(days=keptDay))#.strftime('%Y-%m-%d 00:00:00')

if date.day==1:
	year=(date-timedelta(days=1)).year
	month=(date-timedelta(days=1)).month
	mon=datetime(year, month, 1).strftime('%m')
	st_date=datetime(year, month, 1).strftime('%Y-%m-%d 00:00:00')
	et_date=date.strftime('%Y-%m-%d 00:00:00')
	
else:
	year=date.year
	mon=date.strftime('%m')
	st_date=date.strftime('%Y-%m-01 00:00:00')
	et_date=date.strftime('%Y-%m-%d 00:00:00')

conn=connectDB()
print(f"from {st_date} to {et_date}")

#table
table_cursor=conn.cursor()
table_sql="select tableDesc from mgmtETL.NameList where gatewayId>0 and protocol is NOT NULL group by tableDesc"
table_cursor.execute(table_sql)

table=[]
for i in table_cursor:
	table.append(i[0])
print(table)

index=0
while index<len(table):
	
	print(f"---------- {table[index]} ----------")
	#data fetched
	dpf_cursor=conn.cursor()
	sqlCommand=f"select ts, siteId, name from dataPlatform.{table[index]} where ts>=\'{st_date}\' and ts<\'{et_date}\'"
	#print(sqlCommand)
	dpf_cursor.execute(sqlCommand)
	
	#show msg if no data
	if dpf_cursor.rowcount==0:
		print(f" {table[index]} has no data during the time. PASS!!!")
		index+=1
		continue
	
	print(f" Processing...")
	
	row=0
	delete_row=0
	move_row=0
	
	for data in dpf_cursor:
		ts=data[0]
		sId=data[1]
		name=data[2]
		
		dpfyearly_cursor=conn.cursor()
		sqlCommand=f"select * from dataPlatform{year}.{table[index]}_{mon} where ts='{ts}' and siteId={sId} and name='{name}'"
		#print(sqlCommand)
		dpfyearly_cursor.execute(sqlCommand)
		data=dpfyearly_cursor.fetchone()
		
		if data is None:
			print(f" [Error]:{sId} {name} doesn't exit in dataPlatform{year}.{table[index]}_{mon}!!!")
			with open(f"./error_{table[index]}.log", 'a') as f:
				f.write(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
				f.write(f"---------- {sId} {name} ----------\n")
				f.write(f" [Error]:{sId} {name} at {ts} doesn't exit in dataPlatform{year}.{table[index]}_{mon}!!!\n")
			
			#replace data
			move_cursor=conn.cursor
			move_sql=f" replace into `dataPlatform{year}`.`{table[index]}_{mon}` select * from dataPlatform.{table[index]} where ts={ts} and siteId={sId} and name={name}"
			#print(move_sql)
			move_cursor.execute(move_sql)
			conn.commit()
			
			#delete data
			delete(table[index], ts, sId, name)
			
			move_row+=1
		else:
			#print(" deleting ...")
			delete(table[index], ts, sId, name)
			delete_row+=1
		
		row+=1
	
	print(f" total {row} rows fetched")
	print(f" {move_row} rows in {year}/{mon} moved")
	print(f" {delete_row} rows deleted in {year}/{mon}")
	
	index+=1

conn.close()
programEndTime=datetime.now()
print(f"----- Connection Closed ----- took:{(programEndTime-programStartTime).seconds}s")
