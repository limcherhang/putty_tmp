#!/bin/bash
# Program:
# History:
# 2020/06/19 dwyang  更新為mgmt 保留前x天資料，其餘搬移+刪除
# 2020/08/03 dwyang  iotdata新規則
#                    schemas iotdata+年  ==> iotdata2020 
#                    tables name+_月 ==> ain_01
# 2020/08/03 dwyang  單純般資料至iotdata，並且補資料也會同時搬動
PATH=~/bin:/usr/sbin:$PATH

programStTime=$(date "+%Y-%m-%d %H:%M:%S")
echo "$programStTime Start Program: DB Data Platform REPLACE Data Platform History"

day=$(date "+%d")

if [ $day == 1 ]; then
#每個月1號，特殊處理
	
	today=$(date "+%Y-%m-%d 00:00")

	year=$(date "+%Y" --date="-1 month")
	month=$(date "+%m" --date="-1 month")
	keepMonth=$(date "+%Y-%m-01 00:00" --date="-1 month")
	
else	
	today=$(date "+%Y-%m-%d 00:00")

	year=$(date "+%Y")
	month=$(date "+%m")
	keepMonth=$(date "+%Y-%m-01 00:00")
fi

host="127.0.0.1"

dbdataPlatform="dataPlatform"
dbdataPlatformYear="dataPlatform$year"

dbProcessPlatform="processPlatform"
dbProcessPlatformYear="processPlatform$year"

echo "Backup(<$today) Up Starting..."

#dataPlatform Table List
table=($(mysql -h ${host} -D$dbdataPlatformYear -ss -e"SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables
WHERE table_schema = '$dbdataPlatformYear'
group by tbl_name;"))

whileNum=0
while :
do
	if [ "${table[$whileNum]}" == "" ]; then
		break
	fi
	
	tbIotmgmt=${table[$whileNum]}
	tbIotdata="${table[$whileNum]}_$month"
	
	echo "dbdataPlatformYear:$dbdataPlatformYear
	REPLACE INTO $tbIotdata SELECT *
	FROM	
		dataPlatform.$tbIotmgmt
	WHERE  
		ts >='$keepMonth' and
		ts < '$today' 
	"
	
	mysql -h ${host} -D$dbdataPlatformYear -ss -e"REPLACE INTO $tbIotdata SELECT *
	FROM	
		dataPlatform.$tbIotmgmt
	WHERE  
		ts >='$keepMonth' and
		ts < '$today' 
	"
	
	# 之前月份(補資料查詢)
	monthNum=1
	
	while :
	do
		monthCheck=$(date +%m -d "$year-$monthNum-01")
		#echo "$monthCheck == $month"
		if [ $monthCheck == $month ]; then
			break
		fi
		
		stMonth=$(date "+%Y-%m-%d %H:%M" -d "$year-$monthNum-01 00:00")
		monthNum=$(($monthNum+1))
		endMonth=$(date "+%Y-%m-%d %H:%M" -d "$year-$monthNum-01 00:00")
		
		dataCheck=0
		dataCheck=($(mysql -h ${host} -D$dbdataPlatform -ss -e"SELECT count(*) 
		FROM(
			SELECT * FROM dataPlatform.$tbIotmgmt
			WHERE 
			ts >='$stMonth' and
			ts < '$endMonth' 
			limit 1
		) as a
		"))
		
		if [ $dataCheck == 1 ]; then
			echo "$stMonth ~ $endMonth $dataCheck"
			
			tbIotdataAgain="${table[$whileNum]}_$monthCheck"
			
			echo "REPLACE INTO $tbIotdataAgain SELECT *
			FROM	
				dataPlatform.$tbIotmgmt
			WHERE  
				ts >='$stMonth' and
				ts < '$endMonth' 
			"
			
			mysql -h ${host} -D$dbdataPlatformYear -ss -e"REPLACE INTO $tbIotdataAgain SELECT *
			FROM	
				dataPlatform.$tbIotmgmt
			WHERE  
				ts >='$stMonth' and
				ts < '$endMonth' 
			"
		fi
		
	done
	
	
	#前一年資料 Year
	preYearTime=$(date "+%Y-01-01 00:00")
	
	preDataCheck=0
	preDataCheck=($(mysql -h ${host} -D$dbdataPlatform -ss -e"SELECT count(*) 
			FROM(
				SELECT * FROM dataPlatform.$tbIotmgmt
				WHERE
				ts < '$preYearTime' 
				limit 1
			) as a
			"))
			
	if [ $preDataCheck == 1 ]; then
	
		preYear=$(date "+%Y" --date="-1 year")
		preYearDBdata="iotdata$preYear"
		preMonthNum=12
		
		while :
		do
			monthCheck=$(date +%m -d "$preYear-$preMonthNum-01")
			#echo "pre Year Data:$preYear-$monthCheck"
			
			stMonth=$(date "+%Y-%m-%d %H:%M" -d "$preYear-$preMonthNum-01 00:00")
			
			if [ $preMonthNum == 12 ]; then
				endMonth=$(date "+%Y-%m-%d %H:%M" -d "$year-01-01 00:00")
			else
				endMonthNum=$(($preMonthNum+1))
				endMonth=$(date "+%Y-%m-%d %H:%M" -d "$preYear-$endMonthNum-01 00:00")
			fi

			dataCheck=0
			dataCheck=($(mysql -h ${host} -D$dbdataPlatform -ss -e"SELECT count(*) 
			FROM(
				SELECT * FROM dataPlatform.$tbIotmgmt
				WHERE 
				ts >='$stMonth' and
				ts < '$endMonth' 
				limit 1
			) as a
			"))
			
			if [ $dataCheck == 1 ]; then
				echo "*pre Year($preYearDBdata) $stMonth ~ $endMonth $dataCheck"
				
				tbIotdataAgain="${table[$whileNum]}_$monthCheck"
				
				echo "dbdataPlatformYear:$preYearDBdata
					REPLACE INTO $tbIotdataAgain SELECT *
				FROM	
					dataPlatform.$tbIotmgmt
				WHERE  
					ts >='$stMonth' and
					ts < '$endMonth' 
				"
				
				mysql -h ${host} -D$preYearDBdata -ss -e"REPLACE INTO $tbIotdataAgain SELECT *
				FROM	
					dataPlatform.$tbIotmgmt
				WHERE  
					ts >='$stMonth' and
					ts < '$endMonth' 
				"
			fi
			
			preMonthNum=$(($preMonthNum-1))
			
			if [ $preMonthNum == 0 ]; then
				break
			fi
			
		done
	fi
	
	whileNum=$(($whileNum+1))
done

#processPlatform Table List
table=($(mysql -h ${host} -D$dbProcessPlatformYear -ss -e"SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables
WHERE table_schema = '$dbProcessPlatformYear'
group by tbl_name;"))

whileNum=0
while :
do
	if [ "${table[$whileNum]}" == "" ]; then
		break
	fi
	
	tbIotmgmt=${table[$whileNum]}
	tbIotdata="${table[$whileNum]}_$month"
	
	echo "dbProcessPlatformYear:$dbProcessPlatformYear
	REPLACE INTO $tbIotdata SELECT *
	FROM	
		processPlatform.$tbIotmgmt
	WHERE  
		ts >='$keepMonth' and
		ts < '$today' 
	"
	
	mysql -h ${host} -D$dbProcessPlatformYear -ss -e"REPLACE INTO $tbIotdata SELECT *
	FROM	
		processPlatform.$tbIotmgmt
	WHERE  
		ts >='$keepMonth' and
		ts < '$today' 
	"

	whileNum=$(($whileNum+1))
done

programEndTime=$(date "+%Y-%m-%d %H:%M:%S")

st="$(date +%s -d "$programStTime")"
end="$(date +%s -d "$programEndTime")"

sec=$(($end-$st)) 

echo "End Program Run Time $programStTime ~ $programEndTime  花費:$sec"
exit 0
