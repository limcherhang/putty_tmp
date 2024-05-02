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

schemas="dataETL"

dbSource=$schemas
dbDestination="$schemas$year"

echo " REPLACE Data Time $keepMonth to $today"
echo " REPLACE Data Schemas $dbSource to $dbDestination"


#Table List
table=($(mysql -h ${host} -ss -e"SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables
WHERE table_schema = '$dbDestination'
group by tbl_name;"))

whileNum=0
while :
do
	if [ "${table[$whileNum]}" == "" ]; then
		break
	fi
	
	tbSource=${table[$whileNum]}
	tbDestination="${table[$whileNum]}_$month"
	
	echo " REPLACE Data Table $tbSource to $tbDestination"
	
	echo "
	REPLACE INTO $tbDestination SELECT *
	FROM	
		$dbSource.$tbSource
	WHERE  
		ts >='$keepMonth' and
		ts < '$today' 
	"
	
	mysql -h ${host} -D$dbDestination -ss -e"REPLACE INTO $tbDestination SELECT *
	FROM	
		$dbSource.$tbSource
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
