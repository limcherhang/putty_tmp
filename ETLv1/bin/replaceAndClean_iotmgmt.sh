#!/bin/bash
# Program:
# History:
# 自動確認iotmgmt最早的資料
# 只保留iotmgmt至前一天的資料，其他都replace into iotdata20XX
# 此program以程式使用當下年份的歷史資料table為參考基準
# bash .sh 後須加 
# 1.serverName，如:1.34、1.45、1.52、1.62、1.41 
# 2.每筆刪除筆數，如:1~10000
# 3.刪除間隔，如:1、0

PATH=~/bin:/usr/sbin:$PATH

programStTime=$(date "+%Y-%m-%d %H:%M:%S")

if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ]; then
	echo "請輸入:"
	echo "1.serverName，如:1.41、1.61"
	echo "2.每筆刪除筆數，如:1~10000"
	echo "3.刪除間隔，如:0、1"
	echo " "
	echo "bash replaceAndClean_iotmgmt.sh"
	exit 0
else
	programStTime=$(date "+%Y-%m-%d %H:%M:%S")
	
	echo "$programStTime Run Replace And Clean"
	echo "  Scheram : rawData"
	echo "  IP : $1"
	echo "  DELETE : $2"
	echo "  DELAY TIME : $3"
fi

host="127.0.0.1"
echo "Host=$host"
dbmgmt="iotmgmt"
dbdata="iotdata"

dataStoreDate=$(date -d "-1 days" +"%Y-%m-%d") #資料保留至昨天
dsDate=`date -d "$dataStoreDate" +%s`
nowYear=${dataStoreDate:0:4} #取年份
nowMonth=${dataStoreDate:5:2} #取月份
yesterday=${dataStoreDate:8:2} #取昨天
today=$(date +"%d") #今天日期

dataDeleteRange=$2
sleepTime=$3

# 從iotdata$nowYear取得所有table
tableBuffer=( 
	$(mysql -h $host -ss -e"
	show tables from iotdata$nowYear;")
)
tableCount=0
preTable=""

dataTransfer () {
	
	earliestDataDate=(
		$(mysql -h $host -ss -e"
		select 
			ts 
		from 
			$dbmgmt.${table}
		where 
			ts >= '2000-01-01' and 
			receivedSync >= '2000-01-01' 
		order by ts asc limit 1")
	)
	
	edDate=`date -d "$earliestDataDate" +%s`
	echo -e "\n$dbmgmt.${table}"
	
	year=${earliestDataDate:0:4} #取年份
	month=${earliestDataDate:5:2} #取月份

	if [ -z "$earliestDataDate" ]; then
		echo "Data null, please check again!!"
		compare=0
		
	elif [ "$edDate" -ge "$dsDate" ]; then
		echo "EarliestData: $earliestDataDate"
		echo "Data is Correct !!"
		compare=0
		
	else

		echo "Earliest Data is on $earliestDataDate"
		
		dataKeepDate=""
		# 設定資料保留日期
		if [ "$year" -eq "$nowYear" ] && [ "$month" -eq "$nowMonth" ]; then
			dataKeepDate="$year-$month-$yesterday"
		else
			if [ "$month" -eq 12 ]; then
				dataKeepDate="$(($year+1))-01-01"
			else
				dataKeepDate="$year-$((10#$month+1))-01"
			fi
		fi
		
		# 12月至隔年1/1
		if [ "$month" -eq 12 ]; then
			# 本月資料
			if [ "$year" -eq "$nowYear" ] && [ "$month" -eq "$nowMonth" ]; then
				# 搬運舊資料
				echo -e "  replace into 
				\r    $dbdata$year.${table}_$month
				\r  select * from
				\r    $dbmgmt.${table}
				\r  where
				\r    ts >= '$earliestDataDate' and
				\r    ts <  '$year-$month-$today'\n"
				mysql -h $host -ss -e"
				replace into 
					$dbdata$year.${table}_$month 
				select * from 
					$dbmgmt.${table} 
				where
					ts >= '$earliestDataDate' and
					ts <  '$year-$month-$today';"
					
				# 確認iotmgmt&iotdata資料筆數
				echo "Counting iotdata and iotmgmt..."
				mgmtNum=0
				dataNum=0
				mgmtNum=($(mysql -h $host -ss -e"
				select count(*) from (
					select * from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$year-$month-$yesterday' and 
						ieee is not null
					group by ts, ieee
					)as a
				"))
				
				dataNum=($(mysql -h $host -ss -e"
				select count(*) from (
					select count(*) from
						$dbmgmt.${table} as a,
						$dbdata$year.${table}_$month as b
					where
						a.ts = b.ts and
						a.ieee = b.ieee and
						a.ts >= '$earliestDataDate' and
						a.ts <  '$year-$month-$yesterday' and 
						a.ieee is not null
					group by a.ts, a.ieee
					)as a
				"))
				
				
				echo "mgmtNum = $mgmtNum"
				echo "dataNum = $dataNum"
				# 刪除舊資料
				# echo -e "\nReady to delete data from iotmgmt..."
				# sleep 5
				delNum=0
				if [ "$dataNum" -eq "$mgmtNum" ] && [ "$dataNum" -ne 0 ] && [ "$mgmtNum" -ne 0 ]; then
					# 刪除資料總數為iotmgmt.table時間內所有筆數而非group by的筆數
					delCnt=($(mysql -h $host -ss -e"
					select count(*) from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$dataKeepDate'
					"))
					echo "delCnt  = $delCnt"
					echo -e "\nReady to delete data from iotmgmt..."
					sleep 5
					
					while [ "$delNum" -lt "$delCnt" ]
					do
						echo -e "\ndelNum from $delNum"
						echo -e "  delete from
						\r    $dbmgmt.${table}
						\r  where
						\r    ts >= '$earliestDataDate' and
						\r    ts <  '$year-$month-$yesterday' 
						\r	limit $dataDeleteRange"
						mysql -h $host -ss -e"
						delete from
							$dbmgmt.${table}
						where
							ts >= '$earliestDataDate' and
							ts <  '$year-$month-$yesterday'
						limit $dataDeleteRange;"
						delNum=$(($delNum+$dataDeleteRange))
						sleep $sleepTime
					done
				fi
			else # 非本月資料
				# 搬運舊資料
				echo -e "  replace into 
				\r    $dbdata$year.${table}_$month
				\r  select * from 
				\r    $dbmgmt.${table}
				\r  where 
				\r    ts >= '$earliestDataDate' and
				\r    ts <  '$(($year+1))-01-01'\n"
				mysql -h $host -ss -e"
				replace into 
				    $dbdata$year.${table}_$month
				select * from 
				    $dbmgmt.${table}
				where 
				    ts >= '$earliestDataDate' and
				    ts <  '$(($year+1))-01-01';"
					
				# 確認iotmgmt&iotdata資料筆數
				echo "Counting iotdata and iotmgmt..."
				mgmtNum=0
				dataNum=0
				if [ ${table} == "plc" ]; then
					mgmtNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select * from 
							$dbmgmt.${table}
						where	
							ts >= '$earliestDataDate' and
							ts <  '$(($year+1))-01-01'
						group by ts
						)as a
					"))
					
					dataNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select count(*) from
							$dbmgmt.${table} as a,
							$dbdata$year.${table}_$month as b
						where
							a.ts = b.ts and
							a.ts >= '$earliestDataDate' and
							a.ts <  '$(($year+1))-01-01'
						group by a.ts
						)as a
					"))
				else
					mgmtNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select * from 
							$dbmgmt.${table}
						where	
							ts >= '$earliestDataDate' and
							ts <  '$(($year+1))-01-01' and 
							ieee is not null
						group by ts, ieee
						)as a
					"))
					
					dataNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select count(*) from
							$dbmgmt.${table} as a,
							$dbdata$year.${table}_$month as b
						where
							a.ts = b.ts and
							a.ieee = b.ieee and
							a.ts >= '$earliestDataDate' and
							a.ts <  '$(($year+1))-01-01' and 
							a.ieee is not null
						group by a.ts, a.ieee
						)as a
					"))
				fi
				
				echo "mgmtNum = $mgmtNum"
				echo "dataNum = $dataNum"
				
				# 刪除舊資料
				# echo -e "\nReady to delete data from iotmgmt..."
				# sleep 5
				delNum=0
				echo "delCnt  = $delCnt"
				if [ "$dataNum" -eq "$mgmtNum" ]; then
					# 刪除資料總數為iotmgmt.table時間內所有筆數而非group by的筆數
					delCnt=($(mysql -h $host -ss -e"
					select count(*) from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$dataKeepDate'
					"))
					echo "delCnt  = $delCnt"
					echo -e "\nReady to delete data from iotmgmt..."
					sleep 5
					
					while [ "$delNum" -lt "$delCnt" ]
					do
						echo -e "\ndelNum from $delNum"
						echo -e "  delete from 
						\r    $dbmgmt.${table}
						\r  where 
						\r    ts >= '$earliestDataDate' and
						\r    ts <  '$(($year+1))-01-01'
						\r	limit $dataDeleteRange"
						mysql -h $host -ss -e"
						delete from 
							$dbmgmt.${table}
						where 
							ts >= '$earliestDataDate' and 
							ts <  '$(($year+1))-01-01' 
						limit $dataDeleteRange;"
						delNum=$(($delNum+$dataDeleteRange))
						sleep $sleepTime
					done
				else 
					echo "3 delCnt  = $delCnt"
				fi
			fi
		else
			# 本月資料
			if [ "$year" -eq "$nowYear" ] && [ "$month" -eq "$nowMonth" ]; then
				# 搬運舊資料
				echo -e "  replace into 
				\r    $dbdata$year.${table}_$month
				\r  select * from 
				\r    $dbmgmt.${table}
				\r  where 
				\r    ts >= '$earliestDataDate' and 
				\r    ts <  '$year-$month-$today'\n"
				mysql -h $host -ss -e"
				replace into 
				    $dbdata$year.${table}_$month
				select * from 
				    $dbmgmt.${table}
				where 
				    ts >= '$earliestDataDate' and 
				    ts <  '$year-$month-$today';"
					
				# 確認iotmgmt&iotdata資料筆數
				echo "Counting iotdata and iotmgmt..."
				mgmtNum=0
				dataNum=0

				mgmtNum=($(mysql -h $host -ss -e"
				select count(*) from (
					select * from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$year-$month-$yesterday' and 
						ieee is not null
					group by ts, ieee
					)as a
				"))
				
				dataNum=($(mysql -h $host -ss -e"
				select count(*) from (
					select count(*) from
						$dbmgmt.${table} as a,
						$dbdata$year.${table}_$month as b
					where
						a.ts = b.ts and
						a.ieee = b.ieee and
						a.ts >= '$earliestDataDate' and
						a.ts <  '$year-$month-$yesterday' and 
						a.ieee is not null
					group by a.ts, a.ieee
					)as a
				"))
				
				
				echo "mgmtNum = $mgmtNum"
				echo "dataNum = $dataNum"
				
				# 刪除舊資料
				# echo -e "\nReady to delete data from iotmgmt..."
				# sleep 5
				delNum=0
				if [ "$dataNum" -eq "$mgmtNum" ]; then
					# 刪除資料總數為iotmgmt.table時間內所有筆數而非group by的筆數
					delCnt=($(mysql -h $host -ss -e"
					select count(*) from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$dataKeepDate'
					"))
					echo "delCnt  = $delCnt"
					echo -e "\nReady to delete data from iotmgmt..."
					sleep 5
					
					while [ "$delNum" -lt "$delCnt" ]
					do
						echo -e "\ndelNum from $delNum"
						echo -e "  delete from 
						\r    $dbmgmt.${table}
						\r  where 
						\r    ts >= '$earliestDataDate' and 
						\r    ts <  '$year-$month-$yesterday'
						\r	limit $dataDeleteRange"
						mysql -h $host -ss -e"
						delete from 
							$dbmgmt.${table}
						where 
							ts >= '$earliestDataDate' and 
							ts <  '$year-$month-$yesterday' 
						limit $dataDeleteRange;"
						delNum=$(($delNum+$dataDeleteRange))
						sleep $sleepTime
					done
				fi
			else # 非本月資料
				# 搬運舊資料
				echo -e "  replace into 
				\r    $dbdata$year.${table}_$month
				\r  select * from 
				\r    $dbmgmt.${table}
				\r  where 
				\r    ts >= '$earliestDataDate' and 
				\r    ts <  '$year-$((10#$month+1))-01'\n"
				mysql -h $host -ss -e"
				replace into 
				    $dbdata$year.${table}_$month
				select * from 
				    $dbmgmt.${table}
				where 
				    ts >= '$earliestDataDate' and 
				    ts <  '$year-$((10#$month+1))-01';"
					
				# 確認iotmgmt&iotdata資料筆數
				echo "Counting iotdata and iotmgmt..."
				mgmtNum=0
				dataNum=0
				if [ ${table} == "plc" ]; then
					mgmtNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select * from 
							$dbmgmt.${table}
						where	
							ts >= '$earliestDataDate' and
							ts <  '$year-$((10#$month+1))-01'
						group by ts
						)as a
					"))
					
					dataNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select count(*) from
							$dbmgmt.${table} as a,
							$dbdata$year.${table}_$month as b
						where
							a.ts = b.ts and
							a.ts >= '$earliestDataDate' and
							a.ts <  '$year-$((10#$month+1))-01'
						group by a.ts
						)as a
					"))
				else
					mgmtNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select * from 
							$dbmgmt.${table}
						where	
							ts >= '$earliestDataDate' and
							ts <  '$year-$((10#$month+1))-01' and 
							ieee is not null
						group by ts, ieee
						)as a
					"))
					
					dataNum=($(mysql -h $host -ss -e"
					select count(*) from (
						select count(*) from
							$dbmgmt.${table} as a,
							$dbdata$year.${table}_$month as b
						where
							a.ts = b.ts and
							a.ieee = b.ieee and
							a.ts >= '$earliestDataDate' and
							a.ts <  '$year-$((10#$month+1))-01' and 
							a.ieee is not null
						group by a.ts, a.ieee
						)as a
					"))
				fi
					
				echo "mgmtNum = $mgmtNum"
				echo "dataNum = $dataNum"	
				
				# 刪除舊資料
				# echo -e "\nReady to delete data from iotmgmt..."
				# sleep 5
				delNum=0
				if [ "$dataNum" -eq "$mgmtNum" ]; then
					# 刪除資料總數為iotmgmt.table時間內所有筆數而非group by的筆數
					delCnt=($(mysql -h $host -ss -e"
					select count(*) from 
						$dbmgmt.${table}
					where	
						ts >= '$earliestDataDate' and
						ts <  '$dataKeepDate'
					"))
					echo "delCnt  = $delCnt"
					echo -e "\nReady to delete data from iotmgmt..."
					sleep 5
					
					while [ "$delNum" -lt "$delCnt" ]
					do
						echo -e "\ndelNum from $delNum"
						echo -e "  delete from 
						\r    $dbmgmt.${table}
						\r  where 
						\r    ts >= '$earliestDataDate' and 
						\r    ts <  '$year-$((10#$month+1))-01' 
						\r  limit $dataDeleteRange"
						mysql -h $host -ss -e"
						delete from 
							$dbmgmt.${table}
						where 
							ts >= '$earliestDataDate' and 
							ts <  '$year-$((10#$month+1))-01' 
						limit $dataDeleteRange;"
						delNum=$(($delNum+$dataDeleteRange))
						sleep $sleepTime
					done
				fi
			fi	
		fi
		compare=1
	fi
}
	
while :
do
	if [ "${tableBuffer[tableCount]}" == "" ]; then
		break
	fi
	table=${tableBuffer[tableCount]}
	table=`echo "$table" | sed "s/_.*//g"`
	
	# 比較當前table名稱與上一個table名稱
	if [ "$preTable" != "$table" ]; then 
		preTable=$table
		dataTransfer
		case "$compare" in
			0)  #資料未變更
				tableCount=$(($tableCount+1))
				;;
			1)	#資料變更，重新檢查
				tableCount=$(($tableCount+0))
				preTable=""
				;;
		esac
	else
		tableCount=$(($tableCount+1))
	fi
done


programEndTime=$(date "+%Y-%m-%d %H:%M:%S")
echo ""
st="$(date +%s -d "$programStTime")"
end="$(date +%s -d "$programEndTime")"
sec=$(($end-$st))
min=$(($sec/60))
sec=$(($sec%60))
echo "End Program Run Time $programStTime ~ $programEndTime  花費:${min}min ${sec}sec"