#!/bin/bash
# Program:
# History:
# 2020/06/19 dwyang  更新為mgmt 保留前x天資料，其餘搬移+刪除
# 2020/08/03 dwyang  iotdata新規則
#                    schemas iotdata+年  ==> iotdata2020 
#                    tables name+_月 ==> ain_01
# 2020/08/03 dwyang  保留前x天資料，檢查沒問題後，刪除iotmgmt資料

PATH=~/bin:/usr/sbin:$PATH

if [ "${1}" == "" ]; then
	echo "請輸入保留多少日的資料，例如昨天為1，前天為2"
	exit 0
fi

keepDayNum=${1}
			      #<
if [ $keepDayNum -lt 1 ]; then
	echo "請輸入保留多少日的資料，例如昨天為1，前天為2"
	echo "[ERROR]至少大於等於1"
	exit 0
fi


programStTime=$(date "+%Y-%m-%d %H:%M:%S")
echo "$programStTime Start Program: DB Clean Data Platform"

day=$(date "+%d" --date="-$keepDayNum day")
dayNum=$((10#$day))

if [ $dayNum == 1 ]; then

	#如果資料結束時間是每個月1號
	year=$(date "+%Y" --date="-1 month")
	month=$(date "+%m" --date="-1 month")
	
	startDay=$(date "+%Y-%m-01" --date="-1 month")
	keepDay=$(date "+%Y-%m-%d" --date="-$keepDayNum day")
	
	yearPre=$(date "+%Y" --date="-2 month")
	monthPre=$(date "+%m" --date="-2 month")
	
	startPreDay=$(date "+%Y-%m-01" --date="-2 month")
	keepPreDay=$(date "+%Y-%m-01" --date="-1 month")
	
elif [ $dayNum == 2 ]; then

	#如果資料結束時間是每個月2號
	year=$(date "+%Y" --date="-$keepDayNum day")
	month=$(date "+%m" --date="-$keepDayNum day")
	
	startDay=$(date "+%Y-%m-%d" --date="-2 day")
	keepDay=$(date "+%Y-%m-%d" --date="-$keepDayNum day")
	
	yearPre=$(date "+%Y" --date="-1 month")
	monthPre=$(date "+%m" --date="-1 month")
	
	startPreDay=$(date "+%Y-%m-01" --date="-1 month")
	keepPreDay=$(date "+%Y-%m-01")
	
else

	year=$(date "+%Y" --date="-$keepDayNum day")
	month=$(date "+%m" --date="-$keepDayNum day")

	startDay=$(date "+%Y-%m-01" --date="-$keepDayNum day")
	keepDay=$(date "+%Y-%m-%d" --date="-$keepDayNum day")
	
	yearPre=$(date "+%Y" --date="-1 month")
	monthPre=$(date "+%m" --date="-1 month")	
	
	startPreDay=$(date "+%Y-%m-01" --date="-1 month")
	keepPreDay=$(date "+%Y-%m-01")
fi

host="127.0.0.1"

schemas="dataETL"

dbClean=$schemas
dbHistory="$schemas$year"

echo "Clean Schemas:$dbClean History Data Schemas:dbHistory"
echo "Clean Time $startDay ~ $keepDay Starting..."

#Table List
table=($(mysql -h ${host} -ss -e"SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables
WHERE table_schema = '$dbHistory'
group by tbl_name;"))


#本月份
whileNum=0
while :
do
	if [ "${table[$whileNum]}" == "" ]; then
		break
	fi

	stDay=$(date +%Y-%m -d "$startDay")
	stDayTimeNum=$(date +%d -d "$startDay")
	stDayTimeNum=$((10#$stDayTimeNum))
	
	endDayTimeNum=$(date +%d -d "$keepDay")
	endDayTimeNum=$((10#$endDayTimeNum))
	
	tbClean=${table[$whileNum]}
	tbHistory="${table[$whileNum]}_$month"
	
	echo "Clean table:$tbClean History table:$tbHistory"
	while :
	do	

		monthNum=$((10#$month))
		#大月 31天
		if [ $monthNum == 1 ] || [ $monthNum == 3 ] || [ $monthNum == 5 ] || [ $monthNum == 7 ] || [ $monthNum == 8 ] || [ $monthNum == 10 ] || [ $monthNum == 12 ]; then

			if [ $endDayTimeNum != 1 ]; then
				if [ $stDayTimeNum == $endDayTimeNum ]; then
					echo " End Time"
					break
				fi
			else
				if [ $stDayTimeNum == 32 ]; then
					echo " End Time"
					break
				fi
			fi
			
			if [ $stDayTimeNum == 31 ]; then
				
				num=$(($monthNum+1))
				runEndDay=$(date +%Y-%m-%d -d "$year-$num-01")

			else
				runEndDayNum=$(($stDayTimeNum+1))
				runEndDay=$(date "+%Y-%m-%d" -d "$stDay-$runEndDayNum")
			fi
		#2月
		elif [ $monthNum == 2 ]; then
		
			if [ $endDayTimeNum != 1 ]; then
				if [ $stDayTimeNum == $endDayTimeNum ]; then
					echo " End Time"
					break
				fi
			else
				if [ $stDayTimeNum == 29 ]; then
					echo " End Time"
					break
				fi
			fi
			
			#if [ $stDayTimeNum == 29 ]; then
			if [ $stDayTimeNum == 28 ]; then
				

				num=$(($monthNum+1))
				runEndDay=$(date +%Y-%m-%d -d "$year-$num-01")
				
			else
				runEndDayNum=$(($stDayTimeNum+1))
				runEndDay=$(date "+%Y-%m-%d" -d "$stDay-$runEndDayNum")
			fi

		#小月 30天
		else
		
			if [ $endDayTimeNum != 1 ]; then
				if [ $stDayTimeNum == $endDayTimeNum ]; then
					echo " End Time"
					break
				fi
			else
				if [ $stDayTimeNum == 31 ]; then
					echo " End Time"
					break
				fi
			fi
			
			if [ $stDayTimeNum == 30 ]; then
				
				num=$(($monthNum+1))
				runEndDay=$(date +%Y-%m-%d -d "$year-$num-01")
			else
				runEndDayNum=$(($stDayTimeNum+1))
				runEndDay=$(date "+%Y-%m-%d" -d "$stDay-$runEndDayNum")
			fi
		fi
		
		runStDay=$(date "+%Y-%m-%d" -d "$stDay-$stDayTimeNum")
		
		echo "$(date "+%Y-%m-%d %H:%M:%S") Run data day:$runStDay~$runEndDay ; $stDayTimeNum --> $endDayTimeNum"


		tbCleanNum=0
		tbCleanNum=($(mysql -h ${host} -D$dbClean -ss -e"SELECT count(*)
		FROM	
			(SELECT *
				FROM	
					$tbClean
				WHERE
					ts >='$runStDay 00:00' and
					ts < '$runEndDay 00:00' 
			)as a
		"))
		
		printf "   Clean Table:   %20s %10d\n" $tbClean $tbCleanNum
		
		tbHistoryNum=0
		tbHistoryNum=($(mysql -h ${host} -D$dbHistory -ss -e"SELECT count(*)
		FROM	
			$tbHistory
		where 	
			ts >= '$runStDay 00:00' and
			ts <  '$runEndDay 00:00' 
		"))
		
		printf "   History Table: %20s %10d\n" $tbHistory $tbHistoryNum
		
		if [ $tbCleanNum == $tbHistoryNum ] && [ $tbCleanNum != 0 ] &&  [ $tbHistoryNum != 0 ]; then
			#delete while by hours
			
			hoursNum=0
			while :
			do	

				if [ $hoursNum == 24 ]; then
					break
				elif [ $hoursNum == 23 ]; then
				
					deleteData=0
					while :
					do	
						deleteData=($(mysql -h ${host} -D$dbClean -ss -e"
						SELECT 
						  count(*) 
						from 
						    $tbClean 
						where 
							ts>='$runStDay $hoursNum:00:00' and 
							ts<'$runEndDay 00:00:00';
						"))
						
						if [ $deleteData == 0 ]; then
							break
						fi
						
						echo "$deleteData delete from $dbClean.$tbClean where ts>='$runStDay $hoursNum:00:00' and ts<'$runEndDay 00:00:00' limit 100;"
						mysql -h ${host} -D$dbClean -ss -e"delete from $dbClean.$tbClean where ts>='$runStDay $hoursNum:00:00' and ts<'$runEndDay 00:00:00' limit 100;"
						
					 done

				else
					hoursEndNum=$(($hoursNum+1))
					
					deleteData=0
					while :
					do	
						
						deleteData=($(mysql -h ${host} -D$dbClean -ss -e"
						SELECT 
						  count(*) 
						from 
						    $tbClean 
						where  
						  ts>='$runStDay $hoursNum:00:00' and 
						  ts<'$runStDay $hoursEndNum:00:00';
						"))
						
						if [ $deleteData == 0 ]; then
							break
						fi
						
						echo "$deleteData delete from $dbClean.$tbClean where ts>='$runStDay $hoursNum:00:00' and ts<'$runStDay $hoursEndNum:00:00' limit 100;"
						mysql -h ${host} -D$dbClean -ss -e"delete from $dbClean.$tbClean where ts>='$runStDay $hoursNum:00:00' and ts<'$runStDay $hoursEndNum:00:00' limit 100;"
						
					done
										
				fi
				hoursNum=$(($hoursNum+1))
			done
		elif [ $tbCleanNum == 0 ] && [ $tbHistoryNum -gt 0 ]; then
		
			echo "------------------Next------------------"
			
		elif [ $tbCleanNum == 0 ] && [ $tbHistoryNum == 0 ]; then
			echo "------------------Next------------------"
			
		else
			echo " [ERROR] 資料數量不對稱"
			echo "------------------Next------------------"
		fi
		
		stDayTimeNum=$(($stDayTimeNum+1))
		
	done

	whileNum=$(($whileNum+1))
done

programEndTime=$(date "+%Y-%m-%d %H:%M:%S")

st="$(date +%s -d "$programStTime")"
end="$(date +%s -d "$programEndTime")"

sec=$(($end-$st)) 

echo "End Program Run Time $programStTime ~ $programEndTime  花費:$sec"

exit 0