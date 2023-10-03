#!/bin/bash
#####################################################################################
#
#       Created by: Sharan
#       Date: 2019-Dec-13
#       Description:
#		    This shell script developed for testing the ETL Jobs for Outbound Interfaces
#       Example:
#		      Outbound.sh
#
#####################################################################################
# Parameters
# ==========
# SOURCENAME          char  Source system to be tested
# JOBNAME        		  char  The ETL job to be tested

BASE_FOLDER="/home/sshuser/Test_Automation/Outbound"
. $BASE_FOLDER/environment_outbound.sh

SOURCENAME=$1
JOBNAME=$2
currentdate=`date +%y/%m/%d-%H:%M:%S`

echo currentdate $currentdate
LINE=$(grep -n "SOURCENAME="$SOURCENAME $BASE_FOLDER/config_outbound.sh | cut -d : -f 1) #Getting the line numbers which are matching with the SOURCENAME  
#echo LINE $LINE
arr=(`echo ${LINE}`); #Assigning matching line numbers to Array 
for i in "${arr[@]}"
do
#echo $i
ENDLINE=(`expr $i + $num`)

#echo ENDLINE $ENDLINE
LIN2=$(sed -n "${i},${ENDLINE}!d;/"JOB_NAME="${JOBNAME}\b/!d;=" config_outbound.sh) #Checking whether the Job Name is present in between i and ENDLINE
#echo LIN2 $LIN2
if [ -n "$LIN2" ]
then
while [ $LIN2 -le $ENDLINE ]
do
#echo LIN22 $LIN2
		
if [ -z "$TESTCASE" ]
then			
TESTCASE=$(sed -n "${LIN2} s/^ *TESTCASE=*//p" config_outbound.sh)
#echo TESTCASE $TESTCASE

elif [ -z "$OBJECTNAME" ]
then			
OBJECTNAME=$(sed -n "${LIN2} s/^ *OBJECT_NAME=*//p" config_outbound.sh)
#echo OBJECT_NAME $OBJECTNAME
     
elif [ -z "$OBJECTLAYER" ]
then
OBJECTLAYER=$(sed -n "${LIN2} s/^ *OBJECT_LAYER*=*//p" config_outbound.sh)
#echo OBJECT_LAYER $OBJECTLAYER

elif [ -z "$INTERFACEID" ]
then
INTERFACEID=$(sed -n "${LIN2} s/^ *INTERFACE_ID*=*//p" config_outbound.sh)
#echo INTERFACE_ID $INTERFACEID

elif [ -z "$SCENARIONAME" ]
then
SCENARIONAME=$(sed -n "${LIN2} s/^ *SCENARIO_NAME*=*//p" config_outbound.sh)
#echo SCENARIO_NAME $SCENARIONAME

elif [ -z "$SCENARIOVERSION" ]
then
SCENARIOVERSION=$(sed -n "${LIN2} s/^ *SCENARIO_VERSION*=*//p" config_outbound.sh)
#echo SCENARIO_VERSION $SCENARIOVERSION

elif [ -z "$INTERFACENAME" ]
then
INTERFACENAME=$(sed -n "${LIN2} s/^ *INTERFACE_NAME*=*//p" config_outbound.sh)
#echo INTERFACE_NAME $INTERFACENAME
	
fi
LIN2=(`expr $LIN2 + 1`)
done
fi
done

echo TESTCASE $TESTCASE
echo OBJECT_NAME $OBJECTNAME
echo OBJECT_LAYER $OBJECTLAYER
echo INTERFACE_ID $INTERFACEID
echo SCENARIO_NAME $SCENARIONAME
echo SCENARIO_VERSION $SCENARIOVERSION
echo INTERFACE_NAME $INTERFACENAME


if [ -z "$SOURCENAME" ] || [ -z "$JOBNAME" ] || [ -z "$TESTCASE" ] || [ -z "$OBJECTNAME" ] || [ -z "$OBJECTLAYER" ] || [ -z "$INTERFACEID" ] || [ -z "$SCENARIONAME" ] || [ -z "$SCENARIOVERSION" ] || [ -z "$INTERFACENAME" ]
then
echo INSUFFICIENT PARAMETERS
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,stage,test_result,comments,execute_date) 
values('$SOURCENAME','$TESTCASE','OUTBOUND','FAILED','Input parameters not sufficient',sysdate); 
exit;
EOF
else

#Rerunnabilty starts

#Getting last successful JOB_RUN_ID
LAST_SUCCESSFUL_RUN=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0; 
exit;
EOF`
echo LAST SUCCESSFUL JOB_RUN_ID $LAST_SUCCESSFUL_RUN

#Checking the need for reprocessing 1 if reprocessing is needed, 0 otherewise
REPROCESS_CHECK_FLAG=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT COUNT(*) FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>(SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0) AND JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME')AND REPROCESS_FLAG=1; 
exit;
EOF`
echo REPROCESS_CHECK_FLAG $REPROCESS_CHECK_FLAG

if [ $REPROCESS_CHECK_FLAG = 1 ] #Reprocessing condition check block
then
#Job to be reprocessed
REPROCESS_JOB=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_RUN_ID FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>(SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0) AND JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND REPROCESS_FLAG=1;
exit;
EOF`
echo REPROCESSING THE JOB $REPROCESS_JOB

date_from=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_FROM FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT JOB_RUN_ID FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>(SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0) AND JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND REPROCESS_FLAG=1); 
exit;
EOF`

date_to=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_TO FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT JOB_RUN_ID FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>(SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0) AND JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND REPROCESS_FLAG=1); 
exit;
EOF`

date_from=$(echo $date_from | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters
date_to=$(echo $date_to | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters

echo FROM_DATE $date_from
echo TO_DATE $date_to

IFS=":"    #Field Seperator
#Loop to get the data from csv
while read f1 f2
do
job=$f1
if [ "$job" == "$JOBNAME" ]    #if statement for job name comparison to take data from the csv file
then
echo JOBNAME: $job
sql=$f2
echo QUERY IS: $sql
echo
echo
fi    #End of if statement for job name comparison to take data from the csv file
done < data.csv
unset IFS
#End of inner loop to get data from csv

sql=$(echo $sql | sed -e "s/'\$date_from'/'$date_from'/g") #Substituting the actual data in the Query
sql=$(echo $sql | sed -e "s/'\$date_to'/'$date_to'/g") #Substituting the actual data in the Query

echo
echo
echo AFTER SUBSTITUTION: $sql

#Getting data from the 3NF/DWDD that is to be populated in the file
if [ "$OBJECTLAYER" = "DW_3NF" ]
then
echo DW_3NF Layer
table_data=`sqlplus -s $TNFDB_CONNECTION <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF`
else
echo DWDD Layer
table_data=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF`
fi
#End of fetching data from 3NF/DWDD

table_data=`echo $table_data|tr -d [:' ']`    #Removing the unwanted spaces from the query result
before_run_date=`date +%Y%m%d%H%M%S`

echo $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME
echo JOB STARTS.............
sh /insights/app/scripts/ODI_SCENARIO_OB.sh $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME


outdirpath=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select OB_TARGET_DIR_PATH from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF`
echo TARGET_DIR $outdirpath

interfacefileid=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select INTERFACE_FILE_ID from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF`
echo INTERFACE_FILE_ID $interfacefileid

jobrunid=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select MAX(JOB_RUN_ID) from FILE_CONTROL where INTERFACE_FILE_ID=$interfacefileid;  
exit;
EOF`
echo JOB_RUN_ID $jobrunid

FILENAME=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select FILE_NAME from FILE_CONTROL where JOB_RUN_ID=$jobrunid;
exit;
EOF`
echo FILE_NAME $FILENAME
FILENAME=`echo $FILENAME|tr -d [:' ']`

file_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select FILE_PROCESSING_STATUS from FILE_CONTROL where FILE_NAME='$FILENAME';
exit;
EOF`
echo FILE_PROCESSING_STATUS $file_status

expected_status=5
echo EXPECTED_STATUS $expected_status

OUT_FILENAME=$FILENAME
OUT_FILENAME=`echo $OUT_FILENAME|tr -d [:' ']`

echo BEFORE_JOBRUN_DATE $before_run_date

filedate=$(echo $(echo $(echo $OUT_FILENAME | rev)|cut -c5-18)| rev)
echo LATEST_FILE_DATE $filedate

echo OUTPUT_FILE_NAME $OUT_FILENAME
echo $outdirpath'/'$OUT_FILENAME

if [ $before_run_date -lt $filedate ]
then
	if [ $(ls $outdirpath/$OUT_FILENAME) ]
	then
		echo File Present
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-FILE_AVAILABILITY_CHECK','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF

#File Control Table checks
		if [ $file_status = 5 ]
		then
			echo Entry created in File Control Table
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF

		elif [ $file_status = 2 ]
		then
			echo File creation failed
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','FAILED','File_Control table entry is 2 ERROR',sysdate,'$INTERFACENAME');
exit;
EOF

		else
			echo Entry not found in File Control Table
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','FAILED','File is Created Entry not Found in FILE_CONTROL table',sysdate,'$INTERFACENAME');
exit;
EOF
		fi
#File Control Table Checks ends

#Data Validation
head=$(cat $outdirpath/$OUT_FILENAME | head -n 1)
file_data=$(cat $outdirpath/$OUT_FILENAME)
file_data=$(echo "$file_data" | tr '","' ' ')
head=$(echo "$head" | tr ',' ' ')
file_data=`echo $file_data|tr -d [:' ']`
head=`echo $head|tr -d [:' ']`
file_data=${file_data//$head}
		if [ "$table_data" == "$file_data" ]
		then
			echo data matching
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-DATA_VALIDATION','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
		else
			echo data mismatch
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-DATA_VALIDATION','OUTBOUND','FAILED','File data doesnot match with the expected data',sysdate,'$INTERFACENAME');
exit;
EOF
		fi
	else
		echo FileName Mismatch
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','Filename Mismatch or file is not created in the output directory',sysdate,'$INTERFACENAME');
exit;
EOF
	fi
#Data Validation Ends

#Extract process control checks
new_date_from=`sqlplus -s $DB_CONNECTION <<EOF
set head off
SELECT EXTRACT_DATE_FROM FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));
exit;
EOF`
echo NEW_DATE_FROM: $new_date_from

new_date_to=`sqlplus -s $DB_CONNECTION <<EOF
set head off
SELECT EXTRACT_DATE_TO FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));
exit;
EOF`
echo NEW_DATE_TO: $new_date_to

old_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT STATUS_FLAG FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=$REPROCESS_JOB;
exit;
EOF`
echo OLD_EXTRACT_STATUS: $old_status

new_date_from=$(echo $new_date_from | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters
new_date_to=$(echo $new_date_to | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters

new_date_from=`echo $new_date_from|tr -d [:' ']`
new_date_to=`echo $new_date_to|tr -d [:' ']`
date_from=`echo $date_from|tr -d [:' ']`
date_to=`echo $date_to|tr -d [:' ']`


new_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT STATUS_FLAG FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=$jobrunid;
exit;
EOF`
echo NEW_EXTRACT_STATUS: $new_status

if [ "$new_date_from" == "$date_from" ] && [ "$new_date_to" == "$date_to" ]
then
  if [ $old_status = 0 ] && [ $new_status = 1 ]
  then
    echo PASSED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $old_status != 0 ] && [ $new_status != 1 ]
  then
    echo  INCORRECT PREVIOUS_RUN_STATUS
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Incorrect run STATUS_FLAG',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $old_status != 0 ]
  then
    echo PREVIOUS_RUN_STATUS: $old_status
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Previous Run STATUS_FLAG ${old_status}',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $new_status != 1 ]
  then
    echo CURRENT_RUN_STATUS: $new_status
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Current Run STATUS_FLAG ${new_status}',sysdate,'$INTERFACENAME');
exit;
EOF
  fi
elif [ "$new_date_from" != "$date_from" ] || [ "$new_date_to" != "$date_to" ]
then
  if [ $old_status = 0 ] && [ $new_status = 1 ]
  then
    echo INCORRECT EXTRACT_DATES
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Incorrect EXTRACT DATES',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $old_status != 0 ] && [ $new_status != 1 ]
  then
    echo INCORRECT EXTRACT_DATES & INCORRECT RUN_STATUS
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Incorrect EXTRACT DATES & Previous run STATUS_FLAG ${old_status} & Current run STATUS_FLAG ${new_status}',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $old_status != 0 ]
  then
    echo INCORRECT EXTRACT_DATES AND PREVIOUS_RUN_STATUS: $old_status
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Incorrect EXTRACT DATES & Previous run STATUS_FLAG ${old_status}',sysdate,'$INTERFACENAME');
exit;
EOF
  elif [ $new_status != 1 ]
  then
    echo INCORRECT EXTRACT_DATES AND CURRENT_RUN_STATUS: $new_status  
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','FAILED','Incorrect EXTRACT DATES & Current run STATUS_FLAG ${new_status}',sysdate,'$INTERFACENAME');
exit;
EOF
  fi
fi
#Exctract process control checks ends

#job_process_control checks
rerun_flag=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT REPROCESS_FLAG FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$jobrunid;  
exit;
EOF`

if [ $rerun_flag = 0 ]
then
echo JOB_PROCESS_CONTROL CHECK PASSED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','Reprocess Flag is ${rerun_flag}',sysdate,'$INTERFACENAME');
exit;
EOF

fi
#job_process_control checks ends

else
echo File is not created
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','REPROCESS-FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','File is not created',sysdate,'$INTERFACENAME');
exit;
EOF
fi

elif [ $REPROCESS_CHECK_FLAG = 0 ]  #Job is executing for the first time(Not reprocessing)
then
echo PROCESSING THE JOB FOR THE FIRST TIME
echo $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME
echo JOB STARTS.............
before_run_date=`date +%Y%m%d%H%M%S`
sh /insights/app/scripts/ODI_SCENARIO_OB.sh $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME



date_from=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_FROM FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));  
exit;
EOF`

date_to=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_TO FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));  
exit;
EOF`

date_from=$(echo $date_from | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters
date_to=$(echo $date_to | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters

IFS=":"    #Field Seperator
#Loop to get the data from csv
while read f1 f2
do
job=$f1
if [ "$job" == "$JOBNAME" ]    #if statement for job name comparison to take data from the csv file
then
echo JOBNAME: $job
sql=$f2
echo QUERY IS: $sql
echo
echo
fi    #End of if statement for job name comparison to take data from the csv file
done < data.csv
unset IFS
#End of inner loop to get the data from csv

echo FROM_DATE $date_from
echo TO_DATE $date_to

sql=$(echo $sql | sed -e "s/'\$date_from'/'$date_from'/g") #Substituting the actual data in the Query
sql=$(echo $sql | sed -e "s/'\$date_to'/'$date_to'/g") #Substituting the actual data in the Query

echo
echo
echo AFTER SUBSTITUTION: $sql


#Getting data from the 3NF/DWDD that is to be populated in the file
if [ "$OBJECTLAYER" = "DW_3NF" ]
then
echo DW_3NF Layer
table_data=`sqlplus -s $TNFDB_CONNECTION <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF`
else
echo DWDD Layer
table_data=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF`
fi
#End of fetching data from 3NF/DWDD

table_data=`echo $table_data|tr -d [:' ']`    #Removing the unwanted spaces from the query result


#echo $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME
  #sh /insights/app/scripts/ODI_SCENARIO_OB.sh $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME

outdirpath=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select OB_TARGET_DIR_PATH from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF`
echo TARGET_DIR $outdirpath

interfacefileid=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select INTERFACE_FILE_ID from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF`
echo INTERFACE_FILE_ID $interfacefileid

jobrunid=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select MAX(JOB_RUN_ID) from FILE_CONTROL where INTERFACE_FILE_ID=$interfacefileid;  
exit;
EOF`
echo JOB_RUN_ID $jobrunid

FILENAME=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select FILE_NAME from FILE_CONTROL where JOB_RUN_ID=$jobrunid;
exit;
EOF`
echo FILE_NAME $FILENAME
FILENAME=`echo $FILENAME|tr -d [:' ']`

file_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
select FILE_PROCESSING_STATUS from FILE_CONTROL where FILE_NAME='$FILENAME';
exit;
EOF`
echo FILE_PROCESSING_STATUS $file_status

expected_status=5
echo EXPECTED_STATUS $expected_status

OUT_FILENAME=$FILENAME
OUT_FILENAME=`echo $OUT_FILENAME|tr -d [:' ']`

echo BEFORE_JOBRUN_DATE $before_run_date

filedate=$(echo $(echo $(echo $OUT_FILENAME | rev)|cut -c5-18)| rev)
echo LATEST_FILE_DATE $filedate

echo OUTPUT_FILE_NAME $OUT_FILENAME
echo $outdirpath'/'$OUT_FILENAME

if [ $before_run_date -lt $filedate ]
then

	if [ $(ls $outdirpath/$OUT_FILENAME) ]
	then
		echo File Present
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
		if [ $file_status = 5 ]
		then
			echo Entry created in File Control Table
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF

		elif [ $file_status = 2 ]
		then
			echo File creation failed
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','FAILED','File_Control table entry is 2 ERROR',sysdate,'$INTERFACENAME');
exit;
EOF

		else
			echo Entry not found in File Control Table
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(FILE_CONTROL)','OUTBOUND','FAILED','File is Created Entry not Found in FILE_CONTROL table',sysdate,'$INTERFACENAME');
exit;
EOF
		fi

head=$(cat $outdirpath/$OUT_FILENAME | head -n 1)
file_data=$(cat $outdirpath/$OUT_FILENAME)
file_data=$(echo "$file_data" | tr '","' ' ')
head=$(echo "$head" | tr ',' ' ')
file_data=`echo $file_data|tr -d [:' ']`
head=`echo $head|tr -d [:' ']`
file_data=${file_data//$head}
		if [ "$table_data" == "$file_data" ]
		then
			echo data matching
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','DATA_VALIDATION','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
		else
			echo data mismatch
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','DATA_VALIDATION','OUTBOUND','FAILED','File data doesnot match with the expected data',sysdate,'$INTERFACENAME');
exit;
EOF
		fi
	else
		echo FileName Mismatch
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','Filename Mismatch or file is not created in the output directory',sysdate,'$INTERFACENAME');
exit;
EOF
	fi

#Extract_process_control Checks
new_date_from=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_FROM FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));
exit;
EOF`

new_date_to=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT EXTRACT_DATE_TO FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));
exit;
EOF`

new_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT STATUS_FLAG FROM EXTRACT_PROCESS_CONTROL WHERE JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));  
exit;
EOF`

echo NEW_STATUS $new_status

new_date_from=$(echo $new_date_from | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters
new_date_to=$(echo $new_date_to | sed -e 's/[\r\n]//g') #Removing the unwanted newline characters

echo NEW_DATE_FROM $new_date_from
echo NEW_DATE_TO $new_date_to

new_date_from=`echo $new_date_from|tr -d [:' ']`
new_date_to=`echo $new_date_to|tr -d [:' ']`
date_from=`echo $date_from|tr -d [:' ']`
date_to=`echo $date_to|tr -d [:' ']`

if [ "$new_date_from" == "$date_from" ] && [ "$new_date_to" == "$date_to" ]
then
if [ $new_status = 1 ]
then
echo EXTRACT_PROCESS_CONTROL PASSED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(EXTRACT_PROCESS_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
else
echo EXTRACT_PROCESS_CONTROL FAILED STATUS_FLAG NOT EXPECTED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','STATUS_FLAG is ${new_status}',sysdate,'$INTERFACENAME');
exit;
EOF
fi
elif [ "$new_date_from" != "$date_from" ] || [ "$new_date_to" != "$date_to" ]
then
if [ $new_status = 1 ]
then
echo EXTRACT_PROCESS_CONTROL FAILED INCORRECT_EXTRACT_DATE
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','INCORRECT EXTRACT_DATE',sysdate,'$INTERFACENAME');
exit;
EOF
else
echo EXTRACT_PROCESS_CONTROL FAILED INCORRECT_EXTRACT_DATE STATUS_FLAG NOT EXPECTED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','INCORRECT EXTRACT_DATE - STATUS_FLAG is ${new_status}',sysdate,'$INTERFACENAME');
exit;
EOF
fi
fi
#Extract_process_control Checks ends


#Job_process_control checks
rerun_flag=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT REPROCESS_FLAG FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$jobrunid;  
exit;
EOF`

run_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_RUN_STATUS FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$jobrunid;  
exit;
EOF`

if [ $rerun_flag = 0 ] && [ $run_status = 1 ]
then
echo JOB_PROCESS_CONTROL CHECK PASSED
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
elif [ $run_status = 0 ]
then
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','JOB_RUN_STATUS is ${run_status}',sysdate,'$INTERFACENAME');
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK(JOB_PROCESS_CONTROL)','OUTBOUND','FAILED','Reprocess Flag is ${rerun_flag}',sysdate,'$INTERFACENAME');
exit;
EOF
fi
#job_process_cotrol checks ends


#Changing the reprocess_flag to 1
sqlplus -s $DB_CONNECTION <<EOF
UPDATE JOB_PROCESS_CONTROL SET REPROCESS_FLAG=1 WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_ID=(SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME'));
commit;
exit;
EOF

#Reprocessing the job
#sh outbound_data.sh $SOURCENAME $JOBNAME
sh $BASE_FOLDER/outbound.sh

else
echo File is not created
sqlplus -s $DB_CONNECTION <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','File is not created',sysdate,'$INTERFACENAME');
exit;
EOF
fi

fi  #Reprocessing check ends
#Rerunnability Ends

fi
