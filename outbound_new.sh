#!/bin/bash
# ###################################################################################
#
#       Created by: Sharan Shivamurthy
#       Date: 2019-Dec-13
#       Description:
#		    This shell script developed for testing the ETL Jobs for Outbound Interfaces
#       Example:
#		      rebus.sh
#
#####################################################################################
# Parameters
# ==========
# SOURCENAME          char  Source system to be tested
# JOBNAME        		  char  The ETL job to be tested

BASE_FOLDER="/home/sshuser/Test_Automation/Outbound"
. $BASE_FOLDER/environment_outbound.sh

currentdate=`date +%y/%m/%d-%H:%M:%S`
echo currentdate $currentdate

LINE=$(grep -n "SOURCENAME=" $BASE_FOLDER/config_outbound.sh | cut -d : -f 1) #Getting the line numbers which are matching with the SOURCENAME  
echo LINE $LINE
arr=(`echo ${LINE}`);

for i in "${arr[@]}"
do
echo
ENDLINE=(`expr $i + $num`)
LIN2=$i
while [ $LIN2 -lt $ENDLINE ]
do
#echo LIN22 $LIN2
		
if [ -z "$SOURCENAME" ]
then	
SOURCENAME=$(sed -n "${LIN2} s/^ *SOURCENAME=*//p" config_outbound.sh)

elif [ -z "$JOBNAME" ]
then
JOBNAME=$(sed -n "${LIN2} s/^ *JOB_NAME=*//p" config_outbound.sh)

elif [ -z "$TESTCASE" ]
then
TESTCASE=$(sed -n "${LIN2} s/^ *TESTCASE=*//p" config_outbound.sh)

elif [ -z "$OBJECTNAME" ]
then
OBJECTNAME=$(sed -n "${LIN2} s/^ *OBJECT_NAME=*//p" config_outbound.sh)

elif [ -z "$OBJECTLAYER" ]
then
OBJECTLAYER=$(sed -n "${LIN2} s/^ *OBJECT_LAYER=*//p" config_outbound.sh)

elif [ -z "$INTERFACEID" ]
then
INTERFACEID=$(sed -n "${LIN2} s/^ *INTERFACE_ID=*//p" config_outbound.sh)

elif [ -z "$SCENARIONAME" ]
then
SCENARIONAME=$(sed -n "${LIN2} s/^ *SCENARIO_NAME=*//p" config_outbound.sh)

elif [ -z "$SCENARIOVERSION" ]
then
SCENARIOVERSION=$(sed -n "${LIN2} s/^ *SCENARIO_VERSION=*//p" config_outbound.sh)

elif [ -z "$INTERFACENAME" ]
then
INTERFACENAME=$(sed -n "${LIN2} s/^ *INTERFACE_NAME=*//p" config_outbound.sh)

fi
LIN2=`expr $LIN2 + 1`
done

#echo $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME
echo SOURCENAME: $SOURCENAME
echo JOB_NAME: $JOBNAME
echo TESTCASE: $TESTCASE
echo OBJECT_NAME: $OBJECTNAME
echo OBJECT_LAYER: $OBJECTLAYER
echo INTERFACE_ID: $INTERFACEID
echo SCENARIO_NAME: $SCENARIONAME
echo SCENARIO_VERSION: $SCENARIOVERSION
echo INTERFACE_NAME: $INTERFACENAME

if [ -z "$SOURCENAME" ] || [ -z "$JOBNAME" ] || [ -z "$TESTCASE" ] || [ -z "$OBJECTNAME" ] || [ -z "$OBJECTLAYER" ] || [ -z "$INTERFACEID" ] || [ -z "$SCENARIONAME" ] || [ -z "$SCENARIOVERSION" ] || [ -z "$INTERFACENAME" ]      #if statement to check the Parameter availability
then
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,stage,test_result,comments,execute_date) 
values('$SOURCENAME','$TESTCASE','OUTBOUND','FAILED','Input parameters not sufficient',sysdate); 
exit;
EOF
else
date_from=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
SELECT MAX(EXTRACT_DATE_FROM) FROM EXTRACT_PROCESS_CONTROL;  
exit;
EOF
)
date_to=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
SELECT MAX(EXTRACT_DATE_TO) FROM EXTRACT_PROCESS_CONTROL;  
exit;
EOF
)

date_from=$(echo $date_from | sed -e 's/[\r\n]//g')
date_to=$(echo $date_to | sed -e 's/[\r\n]//g')

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
#End of inner loop

echo FROM_DATE $date_from
echo TO_DATE $date_to

sql=$(echo $sql | sed -e "s/'\$date_from'/'$date_from'/g")
sql=$(echo $sql | sed -e "s/'\$date_to'/'$date_to'/g")

echo
echo
echo AFTER SUBSTITUTION: $sql


if [ "$OBJECTLAYER" = "DW_3NF" ]      #if statement for the OBJECT_LAYER comparison
then
echo DW_3NF Layer
table_data=$( sqlplus -s $TNFDB_USER/$TNFDB_PASSWORD@$TNFDB <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF
)
else
echo DWDD Layer
table_data=$( sqlplus -s $DWDDDB_USER/$DWDDDB_PASSWORD@$DWDDDB <<EOF
set pagesize 500 linesize 120 feedback off verify off heading off echo off
$sql;
exit;
EOF
)
fi      #End of if statement for the OBJECT_LAYER comparison
echo TABLE DATA $table_data
table_data=`echo $table_data|tr -d [:' ']`
before_run_date=`date +%Y%m%d%H%M%S`

echo $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME

 # cd $odidirectory
  sh /insights/app/scripts/ODI_SCENARIO_OB.sh $SOURCENAME $JOBNAME $OBJECTNAME $OBJECTLAYER $INTERFACEID $SCENARIONAME $SCENARIOVERSION $INTERFACENAME

outdirpath=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
select OB_TARGET_DIR_PATH from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF
)
echo TARGET_DIR $outdirpath

interfacefileid=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
select INTERFACE_FILE_ID from INTERFACE_FILE_MASTER where interface_id='$INTERFACEID' and interface_name='$INTERFACENAME';  
exit;
EOF
)
echo INTERFACE_FILE_ID $interfacefileid

jobrunid=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
select MAX(JOB_RUN_ID) from FILE_CONTROL where INTERFACE_FILE_ID=$interfacefileid;  
exit;
EOF
)
echo JOB_RUN_ID $jobrunid

FILENAME=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
select FILE_NAME from FILE_CONTROL where JOB_RUN_ID=$jobrunid;
exit;
EOF
)
echo FILE_NAME $FILENAME
FILENAME=`echo $FILENAME|tr -d [:' ']`

file_status=$( sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
set head off 
select FILE_PROCESSING_STATUS from FILE_CONTROL where FILE_NAME='$FILENAME';
exit;
EOF
)
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
#cd $outdirpath
#ls $outdirpath/$OUT_FILENAME
	if [ $(ls $outdirpath/$OUT_FILENAME) ]
	then
		echo File Present
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
		if [ $file_status = 5 ]
		then
			echo Entry created in File Control Table
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF

		elif [ $file_status = 2 ]
		then
			echo File creation failed
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK','OUTBOUND','FAILED','File_Control table entry is 2 ERROR',sysdate,'$INTERFACENAME');
exit;
EOF

		else
			echo Entry not found in File Control Table
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','AUDIT_TABLE_CHECK','OUTBOUND','FAILED','File is Created Entry not Found in FILE_CONTROL table',sysdate,'$INTERFACENAME');
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
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','DATA_VALIDATION','OUTBOUND','PASSED',sysdate,'$INTERFACENAME');
exit;
EOF
		else
			echo data mismatch
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','DATA_VALIDATION','OUTBOUND','FAILED','File data doesnot match with the expected data',sysdate,'$INTERFACENAME');
exit;
EOF
		fi
	else
		echo FileName Mismatch
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','Filename Mismatch or file is not created in the output directory',sysdate,'$INTERFACENAME');
exit;
EOF
	fi
else
echo File is not created
sqlplus -s $DB_USER/$DB_PASSWORD@$DB <<EOF
insert into hadoop_stage_test_log(source_name,testcase,step_name,stage,test_result,comments,execute_date,interface_name)
values('$SOURCENAME','$TESTCASE','FILE_AVAILABILITY_CHECK','OUTBOUND','FAILED','File is not created',sysdate,'$INTERFACENAME');
exit;
EOF
fi

fi      #End of if statement to check the Parameter availability


SOURCENAME=""
JOBNAME=""
TESTCASE=""
OBJECTNAME=""
OBJECTLAYER=""
INTERFACEID=""
SCENARIONAME=""
SCENARIOVERSION=""
INTERFACENAME=""
done

