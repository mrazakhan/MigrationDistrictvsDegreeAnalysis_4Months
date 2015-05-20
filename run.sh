#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='/export/home/mraza/Rwanda_MigrationDistrictvsDegreeAnalysis_4Months/target/scala-2.10/migrationdegreeanalyzer_4months_2.10-0.1.jar'
Call_file_suffix='-ModalCallVolDegree.csv'


exec_obj_name='MigrationDegreeAnalyzer_4months'


#month_array=(0501 0502 0503 0504 0505 0506 0507 0508 0509 0510 0511 0512 0601 0602 0603 0604 0605 0606 0607 0608 0609 0610) 
month_array=( 0801  0802 0803 0804 )
'''
for ((index=2; index < ${#month_array[@]}-1; index++))
do

        monthl2=${month_array[index-2]}
        monthl1=${month_array[index-1]}
        monthn1=${month_array[index]}
        monthn2=${month_array[index+1]}
        echo ' Monthl1 is ' $monthl1 ' and monthl2 is ' $monthl2
        echo ' Monthn1 is ' $monthn1 ' and monthn2 is ' $monthn2
        spark-submit --class $exec_obj_name --master yarn --driver-memory 28G \
        --executor-memory 44G \
        --executor-cores 28 \
        --num-executors 3 \
        $jar $monthn1 $monthn1$Call_file_suffix $monthn2$Call_file_suffix $monthl1$Call_file_suffix $monthl2$Call_file_suffix  --verbose ;
done
'''
month_array=(0602 0603 0604 0605  ) 

for ((index=2; index < ${#month_array[@]}-1; index++))
do
	
	monthl2=${month_array[index-2]} 
	monthl1=${month_array[index-1]} 
	monthn1=${month_array[index]}
	monthn2=${month_array[index+1]}
	echo ' Monthl1 is ' $monthl1 ' and monthl2 is ' $monthl2
	echo ' Monthn1 is ' $monthn1 ' and monthn2 is ' $monthn2
	spark-submit --class $exec_obj_name --master yarn --driver-memory 28G \
        --executor-memory 44G \
        --executor-cores 28 \
        --num-executors 3 \
	$jar $monthn1 $monthn1$Call_file_suffix $monthn2$Call_file_suffix $monthl1$Call_file_suffix $monthl2$Call_file_suffix  --verbose ;
done
