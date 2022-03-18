#!/bin/bash 


#usage python main.py <option> <dim> <block size, if option 3> 


# 600x600 to 3000x3000, ++400 
# 4096x4096 to 10240x10240 ++2048

MEAN=0


DIR=results
FILE1=generic_values.csv 
FILE2=line_values.csv 
FILE3=block_values.csv

OPTION_1=1
OPTION_2=2
OPTION_3=3

#find and replace file 

rm -r $DIR 2> /dev/null
mkdir $DIR
touch $DIR/$FILE1  $DIR/$FILE2  $DIR/$FILE3


# 1 
for MATRIX_DIM in {600..3000..400}
do
      if [ $MEAN -eq 1 ]
        then
            for TIME in {1..3}
            do  
                python ./main.py $OPTION_1 $MATRIX_DIM 
            done 
      else 
        python ./main.py $OPTION_1 $MATRIX_DIM   
      fi   
done 


# # 2
for MATRIX_DIM in {600..3000..400}
do
      if [ $MEAN -eq 1 ]
        then
            for TIME in {1..3}
            do  
                python ./main.py $OPTION_2 $MATRIX_DIM 
            done 
      else 
        python ./main.py $OPTION_2 $MATRIX_DIM   
      fi   
done 


# # 3 
for MATRIX_DIM in {4096..10240..2048}
do 
    for BLK in 128 256 512
    do 
        if [ $MEAN -eq 1 ]
        then
            for TIME in {1..3}
            do   
                python ./main.py $OPTION_3 $MATRIX_DIM $BLK
            done
        else 
            echo $MATRIX_DIM
            python ./main.py $OPTION_3 $MATRIX_DIM $BLK
        fi 
    done 
done 