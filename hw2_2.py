# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re

# global values.
Dir = '/home/Spark-Distributed-master/homework/hw2/dataset/'
resultDir = '/home/Spark-Distributed-master/homework/hw2/result2/'
newHeader_perHour = ['IDLink','by hour']
newHeader_perDay = ['IDLink','by day']


def per_hour_popularity(line):
    average = []
    ID = line[0]
    average.append(ID)
    tempAverage=0
    for x in range(1,len(line)):
        tempAverage=tempAverage+float(line[x])
    average.append(tempAverage/48)
    return average

def per_day_popularity(line):
    average = []
    ID = line[0]
    average.append(ID)
    tempAverage=0
    for x in range(1,len(line)):
        tempAverage=tempAverage+float(line[x])
    average.append(tempAverage/2)
    return average

def Do(fileName,resultName):
    # input data.
    with open (Dir+fileName+".csv",'r',encoding = 'utf8') as file:
        data = csv.reader(file,delimiter = ",")
        dataset = list(data)

    # create RDD.
    data_RDD = sc.parallelize(dataset)
    # mapping.(per_hour and per day)
    header = data_RDD.first()
    per_hour_result = data_RDD.filter(lambda x: x!=header).map(per_hour_popularity).collect()
    per_day_result = data_RDD.filter(lambda x: x!=header).map(per_day_popularity).collect()

    # write in file (per_hour_result).
    file = open(resultDir+resultName+"_perHour.txt",'a')
    writer = csv.writer(file)
    writer.writerow(newHeader_perHour)
    for i in range(0,len(per_hour_result)):
        writer.writerow(per_hour_result[i])
    file.close()

    # write in file (per_day_result).
    file = open(resultDir+resultName+"_perDay.txt",'a')
    writer = csv.writer(file)
    writer.writerow(newHeader_perDay)
    for i in range(0,len(per_day_result)):
        writer.writerow(per_day_result[i])
    file.close()
    print(fileName,"Parser Success!")


# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="2500m"
sparkDriverMemory="2500m"
sparkCoreMax="2"
outputFile = "result2.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)


fileList_Facebook = ['Facebook_Economy','Facebook_Microsoft','Facebook_Obama','Facebook_Palestine']
fileList_GooglePlus = ['GooglePlus_Economy','GooglePlus_Microsoft','GooglePlus_Obama','GooglePlus_Palestine']
fileList_LinkedIn = ['LinkedIn_Economy','LinkedIn_Microsoft','LinkedIn_Obama','LinkedIn_Palestine']

# start.
for x in fileList_Facebook:
     Do(x,'Facebook')

for x in fileList_GooglePlus:
     Do(x,'GooglePlus')

for x in fileList_LinkedIn:
     Do(x,'LinkedIn')

print("all success!")
