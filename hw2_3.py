# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re

def semtiment_score(line):
    sum = float(line[6])+float(line[7])
    return sum

# global values.
Dir = '/home/Spark-Distributed-master/homework/hw2/dataset/'
resultDir = '/home/Spark-Distributed-master/homework/hw2/result3/'

# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="2500m"
sparkDriverMemory="2500m"
sparkCoreMax="2"


# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)

# input data.
with open (Dir+"News_Final.csv",'r',encoding = 'utf8') as file:
    data = csv.reader(file,delimiter = ",")
    dataset = list(data)

# create RDD.
data_RDD = sc.parallelize(dataset)
header = data_RDD.first()
topic_obama = data_RDD.filter(lambda x: x!=header).filter(lambda x: x[4]=='obama').map(semtiment_score)
topic_economy = data_RDD.filter(lambda x: x!=header).filter(lambda x: x[4]=='economy').map(semtiment_score)
topic_microsoft = data_RDD.filter(lambda x: x!=header).filter(lambda x: x[4]=='microsoft').map(semtiment_score)
topic_palestine = data_RDD.filter(lambda x: x!=header).filter(lambda x: x[4]=='palestine').map(semtiment_score)

# Q3 answer.
topic_obama_ans = [topic_obama.sum(),topic_obama.mean()]
topic_economy_ans = [topic_economy.sum(),topic_economy.mean()]
topic_microsoft_ans = [topic_microsoft.sum(),topic_microsoft.mean()]
topic_palestine_ans = [topic_palestine.sum(),topic_palestine.mean()]

# write in file (per_day_result).
file = open(resultDir+"result3.txt",'a')
writer = csv.writer(file)
writer.writerow(['Topic obama semtiment score(Sum && Average)'])
writer.writerow(topic_obama_ans)
writer.writerow(['Topic economy semtiment score(Sum && Average)'])
writer.writerow(topic_economy_ans)
writer.writerow(['Topic microsoft semtiment score(Sum && Average)'])
writer.writerow(topic_microsoft_ans)
writer.writerow(['Topic palestine semtiment score(Sum && Average)'])
writer.writerow(topic_palestine_ans)
file.close()
print("Parser Success!")
   
