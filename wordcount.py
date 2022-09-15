import sys
from pyspark import SparkContext, SparkConf

if __name__="__main__":

    conf = SparkConf().SetAppName("wordcount").SetMaster("Local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(sys.argv[1])
        
    word_counts = lines.flatMap(lambda line: line.split(' ')) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda count1, count2: count1 + count2) \
                       .collect()

    for (word, count) in word_counts:
        print(word, count)