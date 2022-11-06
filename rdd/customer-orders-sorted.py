from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Customer-Orders-Sorted")
sc = SparkContext(conf=conf)

def format(line):
    x = line.split(",")
    return (int(x[0]), float(x[2]))


lines = sc.textFile("../data/customer-orders.csv")
lines_2 = lines.map(format)
lines_3 = lines_2.reduceByKey(lambda x, y: x + y)
lines_4 = lines_3.map(lambda x: (x[1], x[0])).sortByKey()
lines_5 = lines_4.collect()

for line in lines_5:
    print(line)