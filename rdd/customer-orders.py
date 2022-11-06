from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer-Orders")
sc = SparkContext(conf=conf)

def splitter(line):
    splitted_line = line.split(",")
    return (int(splitted_line[0]), float(splitted_line[2]))


lines = sc.textFile("../data/customer-orders.csv")
lines_two = lines.map(splitter)
lines_three = lines_two.reduceByKey(lambda x, y: x + y)
lines_op = lines_three.collect()

for line in lines_op:
    print(f"{line[0]} : {str(line[1])}")