## File: a2.py
## FirstName: Sourav
## LastName: Bihani
## UB# : 50290969



from pyspark import SparkContext, SparkConf
import sys

def vertex(data):
    d = data.split(" ") 
    t = [int(x) for x in d]
    return [(t[0], t[1]) , (t[1] , t[0])]

def LargeStarInit(record) :
    a,b = record
    return [(b,a),(a,b)]

def SmallStarInit(record) :
    a,b = record
    if b <= a:
       	return [(a,b)] 
    else:
       	return [(b,a)]


def Largestar(list):
    node, gama = list
    minimumgama = min(node,min(gama))
    for i in gama:
        if i > node:
            yield i, minimumgama 

def Smallstar(list):
    node,gama = list
    minimumgama = min(gama)
    if node > minimumgama:
		yield node,minimumgama
    	for i in gama:
        	if i<node and i <>minimumgama:
				yield i,minimumgama
    	
	
if __name__ == "__main__":

    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile(sys.argv[1]) 
    rddLargeStar=text_file.flatMap(vertex).groupByKey().flatMap(Largestar).distinct()
    rddSmallStar=rddLargeStar.flatMap(SmallStarInit).groupByKey().flatMap(Smallstar).distinct()
    diff=rddLargeStar.union(rddSmallStar).subtract(rddLargeStar.intersection(rddSmallStar)).count()

    while diff<>0:
        rddLargeStar=rddSmallStar.flatMap(LargeStarInit).groupByKey().flatMap(Largestar).distinct()
    	rddSmallStar=rddLargeStar.flatMap(SmallStarInit).groupByKey().flatMap(Smallstar).distinct()
	diff=rddLargeStar.union(rddSmallStar).subtract(rddLargeStar.intersection(rddSmallStar)).count()
    	        
       
    rddSmallStar.map(lambda (a, b): "{0} {1}".format(a, b)).saveAsTextFile("output")
    sc.stop()
           
    
    