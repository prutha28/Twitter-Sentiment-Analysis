from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.axis([0, 11, 0, 300])
    plt.legend(loc="upper left", bbox_to_anchor=[0, 1], ncol=1, fancybox=True)
    
    positive_counts=[]
    negative_counts=[]

    for i in range( 0, len(counts)):
        j = counts[i]
        if j != []:
            positive_counts.append(j[0][1])
            negative_counts.append(j[1][1])
    
    plt.plot(positive_counts, label="Positive", marker='o')
    plt.plot(negative_counts, label="Negative", marker='o')
    plt.show()


def load_wordlist(filename):
	""" 
	This function should return a list or set of words from the given filename.
	"""
	# YOUR CODE HERE
	with open(filename) as f:
		content = [x.strip('\n') for x in f.readlines()]
	return content



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})

    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    pword_rdd=tweets.flatMap(lambda line: line.split(" ")).map(lambda word: ("positive",1) if word in pwords else ("positive",0)).reduceByKey(lambda a,b:a+b)
    nword_rdd=tweets.flatMap(lambda line: line.split(" ")).map(lambda word: ("negative",1) if word in nwords else ("negative",0)).reduceByKey(lambda a,b:a+b)

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # make the plot on this rdd -combined_rdd

    combined_rdd=pword_rdd.union(nword_rdd)
    running_counts=combined_rdd.updateStateByKey(updateFunction)
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]

    counts = []
    combined_rdd.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    # print "printing dstream"
    running_counts.pprint()
		
	# Start the computation
    ssc.start()                         
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)


if __name__=="__main__":
    main()
