
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import math
import numpy as np


# Exact Algorithm
def exactAlg(num):
    if num not in exactFreqDict:
        exactFreqDict[num] = 1
    else:
        exactFreqDict[num] += 1
        
        

# Reservoir Sampling
def reservoirSampling(num, m):
    if streamLength <= m:
        S_reservoir.append(num)
    else:
        if random.random() <= m/streamLength:
            idx = random.randint(0, m-1)
            S_reservoir[idx] = num
            


# Stricky Sampling
def stickySampling(num, r, n):
    if num in S_sticky:
        S_sticky[num] += 1
    else:
        if random.random() <= r/n:
            S_sticky[num] = 1



# Operations to perform after receiving an RDD 'batch'
def process_batch(batch):
    # Call global variables
    global streamLength, exactFreqDict, S_reservoir, S_sticky, n, delta, phi, epsilon, m, r
    
    # Transform from RRD of string --> list of int
    intList = batch.map(lambda num: int(num)).collect()

    # Update global data structures
    for elem in intList:
        if streamLength >= n:
            stopping_condition.set()
            return
        else:
            streamLength += 1
            exactAlg(elem)
            reservoirSampling(elem, m)
            stickySampling(elem, r, n)



# main function
if __name__ == '__main__':
    
    # Error message on input reading
    assert len(sys.argv) == 6, "USAGE: n, phi, epsilon, delta, port"

    n = int(sys.argv[1])
    phi = float(sys.argv[2])       
    epsilon = float(sys.argv[3])       
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])

    m = math.ceil(1/phi)    # size of reservoir sample
    r = math.log(1/(delta*phi))/epsilon    # r/n --> sampling rate for sticky sampling


    # Spark configuration
    conf = SparkConf().setMaster("local[*]").setAppName("HW3")
    conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)        # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()
    
    
    # Defining a counter for the number of element to be processed
    streamLength = 0 
        
    # Defining the data structures
    exactFreqDict = {}       # Dicitionary for exact algorithm
    S_reservoir = []         # Array for reservoir sampling
    S_sticky = {}            # Dictionary for Sticky sampling


    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    DStream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following:
    DStream.foreachRDD(lambda batch: process_batch(batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, False)
    
    
    ####################
    # OUTPUT FORMAT
    ####################
    
    print('INPUT PROPERTIES')
    print('n =', n, 'phi =', phi, 'epsilon =', epsilon, 'delta =', delta, 'port =', portExp)


    #######################################################
    # Exact Algorithm
    #######################################################

    print('EXACT ALGORITHM')
    print('Number of items in the data structure =', len(exactFreqDict))

    exactFrequentItems = [key for key, value in exactFreqDict.items() if value >= n*phi]

    print('Number of true frequent items = ', len(exactFrequentItems))

    print("True frequent items:")
    for elem in sorted(exactFrequentItems):
        print(elem)

    #######################################################
    # Reservoir sampling
    #######################################################

    print('RESERVOIR SAMPLING')
    print('Size m of the sample =', len(S_reservoir))

    reservoirFrequentItems = np.unique(S_reservoir)
    print('Number of estimated frequent items =', len(reservoirFrequentItems))

    print('Estimated frequent items:')
    for elem in reservoirFrequentItems:
        if elem in exactFrequentItems:
            print(elem, '+')
        else:
            print(elem, '-')


    #######################################################
    # Sticky sampling
    #######################################################
    print('STICKY SAMPLING')
    print('Number of items in the Hash Table =', len(S_sticky))

    stickyFrequentItems = [key for key, value in S_sticky.items() if value >= (phi - epsilon)*n]

    print('Number of estimated frequent items =', len(stickyFrequentItems))

    print("Estimated frequent items:")
    for elem in sorted(stickyFrequentItems):
        if elem in exactFrequentItems:
            print(elem, '+')
        else:
            print(elem, '-')

    

    








    




    
    









    