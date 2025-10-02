import random
import math
from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel


# Define spark configuration:
conf = SparkConf().setAppName('Homework2')
conf.set("spark.locality.wait", "0s")
# and spark context:
sc = SparkContext(conf=conf)



def MRApproxOutliers(pointsRDD, D, M):
    # step A
    #Map function
    def map_to_cells(point):
        x, y = point
        i = int(x // (D / (2 * 2**0.5)))
        j = int(y // (D / (2 * 2**0.5)))
        return ((i, j), 1)

    #MAP + SHUFFLE + REDUCE PHASES
    cellsRDD = pointsRDD.map(map_to_cells).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] > 0)

    #Collect the cells in a local dictionary
    cells = cellsRDD.collectAsMap()

    # step B
    def get_N3_N7(cell):
        (i, j), count = cell
        N3 = 0
        N7 = 0
        for di in range(-1, 2):
            for dj in range(-1, 2):
                N3 += cells.get((i + di, j + dj), 0)
        for di in range(-3, 4):
            for dj in range(-3, 4):
                N7 += cells.get((i + di, j + dj), 0)
        return ((i, j), (count, N3, N7))

    final_cellsRDD = cellsRDD.map(get_N3_N7)

    #Count the number of sure outliers/uncertain points
    outliers_count = final_cellsRDD.filter(lambda x: x[1][2] <= M).map(lambda x: x[1][0]).sum()
    uncertain_count = final_cellsRDD.filter(lambda x: x[1][1] <= M and x[1][2] > M).map(lambda x: x[1][0]).sum()

    print("Number of sure outliers =", outliers_count)
    print("Number of uncertain points =", uncertain_count)



def SequentialFFT(P, K):
    
    n = len(P)
    C = [0 for _ in range(K)]
    C[0] = P[random.randint(0,n-1)]
    n_centers = 1
    
    distances_list = [math.inf for _ in range(n)]     
    
    while n_centers < K:
        max_distance = 0.0
        maxIDX = -1
        for i in range(n):
            point = P[i]
            
            distance = math.dist(point, C[n_centers - 1])
            
            if distance < distances_list[i]:
                distances_list[i] = distance      #update the distance between point P[i] from the set C
                
            if distances_list[i] > max_distance:
                max_distance = distances_list[i]
                maxIDX = i      #candidate center
        
        if maxIDX == -1:
            print('Error !!!!!')
            return 
        
        # insert the new center
        C[n_centers] = P[maxIDX]
        n_centers += 1

    return C



def minDistR3(point, centers):
    
    min_dist = math.inf
    for center in centers.value:
        dist = math.dist(point, center)
        if dist < min_dist:
            min_dist = dist
            
    return min_dist



def MRFFT(P, K):
    
    # Round 1: FFT on partitions   
    start_time = time.time()
    coreset = P.mapPartitions(lambda partition: SequentialFFT(list(partition), K)).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
    action = coreset.count()
    end_time = time.time()
    print("Running time of MRFFT Round 1 =", round((end_time - start_time)*1000), "ms")
    
    

    # Round 2: FFT on coreset
    start_time = time.time()
    finalCoreset = coreset.collect()
    centers = SequentialFFT(finalCoreset, K)
    end_time = time.time()
    print("Running time of MRFFT Round 2 =", round((end_time - start_time)*1000), "ms")
    
    # unpersist the previous RDD:
    coreset.unpersist()
    
    # create the broadcaste variable
    C_broadcast = sc.broadcast(centers)
    
    # Round 3
    start_time = time.time()
    radius = P.map(lambda x: minDistR3(x, C_broadcast)).reduce(lambda x,y: max(x,y))
    end_time = time.time()
    print("Running time of MRFFT Round 3 =", round((end_time - start_time)*1000), "ms")
    
    print('Radius =', round(radius,8))
    return radius




def main():

    file_path = sys.argv[1]
    M = int(sys.argv[2])       
    K = int(sys.argv[3])       
    L = int(sys.argv[4])        

    print(file_path, 'M=%.0f K=%.0f L=%.0f' %(M,K,L))
    

    rawData = sc.textFile(file_path)  #RDD of strings
    inputPoints = rawData.map(lambda line: tuple(map(float, line.split(',')))).repartition(numPartitions=L).persist(storageLevel=StorageLevel.MEMORY_AND_DISK) #RDD of points


    total_points = inputPoints.count()
    print("Number of points =", total_points)


    D = MRFFT(inputPoints, K)

    start_time = time.time()
    MRApproxOutliers(inputPoints, D, M)
    end_time = time.time()
    print("Running time of MRApproxOutliers =", round((end_time - start_time)*1000), "ms")
    
    
    sc.stop()


if __name__ == "__main__":
    import sys
    import time
    main()


