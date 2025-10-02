

import math
import numpy as np
from pyspark import SparkContext



def ExactOutliers(points, D, M, K):
    N = len(points)
    counts_list = [1 for i in range(N)] #List containing B_s(p,D), initialized with ones

    #Compute distances
    for i in range(N-1):
        for j in range(i+1, N):
            dist = math.dist(points[i], points[j])
            if dist <= D:
                #Update the counts for both i and j
                counts_list[i] += 1
                counts_list[j] += 1

    #Count the number of outliers
    num_outliers = 0
    for count in counts_list:
        if count <= M:
            num_outliers += 1

    print("The number of (D,M)-outliers is:", num_outliers)

    print("The first", min(K, num_outliers), "outliers points in non-decreasing order of |BS(p,D)| are:")
    for idx in np.argsort(counts_list)[:min(K, num_outliers)]:
        print(points[idx])
        
        

def MRApproxOutliers(pointsRDD, D, M, K):
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

    print("Number of sure outliers:", outliers_count)
    print("Number of uncertain points:", uncertain_count)

    #Sort the cells by non-decreasing order of size
    sorted_cells = final_cellsRDD.sortBy(lambda x: x[1][0], ascending=True).take(K)
    print("First", min(K, len(sorted_cells)), "non-empty cells in non-decreasing order of size:")
    for cell in sorted_cells:
        print("Cell:", cell[0], "| Size:", cell[1][0])


def main():
    
    file_path = sys.argv[1]
    D = float(sys.argv[2])
    M = int(sys.argv[3])
    K = int(sys.argv[4])
    L = int(sys.argv[5])
    
    print(f"file path: {file_path}, D={D}, M={M}, K={K}, L={L}")

    sc = SparkContext.getOrCreate()

    assert os.path.isfile(file_path), "File or folder not found"
    rawData = sc.textFile(file_path).cache()     #RDD of strings
    inputPoints = rawData.map(lambda line: tuple(map(float, line.split(',')))).repartition(numPartitions=L)   #RDD of points

    total_points = inputPoints.count()
    print("Total number of points:", total_points)

    if total_points <= 200000:
        listOfPoints = inputPoints.collect()
        start_time_exact = time.time()
        ExactOutliers(listOfPoints, D, M, K)
        end_time_exact = time.time()
        print("ExactOutliers running time:", np.round((end_time_exact - start_time_exact)*1000, 1), "ms")

    start_time_mr = time.time()
    MRApproxOutliers(inputPoints, D, M, K)
    end_time_mr = time.time()
    print("MRApproxOutliers running time:", np.round((end_time_mr - start_time_mr)*1000, 1), "ms")

    sc.stop()


if __name__ == "__main__":
    import sys
    import time
    import os
    main()

