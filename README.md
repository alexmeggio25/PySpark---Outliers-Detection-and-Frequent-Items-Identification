# Outlier Detection with PySpark

This code implements **efficient algorithms for outlier detection** in large datasets, using both an **exact sequential algorithm** and an **approximate MapReduce-based algorithm** in PySpark, exploiting its distributed computing capabilities.  

## 📌 Overview

The code provides two main methods:  

1. **ExactOutliers**  
   - Runs on a local list of points.  
   - Computes the number of \((D,M)\)-outliers by checking pairwise distances.  
   - Returns both the total number of outliers and the first \(K\) outlier points.  
   - Suitable for **small/medium datasets** (≤ 200,000 points).  

2. **MRApproxOutliers**  
   - Runs in **parallel** using PySpark RDDs.  
   - Divides the data space into cells, then computes neighbor counts (N3, N7) to approximate outliers.  
   - Efficient for **large-scale datasets**.  
   - Reports both sure outliers and uncertain points.
 

## ⚙️ Parameters

The program expects the following command-line arguments:

```bash
python outliers.py <file_path> <D> <M> <K> <L>
```

- file_path: path to the input file (CSV with points as x,y)
- D: distance threshold
- M: minimum neighbor count to avoid being an outlier
- K: number of top outliers/cells to display
- L: number of partitions for RDDs in Spark

---

# Outlier Detection with  MRFFT (Map Reduce Farthest First Traversal)

This code implements **outlier detection in large datasets** by combining:  

- **Farthest First Traversal (FFT)** for coreset construction and center selection.  
- **Approximate MapReduce-based outlier detection** using PySpark.  

The algorithm leverages Spark for **scalability** and can handle datasets much larger than the exact approach.


## 📌 Overview

The code is based on two main components:

### 1. **Farthest First Traversal (FFT)**
- A sequential algorithm that selects representative points (centers) from a dataset.  
- Ensures diversity by iteratively picking the farthest point from the already chosen centers.  
- Used here to build a **coreset** in multiple rounds:  
  - **Round 1:** Run FFT in parallel on partitions.  
  - **Round 2:** Apply FFT again on the union of selected centers.  
  - **Round 3:** Compute the maximum distance (radius) of points from their nearest center.  

### 2. **MRApproxOutliers**
- Uses a **grid-based MapReduce approximation** to detect outliers.  
- Each point is mapped to a cell, and neighbor counts \(N3, N7\) are computed.  
- Outputs:  
  - Number of **sure outliers** (points definitely outside threshold).  
  - Number of **uncertain points** (points near the boundary).  


## ⚙️ Parameters

The program requires the following command-line arguments:

```bash
python fft_outliers.py <file_path> <M> <K> <L>
```

- file_path: path to the input file (CSV with x,y points)
- M: minimum neighbor count (outlier threshold)
- K: number of centers for FFT
- L: number of partitions for Spark RDDs

--- 

# Frequent Items Identification in Data Streams with PySpark Streaming

This code implements **frequent items identification** in a data stream using three different approaches:  

- ✅ **Exact Counting Algorithm** (baseline, requires storing all items).  
- ✅ **Reservoir Sampling** (memory-bounded random sampling).  
- ✅ **Sticky Sampling** (probabilistic approximate frequency counting).  

It processes an **unbounded stream of data** with **PySpark Streaming**, and compares the results of approximate algorithms against the exact solution.


## 📌 Overview

### 1. **Exact Algorithm**
- Stores all item frequencies in a dictionary.  
- Returns the **true frequent items** (those appearing at least `φ * n` times).  
- **Not scalable** for very large streams.  

### 2. **Reservoir Sampling**
- Maintains a **fixed-size random sample** of the stream (`m = ceil(1/φ)`).  
- Estimates frequent items by analyzing only the reservoir contents.  
- ✅ Memory efficient.  
- ❌ May misclassify some items (false positives/false negatives).  

### 3. **Sticky Sampling**
- Keeps a dynamic hash table of candidate items with approximate counters.  
- Uses sampling probability `r/n = log(1/(δφ)) / εn` to decide which new items to insert.  
- Guarantees \((ε, δ)\)-approximation for frequent item detection.  
- Much more scalable than the exact method.  


## ⚙️ Parameters

The program requires **5 arguments**:

```bash
python frequent_items_stream.py <n> <phi> <epsilon> <delta> <port>
```

- n: total number of items to process (stream length)
- phi (φ): support threshold for frequent items (fraction of n)
- epsilon (ε): error tolerance for Sticky Sampling
- delta (δ): failure probability for Sticky Sampling
- port: port for the input data stream

