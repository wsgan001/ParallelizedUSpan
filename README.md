# Final Project of Cloud Computing and Big Date Analytics Class

## Main Purpose
- Implement High-Utility Pattern Mining algorithm with Apache Spark(TM)
- The algorithm was introduced in this [paper](http://www.eecs.yorku.ca/research/techreports/2016/EECS-2016-03.pdf)

## Build With
* [Gradle](https://gradle.org/) - Dependency Management
* Apache Spark 1.6.1
## How to run
```
spark-submit com.nctu.CCBDA.Main --master local[2] ParallelizedUSpan-all-1.0.jar input_file threshold_utility [-options]
```
### options
```
-sdg; open system debug
-adg; open algorithm debug
-np; number_of_partition ; number of partition
-ou folder_name; set output folder name
-ga algorithm_name; DP|DFS
-ct; only count candidate pattern
-gp; apply global pruning
```