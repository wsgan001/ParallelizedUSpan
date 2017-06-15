# Final Project of Cloud Computing and Big Date Analytics Class

## Main Purpose
- Implement High-Utility Pattern Mining algorithm with Apache Spark(TM)
- The algorithm was introduced in this [paper](http://www.eecs.yorku.ca/research/techreports/2016/EECS-2016-03.pdf)

## Build With
* [Gradle](https://gradle.org/) - Dependency Management
* Apache Spark 1.6.1
## How to run
- Rebuild project
```
gradle gJar
```
- Put build\libs\ParallelizedUSpan-all-1.0.jar to your master spark machine
- Use following instruction to run the program
```
spark-submit com.nctu.CCBDA.Main --master local[2] ParallelizedUSpan-all-1.0.jar input_file threshold_utility [-options]
```
### options
- for user
```
-ou folder_name; Set the name of output folder in hadoop file system
```
- for debug
```
-sdg; Open system debug
-adg; Open algorithm debug
-np number_of_partition; Number of partition
-ga algorithm_name[DP|DFS]; The algorithm which check whether the pattern is High-Utility-Sequetail-Pattern
-ct; Only count the number of candidate pattern
-gp; Apply global pruning
```
- default
```
spark-submit com.nctu.CCBDA.Main --master local[2] ParallelizedUSpan-all-1.0.jar input_file threshold_utility -np 1 -ga DFS
```