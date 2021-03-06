package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;

import scala.Tuple2;

public class L_HUSPMining {
    public static JavaPairRDD<Integer, DataBasePartition> pruneDataBase(JavaPairRDD<Integer, DataBasePartition> dataBase, Set<Integer> unpromisingItem) {
        return dataBase.mapToPair(new PairFunction<Tuple2<Integer, DataBasePartition>, Integer, DataBasePartition>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<Integer, DataBasePartition> call(Tuple2<Integer, DataBasePartition> partition) {
                partition._2.pruneItem(unpromisingItem);
                return partition;
            }
        });
    }

    public static JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> getL_HUSP(JavaPairRDD<Integer, DataBasePartition> dataBase, double threshold) {
        return dataBase.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, DataBasePartition>, Pattern, Tuple2<ArrayList<Integer>, BigInteger>>() {
            private static final long serialVersionUID = 0;
            @Override
            public Iterable<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>> call(Tuple2<Integer, DataBasePartition> partition) {
                return new USpan(partition._2, partition._1, null).mining(threshold, true).getPatternList();
            }
        }).reduceByKey(new Function2<Tuple2<ArrayList<Integer>,BigInteger>,Tuple2<ArrayList<Integer>,BigInteger>,Tuple2<ArrayList<Integer>,BigInteger>>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<ArrayList<Integer>,BigInteger> call(Tuple2<ArrayList<Integer>,BigInteger> a,Tuple2<ArrayList<Integer>,BigInteger> b) {
                a._1.addAll(b._1);
                return new Tuple2<>(a._1, a._2.add(b._2));
            }
        });
    }

    public static Long getNumOfL_HUSP(JavaPairRDD<Integer, DataBasePartition> dataBase, double threshold) {
        return dataBase.map(new Function<Tuple2<Integer, DataBasePartition>, Long>() {
            private static final long serialVersionUID = 0;
            @Override
            public Long call(Tuple2<Integer, DataBasePartition> partition) {
                return new USpan(partition._2, partition._1, null).mining(threshold, false).getNumOfPattern();
            }
        }).reduce(new Function2<Long, Long, Long>() {
            private static final long serialVersionUID = 0;
            @Override
            public Long call(Long a, Long b) {
                return a + b;
            }
        });
    }

    public static List<BigInteger> getPartitionThresholdUtility(JavaPairRDD<Integer, DataBasePartition> dataBase) {
        List<Tuple2<Integer, BigInteger>> partitionThresholdUtilityTuple;
        partitionThresholdUtilityTuple = dataBase.mapToPair(new PairFunction<Tuple2<Integer,DataBasePartition>, Integer, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<Integer, BigInteger> call(Tuple2<Integer, DataBasePartition> partition) {
                return new Tuple2<>(partition._1, partition._2.thresholdUtility);
            }
        }).sortByKey().collect();
        List<BigInteger> partitionThresholdUtility = new ArrayList<>();
        for(int i = 0; i < partitionThresholdUtilityTuple.size(); i++) {
            if(i != partitionThresholdUtilityTuple.get(i)._1)
                System.err.println("[ERROR] sortByKey() not work !!!! sortByKey() not work !!!! sortByKey() not work");
            partitionThresholdUtility.add(partitionThresholdUtilityTuple.get(i)._2);
        }
        return partitionThresholdUtility;
    }
}