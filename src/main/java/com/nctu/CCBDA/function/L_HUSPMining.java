package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
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
                return new USpan(partition._2, partition._1, null).mining(threshold).getPatternList();
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