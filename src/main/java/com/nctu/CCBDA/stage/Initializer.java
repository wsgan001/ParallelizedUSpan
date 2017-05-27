package com.nctu.CCBDA.stage;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.UtilityMatrix;
import com.nctu.CCBDA.system.FileInfo;

import scala.Tuple2;

public class Initializer implements FileInfo {
    public static JavaRDD<UtilityMatrix> initialize(JavaSparkContext sc) {
        return sc.textFile(DATASETS).map(new Function<String, UtilityMatrix>() {
            private static final long serialVersionUID = 0;
            @Override
            public UtilityMatrix call(String line) {
                return new UtilityMatrix(line);
            }
        });
    }

    public static JavaPairRDD<Long, DataBasePartition> getPartitions(JavaRDD<UtilityMatrix> total,long partitionNum) {
        return total.zipWithIndex().mapToPair(new PairFunction<Tuple2<UtilityMatrix, Long>, Long, UtilityMatrix>(){
            private static final long serialVersionUID = 0;
            public Tuple2<Long, UtilityMatrix> call(Tuple2<UtilityMatrix, Long> t) {
                long newPartitionID = t._2 % partitionNum;
                return new Tuple2<>(newPartitionID, t._1);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<UtilityMatrix>>, Long, DataBasePartition>(){
            private static final long serialVersionUID = 0;
            public Tuple2<Long, DataBasePartition> call(Tuple2<Long, Iterable<UtilityMatrix>> t) {
                DataBasePartition newPartition = new DataBasePartition();
                for(UtilityMatrix matrix: t._2)
                    newPartition.addUtilityMatrix(matrix);
                return new Tuple2<Long, DataBasePartition>(t._1, newPartition);
            }
        });
    }

    public static BigInteger getThresholdUtility(JavaPairRDD<Long, DataBasePartition> dataBase, double threshold) {
        BigInteger utilityOverDataBase = dataBase.map(new Function<Tuple2<Long,DataBasePartition>, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(Tuple2<Long,DataBasePartition> partition) {
                return partition._2.totalUtility;
            }
        }).reduce(new Function2<BigInteger,BigInteger,BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(BigInteger a,BigInteger b) {
                return a.add(b);
            }
        });
        return new BigDecimal(utilityOverDataBase).multiply(new BigDecimal(threshold)).toBigInteger();
    }

    public static List<Integer> getUmPromisingItem(JavaPairRDD<Long, DataBasePartition> partitions, BigInteger thresholdUtility) {
        return partitions.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, DataBasePartition>, Integer, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public Iterable<Tuple2<Integer, BigInteger>> call(Tuple2<Long, DataBasePartition> partition) {
                ArrayList<Tuple2<Integer, BigInteger>> itemUtiInThisSeq = new ArrayList<>();
                for(UtilityMatrix matrix: partition._2.dataBase) {
                    BigInteger seqUtility = matrix.matrix.get(0).firstEntry().getValue()._2;
                    HashSet<Integer> used = new HashSet<>();
                    for(TreeMap<Integer, Tuple2<BigInteger, BigInteger>> itemSet: matrix.matrix)
                        for(Map.Entry<Integer, Tuple2<BigInteger, BigInteger>> item: itemSet.entrySet()) {
                            if(!used.contains(item.getKey())) {
                                itemUtiInThisSeq.add(new Tuple2<>(item.getKey(), seqUtility));
                                used.add(item.getKey());
                            }
                        }
                }
                return itemUtiInThisSeq;
            }
        }).reduceByKey(new Function2<BigInteger, BigInteger, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(BigInteger a, BigInteger b) {
                return a.add(b);
            }
        }).filter(new Function<Tuple2<Integer, BigInteger>, Boolean>() {
            private static final long serialVersionUID = 0;
            @Override
            public Boolean call(Tuple2<Integer, BigInteger> t) {
                return t._2.compareTo(thresholdUtility) < 0;
            }
        }).map(new Function<Tuple2<Integer,BigInteger>, Integer>(){
            private static final long serialVersionUID = 0;
            @Override
            public Integer call(Tuple2<Integer, BigInteger> t) {
                return t._1;
            }
        }).collect();
    }

}