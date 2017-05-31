package com.nctu.CCBDA.function;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Item;
import com.nctu.CCBDA.DSA.ItemSet;
import com.nctu.CCBDA.DSA.Transaction;
import com.nctu.CCBDA.system.FileInfo;

import scala.Tuple2;

public class Initializer implements FileInfo {
    /**
     * Convert all transaction in database to {@link UtilityMatrix UtilityMatrix}
     * 
     * @param sc spark context
     * @return return a RDD containing all utility matrix
     */
    public static JavaRDD<Transaction> initialize(JavaSparkContext sc) {
        return sc.textFile(DATASETS).map(new Function<String, Transaction>() {
            private static final long serialVersionUID = 0;
            @Override
            public Transaction call(String line) {
                return new Transaction(line);
            }
        });
    }
    /**
     * Partition RDD into a few {@link DataBasePartition DataBasePartition}
     * 
     * @param total all utility matrix
     * @param partitionNum the specified number of partitions
     * @return and RDD containing partition id and DataBasePartition
     * 
     * @see #initialize(JavaSparkContext)
     */
    public static JavaPairRDD<Integer, DataBasePartition> getPartitions(JavaRDD<Transaction> total,int partitionNum) {
        return total.zipWithIndex().mapToPair(new PairFunction<Tuple2<Transaction, Long>, Integer, Transaction>(){
            private static final long serialVersionUID = 0;
            public Tuple2<Integer, Transaction> call(Tuple2<Transaction, Long> t) {
                int newPartitionID = (int)(t._2 % partitionNum);
                return new Tuple2<>(newPartitionID, t._1);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Integer, Iterable<Transaction>>, Integer, DataBasePartition>() {
            private static final long serialVersionUID = 0;
            public Tuple2<Integer, DataBasePartition> call(Tuple2<Integer, Iterable<Transaction>> t) {
                DataBasePartition newPartition = new DataBasePartition();
                for(Transaction matrix: t._2)
                    newPartition.addUtilityMatrix(matrix);
                return new Tuple2<Integer, DataBasePartition>(t._1, newPartition);
            }
        });
    }
    /**
     * Get threshold utility from the partitioned database
     * 
     * @param dataBase partitioned database
     * @param threshold in percentage
     * @return the threshold utility
     * 
     * @see #getPartitions(JavaRDD, long)
     */
    public static BigInteger getThresholdUtility(JavaPairRDD<Integer, DataBasePartition> dataBase, double threshold) {
        BigInteger utilityOverDataBase = dataBase.map(new Function<Tuple2<Integer,DataBasePartition>, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(Tuple2<Integer, DataBasePartition> partition) {
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
    /**
     * Get unpromising item from partitioned database
     * 
     * @param partitions partitioned database
     * @param thresholdUtility threshold utility
     * @return unpromising item list
     * 
     * @see #getThresholdUtility(JavaPairRDD, double)
     */
    public static List<Integer> getUmPromisingItem(JavaPairRDD<Integer, DataBasePartition> partitions, BigInteger thresholdUtility) {
        return partitions.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, DataBasePartition>, Integer, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public Iterable<Tuple2<Integer, BigInteger>> call(Tuple2<Integer, DataBasePartition> partition) {
                ArrayList<Tuple2<Integer, BigInteger>> itemUtiInThisSeq = new ArrayList<>();
                for(Transaction transaction: partition._2.sequences) {
                    BigInteger seqUtility = transaction.matrix.get(0).firstItem().suffixQuality.add(transaction.matrix.get(0).firstItem().quality);
                    HashSet<Integer> used = new HashSet<>();
                    for(ItemSet itemSet: transaction.matrix)
                        for(Item item: itemSet.entrySet()) {
                            partition._2.maxItemID = Math.max(partition._2.maxItemID, item.id);
                            if(!used.contains(item.id)) {
                                itemUtiInThisSeq.add(new Tuple2<>(item.id, seqUtility));
                                used.add(item.id);
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

    public static JavaPairRDD<Integer, DataBasePartition> updateInformation(JavaPairRDD<Integer, DataBasePartition> partitions, double threshold) {
        return partitions.mapToPair(new PairFunction<Tuple2<Integer, DataBasePartition>, Integer, DataBasePartition>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<Integer, DataBasePartition> call(Tuple2<Integer, DataBasePartition> partition) {
                for(Transaction transaction: partition._2.sequences)
                    for(ItemSet itemSet: transaction.matrix)
                        for(Item item: itemSet.entrySet())
                            partition._2.maxItemID = Math.max(partition._2.maxItemID, item.id);
                partition._2.thresholdUtility = new BigDecimal(partition._2.totalUtility).multiply(new BigDecimal(threshold)).toBigInteger();
                return partition;
            }
        });
    }
}