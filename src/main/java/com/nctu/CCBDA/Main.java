package com.nctu.CCBDA;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;
import com.nctu.CCBDA.DSA.Transaction;
import com.nctu.CCBDA.function.G_HUSPMining;
import com.nctu.CCBDA.function.Initializer;
import com.nctu.CCBDA.function.L_HUSPMining;
import com.nctu.CCBDA.function.PG_HUSPMining;
import com.nctu.CCBDA.system.FileInfo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.lang.System.err;
public class Main implements FileInfo{
    public static void main(String argc[]) {
        if(argc.length < 1) {
            System.err.println("[ERROR - Main] leak of threshold");
            return;
        }
        boolean debug = false;
        if(argc.length >= 2)
            debug = Boolean.parseBoolean(argc[1]);

        SparkConf conf = new SparkConf().setAppName("Distributed and Parallel High Utility Sequential Pattern Mining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        double threshold = Double.parseDouble(argc[0]);
        Logger.getRootLogger().setLevel(Level.ERROR);
        long beginTime = System.currentTimeMillis();
        System.err.println("[INFO ] START Big-HUSP ALGORITHM");
        /**
         *  Initialization phase
         */
        JavaRDD<Transaction> utilityMatrixRDD = Initializer.initialize(sc);
        JavaPairRDD<Integer, DataBasePartition> rawPartitions = Initializer.getPartitions(utilityMatrixRDD, DEFAULT_PARTITION_NUM);
        rawPartitions.cache();
        BigInteger thresholdUtility = Initializer.getThresholdUtility(rawPartitions, threshold);
        HashSet<Integer> unpromisingItem = new HashSet<>(Initializer.getUmPromisingItem(rawPartitions, thresholdUtility));
        rawPartitions = Initializer.updateInformation(rawPartitions, threshold);

        if(debug) {
            err.println("thresholdUtility = " + thresholdUtility.toString() + "\nUnpromising item:");
            for(int item: unpromisingItem)
                err.printf(item + " ");
            err.println();
        }
        /**
         * L-HUSP Mining
         */
        JavaPairRDD<Integer, DataBasePartition> partitions = L_HUSPMining.pruneDataBase(rawPartitions, unpromisingItem);
        partitions.cache();
        if(debug) {
            partitions.foreach(new VoidFunction<Tuple2<Integer, DataBasePartition>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Integer, DataBasePartition> partition) {
                    System.err.println("Partition " + partition._1 + "\n" + partition._2.toString());
                }
            });
        }

        JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> patterns = L_HUSPMining.getL_HUSP(partitions, threshold);
        if(debug) {
            patterns.foreach(new VoidFunction<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                    System.err.printf("After L_HUSP Mining, Partition %s, utility %s, %s%n", Arrays.toString(pattern._2._1.toArray(new Integer[0])), pattern._2._2, pattern._1);
                }
            });
        }

        List<BigInteger> partitionThresholdUtility = L_HUSPMining.getPartitionThresholdUtility(partitions);
        if(debug) {
            for(BigInteger it: partitionThresholdUtility)
                System.err.println("ThresholdUtility = " + it.toString());
        }
        
        /**
         * PG_HUSP Mining
         */
        JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> prunedPatterns = PG_HUSPMining.getPG_HUSP(patterns, partitions, partitionThresholdUtility, thresholdUtility);
        if(debug) {
            prunedPatterns.foreach(new VoidFunction<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                    System.err.printf("After PG_HUSP Mining, Partition %s, utility %s, %s%n", Arrays.toString(pattern._2._1.toArray(new Integer[0])), pattern._2._2, pattern._1);
                }
            });
        }

        /**
         * G_HUSP Mining
         */
        Broadcast<JavaPairRDD<Integer, DataBasePartition>> broadcastPartition = sc.broadcast(partitions);
        JavaPairRDD<Pattern, BigInteger> globalHUSP = G_HUSPMining.getG_HUSP(broadcastPartition, prunedPatterns, thresholdUtility);
        System.out.println("[INFO ] High Utility Sequential Pattern:");
        globalHUSP.foreach(new VoidFunction<Tuple2<Pattern, BigInteger>>() {
            private static final long serialVersionUID = 0;
            @Override
            public void call(Tuple2<Pattern, BigInteger> p) {
                System.err.println("\t" + p._1.toString() + ", Utility: " + p._2.toString());
            }
        });
        
        long endTime = System.currentTimeMillis();
        System.out.printf("[INFO ] Big-HUSP FINISH, %d min %d sec%n", (endTime - beginTime)/60000, (endTime - beginTime)%60000/1000);
        sc.close();
    }
}