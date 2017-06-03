package com.nctu.CCBDA;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;
import com.nctu.CCBDA.DSA.Transaction;
import com.nctu.CCBDA.function.DFSandAlphaBetaPruning;
import com.nctu.CCBDA.function.G_HUSPMining;
import com.nctu.CCBDA.function.Initializer;
import com.nctu.CCBDA.function.L_HUSPMining;
import com.nctu.CCBDA.function.PG_HUSPMining;
import com.nctu.CCBDA.function.PatternUtilityCauculater;
import com.nctu.CCBDA.system.FileInfo;
import com.nctu.CCBDA.system.LoggerHelper;

import scala.Tuple2;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
public class Main implements FileInfo {
    /**
     * input_file threshold_utility [-options]
     * 
     * options:
     * -sdg ; system debug
     * -adg ; algorithm debug
     * -np number_of_partition ; number of partition
     * -ou folder_name ; set output folder name
     */
    public static void main(String argc[]) {
        if(argc.length < 2) {
            LoggerHelper.errLog("Leak of input_file or threshold");
            return;
        }

        SparkConf conf = new SparkConf().setAppName("Distributed and Parallel High Utility Sequential Pattern Mining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        long beginTime = System.currentTimeMillis();
        LoggerHelper.infoLog("Application ID: " + sc.sc().applicationId());
        LoggerHelper.infoLog("START Big-HUSP ALGORITHM");

        String inputFile = argc[0];
        String outputFolder = null;
        double threshold = Double.parseDouble(argc[1]);
        boolean aDebug = false;
        boolean sDebug = false;
        int partitionNum = 1;
        for(int i = 2; i < argc.length; i++) {
            if(argc[i].equals("-sdg"))
                sDebug = true;
            if(argc[i].equals("-adg"))
                aDebug = true;
            if(argc[i].equals("-np"))
                partitionNum = Integer.parseInt(argc[(i++)+1]);
            if(argc[i].equals("-ou"))
                outputFolder = argc[(i++)+1];
        }
        if(!sDebug)
            Logger.getRootLogger().setLevel(Level.ERROR);

        LoggerHelper.infoLog("\tInput file: " + inputFile);
        LoggerHelper.infoLog("\tThreshold: " + String.format("%.3f%%", threshold * 100f));
        LoggerHelper.infoLog("\tNumber of Patition: " + partitionNum);
        LoggerHelper.infoLog("\tOpen Algorithm Debug: " + aDebug);
        LoggerHelper.infoLog("\tOpen System Debug: " + sDebug);
        LoggerHelper.infoLog("\tSave Result: " + outputFolder);
        /**
         *  Initialization
         */
        LoggerHelper.infoLog("INITIALIZATION");
        JavaRDD<Transaction> utilityMatrixRDD = Initializer.initialize(sc, inputFile);
        JavaPairRDD<Integer, DataBasePartition> rawPartitions = Initializer.getPartitions(utilityMatrixRDD, partitionNum);
        // rawPartitions.cache();
        BigInteger thresholdUtility = Initializer.getThresholdUtility(rawPartitions, threshold);
        LoggerHelper.infoLog("\tThesholdUtility:" + thresholdUtility.toString());
        HashSet<Integer> unpromisingItem = new HashSet<>(Initializer.getUmPromisingItem(rawPartitions, thresholdUtility));
        rawPartitions = Initializer.updateInformation(rawPartitions, threshold);

        if(aDebug) {
            StringBuilder builder = new StringBuilder();
            builder.append("Unpromising item:");
            for(int item: unpromisingItem)
                builder.append(item + " ");
            LoggerHelper.debugLog(builder.toString());
            rawPartitions.foreach(new VoidFunction<Tuple2<Integer, DataBasePartition>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Integer, DataBasePartition> partition) {
                    LoggerHelper.debugLog("Partition " + partition._1 + ", Threshold Utility:" + partition._2.thresholdUtility.toString());
                    System.err.println(partition._2.toString());
                }
            });
        }
        /**
         * L-HUSP Mining
         */
        LoggerHelper.infoLog("L-HUSP MINING");
        LoggerHelper.infoLog("\tPrune data base");
        JavaPairRDD<Integer, DataBasePartition> partitions = L_HUSPMining.pruneDataBase(rawPartitions, unpromisingItem);
        // partitions.cache();
        if(aDebug) {
            partitions.foreach(new VoidFunction<Tuple2<Integer, DataBasePartition>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Integer, DataBasePartition> partition) {
                    LoggerHelper.debugLog("Partition " + partition._1 + ", Threshold Utility:" + partition._2.thresholdUtility.toString());
                    System.err.println(partition._2.toString());
                }
            });
        }
        LoggerHelper.infoLog("\tStart to find L_HUSP");
        JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> patterns = L_HUSPMining.getL_HUSP(partitions, threshold);
        // patterns.cache();
        LoggerHelper.infoLog("\tNumber of L_HUSP " + PG_HUSPMining.getRDDSize1(patterns).toString());
        if(aDebug) {
            patterns.foreach(new VoidFunction<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                    LoggerHelper.debugLog(String.format("L_HUSP Mining result, Partition %s, utility %s, %s", Arrays.toString(pattern._2._1.toArray(new Integer[0])), pattern._2._2, pattern._1));
                }
            });
        }

        List<BigInteger> partitionThresholdUtility = L_HUSPMining.getPartitionThresholdUtility(partitions);
        if(aDebug) {
            for(int partitionID = 0; partitionID < partitionThresholdUtility.size(); partitionID++)
                LoggerHelper.debugLog("Partition:" + partitionID + ", ThresholdUtility:" + partitionThresholdUtility.get(partitionID).toString());
        }
        
        /**
         * PG_HUSP Mining
         */
        LoggerHelper.infoLog("PG_HUSP MINING");
        JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> prunedPatterns = PG_HUSPMining.getPG_HUSP(patterns, partitions, partitionThresholdUtility, thresholdUtility);
        // prunedPatterns.cache();
        LoggerHelper.infoLog("\tNumber of PG_HUSP " + PG_HUSPMining.getRDDSize1(prunedPatterns).toString());
        if(aDebug) {
            prunedPatterns.foreach(new VoidFunction<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>>() {
                private static final long serialVersionUID = 0;
                public void call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                    LoggerHelper.debugLog(String.format("PG_HUSP Mining result, Partition %s, utility %s, %s", Arrays.toString(pattern._2._1.toArray(new Integer[0])), pattern._2._2, pattern._1));
                }
            });
        }

        /**
         * G_HUSP Mining
         */
        LoggerHelper.infoLog("G_HUSP MINING");
        Map<Integer, DataBasePartition> broadcastPartition = partitions.collectAsMap();
        PatternUtilityCauculater cauculater = new DFSandAlphaBetaPruning();
        JavaPairRDD<Pattern, BigInteger> globalHUSP = G_HUSPMining.getG_HUSP(
                                                                        broadcastPartition,
                                                                        prunedPatterns,
                                                                        thresholdUtility,
                                                                        cauculater);
        // globalHUSP.cache();
        if(aDebug) {
            LoggerHelper.debugLog("High Utility Sequential Pattern:");
            globalHUSP.foreach(new VoidFunction<Tuple2<Pattern, BigInteger>>() {
                private static final long serialVersionUID = 0;
                @Override
                public void call(Tuple2<Pattern, BigInteger> p) {
                    LoggerHelper.debugLog("\t" + p._1.toString() + ", Utility: " + p._2.toString());
                }
            });
        }
        LoggerHelper.infoLog("\tNumber of G_HUSP " + PG_HUSPMining.getRDDSize2(globalHUSP).toString());
        if(outputFolder != null) {
            LoggerHelper.infoLog("SAVE RESULT");
            globalHUSP.saveAsTextFile("output_result");
        }
        long endTime = System.currentTimeMillis();
        LoggerHelper.infoLog(String.format("Big-HUSP FINISH, %d min %.3f sec", (endTime - beginTime)/60000, (endTime - beginTime)%60000/1000f));
        sc.close();
    }
}