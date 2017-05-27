package com.nctu.CCBDA;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.UtilityMatrix;
import com.nctu.CCBDA.stage.Initializer;
import com.nctu.CCBDA.stage.L_HUSPMining;
import com.nctu.CCBDA.system.FileInfo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.math.BigInteger;
import java.util.HashSet;
import static java.lang.System.out;
public class Main implements FileInfo{
    public static void main(String argc[]) {
        if(argc.length < 1) {
            System.out.println("[ERROR - Main] leak of threshold");
            return;
        }

        SparkConf conf = new SparkConf().setAppName("Distributed and Parallel High Utility Sequential Pattern Mining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        double threshold = Double.parseDouble(argc[0]);
        /**
         *  Initialization phase
         */
        JavaRDD<UtilityMatrix> utilityMatrixRDD = Initializer.initialize(sc);
        JavaPairRDD<Long, DataBasePartition> rawPartitions = Initializer.getPartitions(utilityMatrixRDD, DEFAULT_PARTITION_NUM);
        BigInteger thresholdUtility = Initializer.getThresholdUtility(rawPartitions, threshold);
        HashSet<Integer> unpromisingItem = new HashSet<>(Initializer.getUmPromisingItem(rawPartitions, thresholdUtility));

        /**
         * L-HUSP Mining
         */
        JavaPairRDD<Long, DataBasePartition> partitions = L_HUSPMining.pruneDataBase(rawPartitions, unpromisingItem);

        out.println("Unpromising item:");
        for(int item: unpromisingItem)
            out.println(item + "");
        out.println();
        partitions.foreach(new VoidFunction<Tuple2<Long, DataBasePartition>>() {
            private static final long serialVersionUID = 0;
            public void call(Tuple2<Long, DataBasePartition> partition) {
                System.out.println("Partition " + partition._1 + "\n" + partition._2.toString());
            }
        });
        sc.close();
    }
}