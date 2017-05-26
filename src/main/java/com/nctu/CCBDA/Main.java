package com.nctu.CCBDA;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.nctu.CCBDA.DSA.UtilityMatrix;
import com.nctu.CCBDA.stage.Initializer;
import com.nctu.CCBDA.system.FileInfo;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


import java.math.BigInteger;
import java.util.HashSet;
import java.util.Map;
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
         *  Initialization utility matrix(UM)
         */
        JavaRDD<UtilityMatrix> utilityMatrixRDD = Initializer.initialize(sc);
        BigInteger thresholdUtility = Initializer.getThresholdUtility(utilityMatrixRDD, threshold);
        HashSet<Integer> unPromisingItem = new HashSet<>(Initializer.getUmPromisingItem(utilityMatrixRDD, thresholdUtility));

        // utilityMatrixRDD.foreach((matrix) -> {
        //     StringBuffer outBuf = new StringBuffer();
        //     outBuf.append("<");
        //     for(int i = 0; i < matrix.matrix.size(); i++) {
        //         if(i != 0)
        //             outBuf.append(", ");
        //         outBuf.append("{");
        //         for(Map.Entry<Integer, Tuple2<BigInteger, BigInteger>> e: matrix.matrix.get(i).entrySet())
        //             outBuf.append("(" + e.getValue()._1.toString() + "," + e.getValue()._2.toString() + ")");
        //         outBuf.append("}");
        //     }
        //     outBuf.append(">");
        //     out.println(out.toString());
        // });
        // JavaPairRDD<String, Integer> wordData = rawData.mapToPair((word) -> new Tuple2<String, Integer>(word, 1));
        // JavaPairRDD<String, Integer> countData = wordData.reduceByKey((a, b) -> a + b);
        // JavaPairRDD<String, Integer> filteredData = countData.filter((tup) -> tup._1.contains("t"));
        // filteredData.saveAsTextFile(argc[1]);
        sc.close();
    }
}