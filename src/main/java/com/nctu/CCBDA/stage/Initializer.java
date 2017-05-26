package com.nctu.CCBDA.stage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

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

    public static BigInteger getThresholdUtility(JavaRDD<UtilityMatrix> umRDD, double threshold) {
        return null;
    }

    public static List<Integer> getUmPromisingItem(JavaRDD<UtilityMatrix> umRDD, BigInteger thresholdUtility) {
        umRDD.flatMapToPair(new PairFlatMapFunction<UtilityMatrix, Integer, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public Iterable<Tuple2<Integer, BigInteger>> call(UtilityMatrix matrix) {
                BigInteger seqUtility = matrix.matrix.get(0).firstEntry().getValue()._2;
                HashSet<Integer> used = new HashSet<>();
                ArrayList<Tuple2<Integer, BigInteger>> itemUtiInThisSeq = new ArrayList<>();
                for(TreeMap<Integer, Tuple2<BigInteger, BigInteger>> itemSet: matrix.matrix)
                    for(Map.Entry<Integer, Tuple2<BigInteger, BigInteger>> item: itemSet.entrySet()) {
                        if(!used.contains(item.getKey()))
                            itemUtiInThisSeq.add(new Tuple2<>(item.getKey(), seqUtility));
                    }
                return itemUtiInThisSeq;
            }
        }).reduceByKey(new Function2<BigInteger, BigInteger, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(BigInteger a,BigInteger b) {
                return a.add(b);
            }
        });
        return null;
    }
}