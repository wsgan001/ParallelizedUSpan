package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;

import scala.Tuple2;

public class PG_HUSPMining {
    public static JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> getPG_HUSP(JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> l__HUSP,
                            JavaPairRDD<Integer, DataBasePartition> partitions,
                            List<BigInteger> partitionThresholdUtility,
                            BigInteger thresholdUtility) {
        return l__HUSP.reduceByKey(new Function2<Tuple2<ArrayList<Integer>,BigInteger>,Tuple2<ArrayList<Integer>,BigInteger>,Tuple2<ArrayList<Integer>,BigInteger>>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<ArrayList<Integer>,BigInteger> call(Tuple2<ArrayList<Integer>,BigInteger> a,Tuple2<ArrayList<Integer>,BigInteger> b) {
                a._1.addAll(b._1);
                return new Tuple2<>(a._1, a._2.add(b._2));
            }
        }).filter(new Function<Tuple2<Pattern,Tuple2<ArrayList<Integer>,BigInteger>>,Boolean>() {
            private static final long serialVersionUID = 0;
            @Override
            public Boolean call(Tuple2<Pattern,Tuple2<ArrayList<Integer>,BigInteger>> t) {
                BigInteger mas = t._2._2.add(thresholdUtility);
                for(int partitionID: t._2._1)
                    mas = mas.subtract(partitionThresholdUtility.get(partitionID));
                
                if(mas.compareTo(thresholdUtility) >= 0)
                    return true;
                else
                    return false;
            }
        });
    }
}