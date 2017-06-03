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
        return l__HUSP.filter(new Function<Tuple2<Pattern,Tuple2<ArrayList<Integer>,BigInteger>>,Boolean>() {
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

    public static BigInteger getRDDSize1(JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> rdd) {
        if(rdd.isEmpty())
            return BigInteger.ZERO;
        return rdd.map(new Function<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> t) {
                return BigInteger.ONE;
            }
        }).reduce(new Function2<BigInteger, BigInteger, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(BigInteger a, BigInteger b) {
                return a.add(b);
            }
        });
    }

    public static BigInteger getRDDSize2(JavaPairRDD<Pattern, BigInteger> rdd) {
        if(rdd.isEmpty())
            return BigInteger.ZERO;
        return rdd.map(new Function<Tuple2<Pattern, BigInteger>, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(Tuple2<Pattern, BigInteger> t) {
                return BigInteger.ONE;
            }
        }).reduce(new Function2<BigInteger, BigInteger, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public BigInteger call(BigInteger a, BigInteger b) {
                return a.add(b);
            }
        });
    }
}