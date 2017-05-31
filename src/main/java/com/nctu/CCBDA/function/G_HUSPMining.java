package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;

import scala.Tuple2;

public class G_HUSPMining {
    public static JavaPairRDD<Pattern, BigInteger> getG_HUSP(Broadcast<JavaPairRDD<Integer, DataBasePartition>> database,
                            JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> l_HUSP,
                            BigInteger thresholdUtility) {

        return l_HUSP.filter(new Function<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>,Boolean>() {
            private static final long serialVersionUID = 0;
            @Override
            public Boolean call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                HashSet<Integer> hadCauculated = new HashSet<>(pattern._2._1);
                BigInteger utility = database.value().map(new Function<Tuple2<Integer, DataBasePartition>, BigInteger>() {
                    private static final long serialVersionUID = 0;
                    @Override
                    public BigInteger call(Tuple2<Integer, DataBasePartition> partition) {
                        if(hadCauculated.contains(partition._1))
                            return BigInteger.ZERO;
                        else {
                            BigInteger utility = new CauculateUtility(partition._2).cauculate(pattern._1);
                            // System.err.println(pattern._1.toString() + " in partition:" + partition._1 + ", utility:" + utility.toString());
                            return utility;
                        }
                    }
                }).reduce(new Function2<BigInteger,BigInteger,BigInteger>() {
                    private static final long serialVersionUID = 0;
                    @Override
                    public BigInteger call(BigInteger a,BigInteger b) {
                        return a.add(b);
                    }
                }).add(pattern._2._2);
                // System.err.println(pattern._1.toString() + ", global utility:" + utility.toString());
                if(utility.compareTo(thresholdUtility) >= 0)
                    return true;
                else
                    return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>, Pattern, BigInteger>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<Pattern, BigInteger> call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                return new Tuple2<>(pattern._1, pattern._2._2);
            }
        });
    }
}