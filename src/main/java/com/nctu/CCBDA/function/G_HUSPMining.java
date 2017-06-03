package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;

import scala.Tuple2;

public class G_HUSPMining {
    public static JavaPairRDD<Pattern, BigInteger> getG_HUSP(Map<Integer, DataBasePartition> database,
                            JavaPairRDD<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> l_HUSP,
                            BigInteger thresholdUtility,
                            PatternUtilityCauculater cauculater) {

        return l_HUSP.filter(new Function<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>,Boolean>() {
            private static final long serialVersionUID = 0;
            @Override
            public Boolean call(Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>> pattern) {
                HashSet<Integer> hadCauculated = new HashSet<>(pattern._2._1);

                BigInteger globalUtility = pattern._2._2;
                for(Map.Entry<Integer, DataBasePartition> e: database.entrySet())
                    if(!hadCauculated.contains(e.getKey())) {
                        BigInteger utility = cauculater.getUtility(e.getValue(), pattern._1);
                        // System.err.println(pattern._1.toString() + " in partition:" + partition._1 + ", utility:" + utility.toString());
                        globalUtility = globalUtility.add(utility);
                    }
                // System.err.println(pattern._1.toString() + ", global utility:" + utility.toString());
                if(globalUtility.compareTo(thresholdUtility) >= 0)
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