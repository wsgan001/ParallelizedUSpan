package com.nctu.CCBDA.stage;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import com.nctu.CCBDA.DSA.DataBasePartition;

import scala.Tuple2;

public class L_HUSPMining {
    public static JavaPairRDD<Long, DataBasePartition> pruneDataBase(JavaPairRDD<Long, DataBasePartition> dataBase, Set<Integer> unpromisingItem) {
        return dataBase.mapToPair(new PairFunction<Tuple2<Long, DataBasePartition>, Long, DataBasePartition>() {
            private static final long serialVersionUID = 0;
            @Override
            public Tuple2<Long, DataBasePartition> call(Tuple2<Long, DataBasePartition> partition) {
                partition._2.pruneItem(unpromisingItem);
                return partition;
            }
        });
    }
}