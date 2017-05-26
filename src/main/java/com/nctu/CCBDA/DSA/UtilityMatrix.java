package com.nctu.CCBDA.DSA;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;

import scala.Tuple2;

public class UtilityMatrix {
    /**
     * item id, item utility, suffix sum of utility exclude itself
     */
    public ArrayList<TreeMap<Integer, Tuple2<BigInteger, BigInteger>>> matrix;
    public UtilityMatrix(String raw) {
        matrix = new ArrayList<>();
        String itemSets[] = raw.split("-1");
        BigInteger totalUtility = new BigInteger(itemSets[itemSets.length - 1].split("SUtility:")[1]);
        for(int i = 0; i < itemSets.length - 1; i++) {
            String items[] = itemSets[i].split("\\[|\\]| ", -1);
            TreeMap<Integer, Tuple2<BigInteger, BigInteger>> itemSet = new TreeMap<>();
            for(int j = 0; j < items.length; j++) {
                if(items[j].length() != 0) {
                    int itemID = Integer.parseInt(items[j]);
                    BigInteger itemUtility = new BigInteger(items[++j]);
                    totalUtility = totalUtility.subtract(itemUtility);
                    itemSet.put(itemID, new Tuple2<>(itemUtility, totalUtility));
                }
            }
            matrix.add(itemSet);
        }
    }
}