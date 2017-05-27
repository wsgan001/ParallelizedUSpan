package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import scala.Tuple2;

public class UtilityMatrix implements Serializable{
    private static final long serialVersionUID= 2L;
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

    public void pruneItem(Set<Integer> unpromisingItem) {
        BigInteger suffixUtility = BigInteger.ZERO;
        for(int i = matrix.size() - 1; i >= 0; i--) {
            TreeMap<Integer, Tuple2<BigInteger, BigInteger>> itemSet = matrix.get(i);
            ArrayList<Map.Entry<Integer, Tuple2<BigInteger, BigInteger>>> traverse = new ArrayList<>(itemSet.entrySet());
            for(int j = traverse.size() - 1; j >= 0; j--) {
                if(unpromisingItem.contains(traverse.get(j).getKey()))
                    itemSet.remove(traverse.get(j).getKey());
                else {
                    itemSet.put(traverse.get(j).getKey(), new Tuple2<>(traverse.get(j).getValue()._1, suffixUtility));
                    suffixUtility = suffixUtility.add(traverse.get(j).getValue()._1);
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(int i = 0; i < matrix.size(); i++) {
            if(i != 0)
                builder.append(",");
            builder.append("<");
            int j = 0;
            for(Map.Entry<Integer, Tuple2<BigInteger, BigInteger>> item: matrix.get(i).entrySet()) {
                if(j++ != 0)
                    builder.append(",");
                builder.append("(" + item.getKey().toString() + "," + item.getValue()._1 + "," + item.getValue()._2 + ")");
            }
            builder.append(">");
        }
        builder.append("}");
        return builder.toString();
    }
}