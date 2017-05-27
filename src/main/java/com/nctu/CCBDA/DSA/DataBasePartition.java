package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Set;

public class DataBasePartition implements Serializable {
    private static final long serialVersionUID= 1L;
    public ArrayList<UtilityMatrix> dataBase;
    public BigInteger totalUtility;
    public DataBasePartition() {
        dataBase = new ArrayList<>();
        totalUtility = BigInteger.ZERO;
    }
    public void addUtilityMatrix(UtilityMatrix matrix) {
        dataBase.add(matrix);
        totalUtility = totalUtility.add(matrix.matrix.get(0).firstEntry().getValue()._2);
    }
    public void pruneItem(Set<Integer> unpromisingItem) {
        for(UtilityMatrix matrix: dataBase)
            matrix.pruneItem(unpromisingItem);
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for(UtilityMatrix matrix: dataBase) {
            if(builder.length() != 0)
                builder.append("\n");
            builder.append(matrix.toString());
        }
        return builder.toString();
    }
}