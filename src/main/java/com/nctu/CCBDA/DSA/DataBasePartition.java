package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Set;

public class DataBasePartition implements Serializable {
    private static final long serialVersionUID= 1L;
    public ArrayList<Transaction> sequences;
    public BigInteger totalUtility;
    public BigInteger thresholdUtility;
    public Integer maxItemID;
    public DataBasePartition() {
        sequences = new ArrayList<>();
        totalUtility = BigInteger.ZERO;
        maxItemID = 0;
    }
    public void addUtilityMatrix(Transaction matrix) {
        sequences.add(matrix);
        totalUtility = totalUtility.add(matrix.getMatrixUtility());
    }
    public void pruneItem(Set<Integer> unpromisingItem) {
        for(Transaction transaction: sequences)
            transaction.pruneItem(unpromisingItem);
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for(Transaction matrix: sequences) {
            if(builder.length() != 0)
                builder.append("\n");
            builder.append(matrix.toString());
        }
        return builder.toString();
    }
}