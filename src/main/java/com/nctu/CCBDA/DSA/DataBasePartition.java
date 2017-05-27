package com.nctu.CCBDA.DSA;

import java.math.BigInteger;
import java.util.ArrayList;

public class DataBasePartition {
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
}