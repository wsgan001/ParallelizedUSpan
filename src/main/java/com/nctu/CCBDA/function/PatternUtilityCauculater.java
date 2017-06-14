package com.nctu.CCBDA.function;

import java.io.Serializable;
import java.math.BigInteger;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Pattern;

public abstract class PatternUtilityCauculater implements Serializable {
    private static final long serialVersionUID = 9487;
    public abstract BigInteger getUtility(DataBasePartition database,Pattern pattern);
    public abstract String getAlgorithmName();
}