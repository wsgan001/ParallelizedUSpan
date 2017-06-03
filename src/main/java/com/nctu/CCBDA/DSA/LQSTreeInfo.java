package com.nctu.CCBDA.DSA;

import java.math.BigInteger;

public class LQSTreeInfo {
    public int itemID;
    public int itemSetID;
    public BigInteger utility;
    public BigInteger suffixUtility;
    public LQSTreeInfo(int itemID,int itemSetID,BigInteger utility,BigInteger suffixUtility) {
        this.itemID = itemID;
        this.itemSetID = itemSetID;
        this.utility = utility;
        this.suffixUtility = suffixUtility;
    }
}