package com.nctu.CCBDA.DSA;

import java.math.BigInteger;

public class LQSTreeInfo {
    public int itemID;
    public int itemSetID;
    public BigInteger utility;
    public LQSTreeInfo(int itemID,int itemSetID,BigInteger utility) {
        this.itemID = itemID;
        this.itemSetID = itemSetID;
        this.utility = utility;
    }
}