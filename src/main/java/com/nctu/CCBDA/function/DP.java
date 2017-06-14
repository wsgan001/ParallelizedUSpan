package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Item;
import com.nctu.CCBDA.DSA.ItemSet;
import com.nctu.CCBDA.DSA.ItemSetPattern;
import com.nctu.CCBDA.DSA.Pattern;
import com.nctu.CCBDA.DSA.Transaction;

public class DP extends PatternUtilityCauculater {
    private static final long serialVersionUID = 948794879487L;
    
    public BigInteger getUtility(DataBasePartition database,Pattern pattern) {
        BigInteger sumUtility = BigInteger.ZERO;
        for(Transaction transaction: database.sequences)
            sumUtility = sumUtility.add(getUtilityInTransaction(pattern, transaction));
        return sumUtility;
    }
    private BigInteger getUtilityInTransaction(Pattern pattern, Transaction transaction) {
        ArrayList<BigInteger> dp = new ArrayList<>();
        // initialize
        int firstItem = pattern.getFirstItem();
        boolean noStartItem = true;
        for(int itemSetID = 0; itemSetID < transaction.matrix.size(); itemSetID++) {
            ItemSet itemSet = transaction.matrix.get(itemSetID);
            Item nextItem = itemSet.getItem(firstItem);
            if(nextItem != null) {
                dp.add(nextItem.quality);
                noStartItem = false;
            } else
                dp.add(null);
        }
        if(noStartItem)
            return BigInteger.ZERO;

        // Dynamic programing
        // Traverse itemSet of the pattern
        for(int itemSetPatternID = 0; itemSetPatternID < pattern.size(); itemSetPatternID++) {
            ItemSetPattern itemSetPattern = pattern.getItemSetPattern(itemSetPatternID);
            if(itemSetPatternID == 0 && itemSetPattern.size() == 1)
                continue;
            // Traverse item of the itemSet
            for(int itemPatternID = (itemSetPatternID == 0 ? 1 : 0); itemPatternID < itemSetPattern.size(); itemPatternID++) {
                int nextItem = itemSetPattern.getItem(itemPatternID);
                if(itemPatternID != 0) {
                    // I-Concatenation
                    boolean allNull = true;
                    for(int itemSetID = 0; itemSetID < transaction.matrix.size(); itemSetID++) {
                        ItemSet itemSet = transaction.matrix.get(itemSetID);
                        Item item = itemSet.getItem(nextItem);
                        BigInteger pre = dp.get(itemSetID);
                        if(item != null && pre != null) {
                            dp.set(itemSetID, item.quality.add(pre));
                            allNull = false;
                        } else
                            dp.set(itemSetID, null);
                    }
                    if(allNull)
                        return BigInteger.ZERO;
                } else {
                    // S-Concatenation
                    BigInteger prefixMax = null;
                    boolean allNull = true;
                    for(int itemSetID = 0; itemSetID < transaction.matrix.size(); itemSetID++) {
                        ItemSet itemSet = transaction.matrix.get(itemSetID);
                        Item item = itemSet.getItem(nextItem);
                        BigInteger pre = dp.get(itemSetID);
                        if(item != null && prefixMax != null) {
                            dp.set(itemSetID, item.quality.add(prefixMax));
                            allNull = false;
                        } else
                            dp.set(itemSetID, null);
                        if(pre != null && (prefixMax == null || pre.compareTo(prefixMax) > 0))
                            prefixMax = pre;
                    }
                    if(allNull)
                        return BigInteger.ZERO;
                }
            }
        }

        BigInteger ret = null;
        for(BigInteger utility: dp)
            if(utility != null && (ret == null || utility.compareTo(ret) > 0))
                ret = utility;
        return ret;
    }
    public String getAlgorithmName() {
        return "Dynamic Programing";
    }
}