package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Item;
import com.nctu.CCBDA.DSA.ItemSet;
import com.nctu.CCBDA.DSA.ItemSetPattern;
import com.nctu.CCBDA.DSA.LQSTreeInfo;
import com.nctu.CCBDA.DSA.Pattern;
import com.nctu.CCBDA.DSA.Transaction;

public class DFSandAlphaBetaPruning extends PatternUtilityCauculater {
    private static final long serialVersionUID = 94879487;
    @Override
    public BigInteger getUtility(DataBasePartition database,Pattern pattern) {
        BigInteger sum = BigInteger.ZERO;
        for(Transaction transaction: database.sequences)
            sum = sum.add(utilityInSequence(pattern, transaction));
        return sum;
    }
    private BigInteger utilityInSequence(Pattern pattern, Transaction transaction) {
        ArrayList<LQSTreeInfo> infos = new ArrayList<>();
        int firstItemID = pattern.getFirstItem();
        for(int itemSetID = 0; itemSetID < transaction.matrix.size(); itemSetID++) {
            ItemSet itemSet = transaction.matrix.get(itemSetID);
            Item item = itemSet.getItem(firstItemID);
            if(item != null)
                infos.add(new LQSTreeInfo(firstItemID, itemSetID, item.quality, item.suffixQuality));
        }
        for(int itemSetPatID = 0; infos.size() != 0 && itemSetPatID < pattern.size(); itemSetPatID++) {
            ItemSetPattern itemSetPattern = pattern.getItemSetPattern(itemSetPatID);
            for(int itemPatID = (itemSetPatID == 0 ? 1 : 0); infos.size() != 0 && itemPatID < itemSetPattern.size(); itemPatID++) {
                ArrayList<LQSTreeInfo> newInfos = new ArrayList<>();
                int itemID = itemSetPattern.get(itemPatID);
                for(LQSTreeInfo info: infos) {
                    if(itemPatID == 0)
                        getS_ConcatentationInfo(itemID, info, transaction, newInfos);
                    else
                        getI_ConcatentationInfo(itemID, info, transaction, newInfos);
                }
                infos = newInfos;
            }
        }
        BigInteger utility = BigInteger.ZERO;
        for(LQSTreeInfo info: infos)
            if(info.utility.compareTo(utility) > 0)
                utility = info.utility;
        return utility;
    }

    private void getI_ConcatentationInfo(int itemID, LQSTreeInfo info, Transaction transaction, ArrayList<LQSTreeInfo> newInfos) {
        Item item = transaction.matrix.get(info.itemSetID).getItem(itemID);
        if(item != null)
            newInfos.add(new LQSTreeInfo(itemID, info.itemSetID, info.utility.add(item.quality), item.suffixQuality));
    }
    
    private void getS_ConcatentationInfo(int itemID, LQSTreeInfo info, Transaction transaction, ArrayList<LQSTreeInfo> newInfos) {
        for(int itemSetID = info.itemSetID + 1; itemSetID < transaction.matrix.size(); itemSetID++) {
            Item item = transaction.matrix.get(itemSetID).getItem(itemID);
            if(item != null)
                newInfos.add(new LQSTreeInfo(itemID, itemSetID, info.utility.add(item.quality), item.suffixQuality));
        }
    }
    public String getAlgorithmName() {
        return "DFS and Alpha-Beta-Pruning";
    }
}