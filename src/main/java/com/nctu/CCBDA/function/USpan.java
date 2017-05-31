package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Item;
import com.nctu.CCBDA.DSA.ItemSet;
import com.nctu.CCBDA.DSA.LQSTreeInfo;
import com.nctu.CCBDA.DSA.Pattern;

import scala.Tuple2;

public class USpan {
    private ArrayList<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>> patterns;
    private DataBasePartition database;
    private int partitionNum;
    // private HashSet<Integer> unpromisinItem;
    public USpan(DataBasePartition database,int partitionNum,HashSet<Integer> unpromisinItem) {
        this.database = database;
        this.partitionNum = partitionNum;
        // this.unpromisinItem = unpromisinItem;
    }
    public ArrayList<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>> getPatternList() {
        return patterns;
    }
    public USpan mining(double threshold) {
        // System.out.printf("thresholdUtility: %s%n", database.thresholdUtility.toString());
        ArrayList<ArrayList<LQSTreeInfo>> utilityList = new ArrayList<>();
        for(int sequenceID = 0; sequenceID < database.sequences.size(); sequenceID++) {
            ArrayList<LQSTreeInfo> initSeqInfo = new ArrayList<>();
            initSeqInfo.add(new LQSTreeInfo(0, -1, BigInteger.ZERO));
            utilityList.add(initSeqInfo);
        }
        patterns = new ArrayList<>();
        dfsInLQSTree(utilityList, new Pattern());
        return this;
    }
    private void dfsInLQSTree(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, Pattern nowPattern) {
        // System.out.println("Now Pattern: " + nowPattern.toString());
        if(utilityList.size() == 0)
            return;
        // I-concatenation
        if(nowPattern.size() != 0) {
            int lastItemID = nowPattern.getLastItem();
            for(int itemID = lastItemID + 1; itemID <= database.maxItemID; itemID++) {
                // if(!unpromisinItem.contains(itemID)) {   // Width Pruning
                    Tuple2<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger> info = getI_ConcatentationInfo(utilityList, itemID);
                    if(info != null) {                      // Depth Pruning
                        nowPattern.addIConcatItem(itemID);
                        if(info._2.compareTo(database.thresholdUtility) >= 0)
                            addPattern(nowPattern, info._2);
                        dfsInLQSTree(info._1, nowPattern);
                        nowPattern.removeLastItem();
                    }
                // }
            }
        }

        for(int itemID = 1; itemID <= database.maxItemID; itemID++) {
            // if(!unpromisinItem.contains(itemID)) {   // Width Pruning
                Tuple2<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger> info = getS_ConcatentationInfo(utilityList, itemID);
                /**
                 * 
                 */
                if(info != null) {                      // Depth Pruning
                    nowPattern.addSConcatItem(itemID);
                    if(info._2.compareTo(database.thresholdUtility) >= 0)
                        addPattern(nowPattern, info._2);
                    dfsInLQSTree(info._1, nowPattern);
                    nowPattern.removeLastItem();
                }
            // }
        }
    }

    private Tuple2<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger> getI_ConcatentationInfo(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, int itemID) {
        // System.out.println("I cat, item " + itemID);
        BigInteger newPatternUtility = BigInteger.ZERO;
        BigInteger possibleMaxUtility = BigInteger.ZERO;
        ArrayList<ArrayList<LQSTreeInfo>> newUtilityList = new ArrayList<>();
        for(int sequenceID = 0; sequenceID < utilityList.size(); sequenceID++) {
            BigInteger maxUtilityInSeq = BigInteger.ZERO;
            BigInteger possibleMaxUtilityInSeq = BigInteger.ZERO;
            ArrayList<LQSTreeInfo> seqUtility = new ArrayList<>();
            for(LQSTreeInfo info: utilityList.get(sequenceID)) {
                ItemSet itemSet = database.sequences.get(sequenceID).matrix.get(info.itemSetID);
                Item nextItem = itemSet.getItem(itemID);
                if(nextItem != null) {
                    BigInteger newUtility = info.utility.add(nextItem.quality);
                    LQSTreeInfo newInfo = new LQSTreeInfo(itemID, info.itemSetID, newUtility);
                    seqUtility.add(newInfo);
                    if(newUtility.compareTo(maxUtilityInSeq) > 0)
                        maxUtilityInSeq = newUtility;
                    BigInteger newPossibleMaxUtilityInSeq = newUtility.add(nextItem.suffixQuality);
                    if(newPossibleMaxUtilityInSeq.compareTo(possibleMaxUtilityInSeq) >= 0)
                        possibleMaxUtilityInSeq = newPossibleMaxUtilityInSeq;
                }
            }
            possibleMaxUtility = possibleMaxUtility.add(possibleMaxUtilityInSeq);
            newPatternUtility = newPatternUtility.add(maxUtilityInSeq);
            newUtilityList.add(seqUtility);
        }
        // System.out.println("possibleMaxUtility = " + possibleMaxUtility.toString());
        if(possibleMaxUtility.compareTo(database.thresholdUtility) >= 0)
            return new Tuple2<>(newUtilityList, newPatternUtility);
        else
            return null;
    }

    private Tuple2<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger> getS_ConcatentationInfo(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, int itemID) {
        // System.out.println("S cat, item " + itemID);
        BigInteger newPatternUtility = BigInteger.ZERO;
        BigInteger possibleMaxUtility = BigInteger.ZERO;
        ArrayList<ArrayList<LQSTreeInfo>> newUtilityList = new ArrayList<>();
        for(int sequenceID = 0; sequenceID < utilityList.size(); sequenceID++) {
            BigInteger maxUtilityInSeq = BigInteger.ZERO;
            BigInteger possibleMaxUtilityInSeq = BigInteger.ZERO;
            ArrayList<LQSTreeInfo> seqUtility = new ArrayList<>();
            for(LQSTreeInfo info: utilityList.get(sequenceID)) {
                for(int itemSetID = info.itemSetID + 1; itemSetID < database.sequences.get(sequenceID).matrix.size(); itemSetID++) {
                    ItemSet itemSet = database.sequences.get(sequenceID).matrix.get(itemSetID);
                    Item nextItem = itemSet.getItem(itemID);
                    // System.out.printf("seqID:%d, setID:%d, itemid:%d, item utility:", sequenceID, itemSetID, itemID);
                    if(nextItem != null) {
                        // System.out.printf("%s%n", nextItem.quality);
                        BigInteger newUtility = info.utility.add(nextItem.quality);
                        LQSTreeInfo newInfo = new LQSTreeInfo(itemID, itemSetID, newUtility);
                        seqUtility.add(newInfo);
                        if(newUtility.compareTo(maxUtilityInSeq) > 0)
                            maxUtilityInSeq = newUtility;
                        BigInteger newPossibleMaxUtilityInSeq = newUtility.add(nextItem.suffixQuality);
                        if(newPossibleMaxUtilityInSeq.compareTo(possibleMaxUtilityInSeq) >= 0)
                            possibleMaxUtilityInSeq = newPossibleMaxUtilityInSeq;
                    } else {
                        // System.out.printf("null%n");
                    }
                }
            }
            possibleMaxUtility = possibleMaxUtility.add(possibleMaxUtilityInSeq);
            newPatternUtility = newPatternUtility.add(maxUtilityInSeq);
            newUtilityList.add(seqUtility);
        }
        // System.out.println("possibleMaxUtility = " + possibleMaxUtility.toString());
        if(possibleMaxUtility.compareTo(database.thresholdUtility) >= 0)
            return new Tuple2<>(newUtilityList, newPatternUtility);
        else
            return null;
    }

    private void addPattern(Pattern pattern, BigInteger utility) {
        patterns.add(new Tuple2<>(pattern.clone(), new Tuple2<>(new ArrayList<Integer>(Arrays.asList(partitionNum)), utility)));
    }    
    private void showUtilityList(ArrayList<ArrayList<LQSTreeInfo>> list) {
        System.out.printf("{");
        for(ArrayList<LQSTreeInfo> st: list) {
            System.out.printf("<");
            for(LQSTreeInfo info: st)
                System.out.printf("(%d,%d,%s)", info.itemID, info.itemSetID, info.utility.toString());
            System.out.printf(">");
        }
        System.out.println("}");
    }
}