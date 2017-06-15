package com.nctu.CCBDA.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import com.nctu.CCBDA.DSA.DataBasePartition;
import com.nctu.CCBDA.DSA.Item;
import com.nctu.CCBDA.DSA.ItemSet;
import com.nctu.CCBDA.DSA.LQSTreeInfo;
import com.nctu.CCBDA.DSA.Pattern;
import com.nctu.CCBDA.DSA.Transaction;

import scala.Tuple2;
import scala.Tuple3;

public class USpan {
    private ArrayList<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>> patterns;
    private Long numOfPattern;
    private DataBasePartition database;
    private int partitionNum;
    private boolean debug = false;
    private boolean allResult;
    // private HashSet<Integer> unpromisinItem;
    public USpan(DataBasePartition database,int partitionNum,HashSet<Integer> unpromisinItem) {
        this.database = database;
        this.partitionNum = partitionNum;
        // this.unpromisinItem = unpromisinItem;
    }
    public ArrayList<Tuple2<Pattern, Tuple2<ArrayList<Integer>, BigInteger>>> getPatternList() {
        return patterns;
    }

    public Long getNumOfPattern() {
        return numOfPattern;
    }
    public USpan mining(double threshold, boolean allResult) {
        this.allResult = allResult;
        numOfPattern = 0L;
        System.out.println("Partition " + partitionNum + " start L-HUPS mining");
        ArrayList<ArrayList<LQSTreeInfo>> utilityList = new ArrayList<>();
        for(int sequenceID = 0; sequenceID < database.sequences.size(); sequenceID++) {
            ArrayList<LQSTreeInfo> initSeqInfo = new ArrayList<>();
            initSeqInfo.add(new LQSTreeInfo(0, -1, BigInteger.ZERO, database.sequences.get(sequenceID).getMatrixUtility()));
            utilityList.add(initSeqInfo);
        }
        patterns = new ArrayList<>();
        dfsInLQSTree(utilityList, new Pattern());
        return this;
    }
    private void dfsInLQSTree(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, Pattern nowPattern) {
        // System.err.printf("" + partitionNum + " ");
        if(debug) System.out.println("Now Pattern: " + nowPattern.toString());
        // I-concatenation
        if(nowPattern.size() != 0) {
            Map<Integer, Pair> swu = getI_SWUItemMap(utilityList);
            for(Map.Entry<Integer, Pair> e: swu.entrySet()) {
                if(debug)  System.out.println("I item:" + e.getKey() + ", sequenceID:" + e.getValue().sequenceID + ", swu:" + e.getValue().swu);
                if(e.getValue().swu.compareTo(database.thresholdUtility) >= 0) {
                    int itemID = e.getKey();
                    Tuple3<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger, BigInteger> info = getI_ConcatentationInfo(utilityList, itemID);
                    if(info != null) {
                        nowPattern.addIConcatItem(itemID);
                        if(info._2().compareTo(database.thresholdUtility) >= 0)
                            addPattern(nowPattern, info._2());
                        if(info._3().compareTo(BigInteger.ZERO) > 0) {
                            // System.err.printf(info._3().toString() + " ");
                            dfsInLQSTree(info._1(), nowPattern);
                        }
                        nowPattern.removeLastItem();
                    }
                }
            }
        }

        // S-concatenation
        Map<Integer, Pair> swu = getS_SWUItemMap(utilityList);
        for(Map.Entry<Integer, Pair> e: swu.entrySet()) {
            if(debug) System.out.println("S item:" + e.getKey() + ", sequenceID:" + e.getValue().sequenceID + ", swu:" + e.getValue().swu);
            if(e.getValue().swu.compareTo(database.thresholdUtility) >= 0) {
                int itemID = e.getKey();
                Tuple3<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger, BigInteger> info = getS_ConcatentationInfo(utilityList, itemID);
                if(info != null) {
                    nowPattern.addSConcatItem(itemID);
                    if(info._2().compareTo(database.thresholdUtility) >= 0)
                        addPattern(nowPattern, info._2());
                    if(info._3().compareTo(BigInteger.ZERO) > 0) {
                        // System.err.printf(info._3().toString() + " ");
                        dfsInLQSTree(info._1(), nowPattern);
                    }
                    nowPattern.removeLastItem();
                }
            }
        }
    }
    private Map<Integer, Pair> getI_SWUItemMap(ArrayList<ArrayList<LQSTreeInfo>> utilityList) {
        Map<Integer, Pair> swu = new TreeMap<>();
        for(int sequenceID = 0; sequenceID < utilityList.size(); sequenceID++) {
            ArrayList<LQSTreeInfo> seqUtility = utilityList.get(sequenceID);
            for(LQSTreeInfo info: seqUtility) {
                Transaction transaction = database.sequences.get(sequenceID);
                BigInteger localSWU = info.utility.add(info.suffixUtility);
                for(int itemID = info.itemID + 1; itemID <= transaction.maxItemID; itemID++) {
                    ItemSet itemSet = transaction.matrix.get(info.itemSetID);
                    Item nextItem = itemSet.getItem(itemID);
                    if(nextItem != null) {
                        Pair pair = swu.get(itemID);
                        if(pair == null) {
                            Pair newPair = new Pair(localSWU, sequenceID);
                            swu.put(itemID, newPair);
                        } else if(pair.sequenceID != sequenceID) {
                            pair.swu = pair.swu.add(localSWU);
                            pair.sequenceID = sequenceID;
                            swu.put(itemID, pair);
                        }
                    }
                }
            }
        }
        return swu;
    }

    private Tuple3<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger, BigInteger> getI_ConcatentationInfo(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, int itemID) {
        // System.out.println("I cat, item " + itemID);
        BigInteger newPatternUtility = BigInteger.ZERO;
        BigInteger possibleMaxUtility = BigInteger.ZERO;
        BigInteger numOfCombination = BigInteger.ZERO;
        ArrayList<ArrayList<LQSTreeInfo>> newUtilityList = new ArrayList<>();
        for(int sequenceID = 0; sequenceID < utilityList.size(); sequenceID++) {
            BigInteger maxUtilityInSeq = BigInteger.ZERO;
            BigInteger possibleMaxUtilityInSeq = BigInteger.ZERO;
            ArrayList<LQSTreeInfo> seqUtility = new ArrayList<>();
            for(LQSTreeInfo info: utilityList.get(sequenceID)) {
                // Get the same itemSet of this info
                ItemSet itemSet = database.sequences.get(sequenceID).matrix.get(info.itemSetID);
                // Find next I-Concatentation item in the same itemSet
                Item nextItem = itemSet.getItem(itemID);
                // If the same itemSet contains next I-Concatentation
                if(nextItem != null) {
                    // New utility equal to previous utility add new item utility
                    BigInteger newUtility = info.utility.add(nextItem.quality);
                    // Build new info
                    LQSTreeInfo newInfo = new LQSTreeInfo(itemID, info.itemSetID, newUtility, nextItem.suffixQuality);
                    // Record new info
                    seqUtility.add(newInfo);
                    // If the utility of this kind of combination is the largest one this 
                    // sequence up to now, then record it
                    if(newUtility.compareTo(maxUtilityInSeq) > 0) {
                        maxUtilityInSeq = newUtility;
                        if(possibleMaxUtilityInSeq.compareTo(BigInteger.ZERO) == 0)
                            possibleMaxUtilityInSeq = nextItem.suffixQuality;
                    }
                    // If the max possible utility of this combination is the largest one in
                    // this sequence up to now, then record it
                }
            }
            // Sum up all max utility in each sequence
            newPatternUtility = newPatternUtility.add(maxUtilityInSeq);
            // Sum up all possible max utility in each sequence
            possibleMaxUtility = possibleMaxUtility.add(possibleMaxUtilityInSeq);
            // Add infos of this sequences in list
            newUtilityList.add(seqUtility);
            numOfCombination = numOfCombination.add(new BigInteger("" + seqUtility.size()));
        }
        if(debug) System.out.println("I possibleMaxUtility = " + newPatternUtility.add(possibleMaxUtility).toString());
        // If and only if max possible utility of this combination higher than
        // threshold utility, return the reslt
        if(newPatternUtility.add(possibleMaxUtility).compareTo(database.thresholdUtility) >= 0)
            return new Tuple3<>(newUtilityList, newPatternUtility, numOfCombination);
        else
            return null;
    }

    private Map<Integer, Pair> getS_SWUItemMap(ArrayList<ArrayList<LQSTreeInfo>> utilityList) {
        Map<Integer, Pair> swu = new TreeMap<>();
        for(int sequenceID = 0; sequenceID < utilityList.size(); sequenceID++) {
            ArrayList<LQSTreeInfo> seqUtility = utilityList.get(sequenceID);
            Transaction transaction = database.sequences.get(sequenceID);
            for(LQSTreeInfo info: seqUtility) {
                BigInteger localSWU = info.utility.add(info.suffixUtility);
                for(int itemSetID = info.itemSetID + 1; itemSetID < transaction.matrix.size(); itemSetID++) {
                    ItemSet itemSet = transaction.matrix.get(itemSetID);
                    for(Item nextItem: itemSet.set) {
                        Pair pair = swu.get(nextItem.id);
                        if(pair == null) {
                            Pair newPair = new Pair(localSWU, sequenceID);
                            swu.put(nextItem.id, newPair);
                        } else if(pair.sequenceID != sequenceID) {
                            pair.swu = pair.swu.add(localSWU);
                            pair.sequenceID = sequenceID;
                            swu.put(nextItem.id, pair);
                        }
                    }
                }
            }
        }
        return swu;
    }
    private Tuple3<ArrayList<ArrayList<LQSTreeInfo>>, BigInteger, BigInteger> getS_ConcatentationInfo(final ArrayList<ArrayList<LQSTreeInfo>> utilityList, int itemID) {
        // System.out.println("S cat, item " + itemID);
        BigInteger newPatternUtility = BigInteger.ZERO;
        BigInteger possibleMaxUtility = BigInteger.ZERO;
        BigInteger numOfCombination = BigInteger.ZERO;
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
                        LQSTreeInfo newInfo = new LQSTreeInfo(itemID, itemSetID, newUtility, nextItem.suffixQuality);
                        seqUtility.add(newInfo);
                        if(newUtility.compareTo(maxUtilityInSeq) > 0) {
                            maxUtilityInSeq = newUtility;
                            if(possibleMaxUtilityInSeq.compareTo(BigInteger.ZERO) == 0)
                                possibleMaxUtilityInSeq = nextItem.suffixQuality;
                        }
                    }
                }
            }
            possibleMaxUtility = possibleMaxUtility.add(possibleMaxUtilityInSeq);
            newPatternUtility = newPatternUtility.add(maxUtilityInSeq);
            newUtilityList.add(seqUtility);
            numOfCombination = numOfCombination.add(new BigInteger("" + seqUtility.size()));
        }
        if(debug) System.out.println("S possibleMaxUtility = " + newPatternUtility.add(possibleMaxUtility).toString());
        if(newPatternUtility.add(possibleMaxUtility).compareTo(database.thresholdUtility) >= 0)
            return new Tuple3<>(newUtilityList, newPatternUtility, numOfCombination);
        else
            return null;
    }

    private void addPattern(Pattern pattern, BigInteger utility) {
        numOfPattern++;
        if(allResult)
            patterns.add(new Tuple2<>(pattern.clone(), new Tuple2<>(new ArrayList<Integer>(Arrays.asList(partitionNum)), utility)));
        if(numOfPattern% 10000 == 0)
            System.out.println("Number of L_HUSP in partition " + partitionNum + " up to now: " + numOfPattern);
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
    
    private class Pair {
        public BigInteger swu;
        public int sequenceID;
        public Pair(BigInteger swu, int sequenceID) {
            this.swu = swu;
            this.sequenceID = sequenceID;
        }
    }
}