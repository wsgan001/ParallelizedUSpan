package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Set;

public class Transaction implements Serializable{
    private static final long serialVersionUID= 2L;
    /**
     * item id, item utility, suffix sum of utility exclude itself
     */
    public ArrayList<ItemSet> matrix;
    public Transaction(String raw) {
        matrix = new ArrayList<>();
        String itemSets[] = raw.split("-1");
        BigInteger totalUtility = new BigInteger(itemSets[itemSets.length - 1].split("SUtility:")[1]);
        for(int i = 0; i < itemSets.length - 1; i++) {
            String items[] = itemSets[i].split("\\[|\\]| ", -1);
            ItemSet itemSet = new ItemSet();
            for(int j = 0; j < items.length; j++) {
                if(items[j].length() != 0) {
                    int itemID = Integer.parseInt(items[j]);
                    BigInteger itemUtility = new BigInteger(items[++j]);
                    totalUtility = totalUtility.subtract(itemUtility);
                    itemSet.addItem(new Item(itemID, itemUtility, totalUtility));
                }
            }
            matrix.add(itemSet);
        }
    }

    public void pruneItem(Set<Integer> unpromisingItem) {
        BigInteger suffixUtility = BigInteger.ZERO;
        for(int i = matrix.size() - 1; i >= 0; i--) {
            ItemSet itemSet = matrix.get(i);
            ArrayList<Item> traverse = new ArrayList<>(itemSet.entrySet());
            for(int j = traverse.size() - 1; j >= 0; j--) {
                if(unpromisingItem.contains(traverse.get(j).id))
                    itemSet.removeItem(traverse.get(j).id);
                else {
                    Item modifiedItem = new Item(traverse.get(j).id, traverse.get(j).quality, suffixUtility);
                    itemSet.removeItem(traverse.get(j).id);
                    itemSet.addItem(modifiedItem);
                    suffixUtility = suffixUtility.add(traverse.get(j).quality);
                }
            }
        }
    }
    public BigInteger getMatrixUtility() {
        if(matrix.size() != 0 && matrix.get(0).size() != 0)
            return matrix.get(0).firstItem().suffixQuality.add(matrix.get(0).firstItem().quality);
        else
            return BigInteger.ZERO;
    }

    public BigInteger getUtility() {
        if(matrix.size() != 0 && matrix.get(0).size() != 0) {
            Item item = matrix.get(0).firstItem();
            return item.quality.add(item.suffixQuality);
        } else
            return BigInteger.ZERO;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(int i = 0; i < matrix.size(); i++) {
            if(i != 0)
                builder.append(",");
            builder.append("<");
            int j = 0;
            for(Item item: matrix.get(i).entrySet()) {
                if(j++ != 0)
                    builder.append(",");
                builder.append("(" + item.id + "," + item.quality + "," + item.suffixQuality + ")");
            }
            builder.append(">");
        }
        builder.append("}");
        return builder.toString();
    }
}