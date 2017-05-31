package com.nctu.CCBDA.DSA;

import java.util.ArrayList;
import java.util.Iterator;

public class Pattern extends ArrayList<ItemSetPattern> {
    public static final long serialVersionUID = 7;
    public void addItemSetPattern(ItemSetPattern itemSet) {
        add(itemSet);
    }
    public ItemSetPattern getItemSetPattern(int index) {
        return get(index);
    }
    public void removeLastItem() {
        if(get(size() - 1).size() == 1)
            remove(size()-1);
        else
            get(size() - 1).removeLastItem();
    }
    public int getLastItem() {
        return get(size() - 1).getLastItem();
    }
    public int getFirstItem() {
        return get(0).getFirstItem();
    }
    public void addIConcatItem(int itemID) {
        get(size() - 1).addItem(itemID);
    }
    public void addSConcatItem(int itemID) {
        ItemSetPattern patSet = new ItemSetPattern();
        patSet.addItem(itemID);
        addItemSetPattern(patSet);
    }
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Pattern))
            return false;
        Pattern pattern = (Pattern)obj;
        Iterator<ItemSetPattern> itA = pattern.iterator();
        Iterator<ItemSetPattern> itB = iterator();
        while(itA.hasNext() && itB.hasNext()) {
            ItemSetPattern itemSetA = (ItemSetPattern)itA.next();
            ItemSetPattern itemSetB = (ItemSetPattern)itB.next();
            if(!itemSetA.equals(itemSetB))
                return false;
        }
        if(!itA.hasNext() && !itB.hasNext())
            return true;
        else
            return false;
    }
    @Override
    public Pattern clone() {
        Pattern ret = new Pattern();
        for(ItemSetPattern itemSet: this)
            ret.addItemSetPattern(itemSet.clone());
        return ret;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(int index = 0; index < size(); index++) {
            if(index != 0)
                builder.append(",");
            builder.append(getItemSetPattern(index));
        }
        builder.append("}");
        return builder.toString();
    }
}