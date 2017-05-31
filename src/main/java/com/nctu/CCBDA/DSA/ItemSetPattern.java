package com.nctu.CCBDA.DSA;

import java.util.ArrayList;
import java.util.Iterator;

public class ItemSetPattern extends ArrayList<Integer> {
    public static final long serialVersionUID = 6;
    public void addItem(int itemID) {
        add(itemID);
    }
    public int getItem(int index) {
        return get(index);
    }
    public int getLastItem() {
        return get(size() - 1);
    }
    public int getFirstItem() {
        return get(0);
    }
    public void removeLastItem() {
        remove(size() - 1);
    }
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ItemSetPattern))
            return false;
        Iterator<Integer> itA = ((ItemSetPattern)obj).iterator();
        Iterator<Integer> itB = this.iterator();
        while(itA.hasNext() && itB.hasNext()) {
            int itemA = (Integer)itA.next();
            int itemB = (Integer)itB.next();
            if(itemA != itemB)
                return false;
        }
        if(!itA.hasNext() && !itB.hasNext())
            return true;
        else
            return false;
    }
    @Override
    public ItemSetPattern clone() {
        ItemSetPattern ret = new ItemSetPattern();
        for(int item: this)
            ret.addItem(item);
        return ret;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("<");
        for(int index = 0; index < size(); index++) {
            if(index != 0)
                builder.append(",");
            builder.append(getItem(index));
        }
        builder.append(">");
        return builder.toString();
    }
}