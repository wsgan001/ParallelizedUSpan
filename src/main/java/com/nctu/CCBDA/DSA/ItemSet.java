package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

public class ItemSet implements Serializable {
    public static final long serialVersionUID = 4;
    private TreeSet<Item> set;
    public ItemSet() {
        set = new TreeSet<>();
    }
    public Item getItem(int itemID) {
        Item tmp = set.ceiling(new Item(itemID, null, null));
        if(tmp != null && tmp.id == itemID)
            return tmp;
        else
            return null;
    }
    public void addItem(Item item) {
        set.add(item);
    }
    public void removeItem(int itemID) {
        set.remove(new Item(itemID, null, null));
    }
    public Set<Item> entrySet() {
        return set;
    }
    public Item firstItem() {
        return set.first();
    }
    public int size() {
        return set.size();
    }
}