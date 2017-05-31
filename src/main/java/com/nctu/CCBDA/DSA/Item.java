package com.nctu.CCBDA.DSA;

import java.io.Serializable;
import java.math.BigInteger;

public class Item implements Comparable<Item>, Serializable {
    public static final long serialVersionUID = 3;
    public int id;
    public BigInteger quality;
    public BigInteger suffixQuality;
    public Item(int id,BigInteger quality,BigInteger suffixQuality) {
        this.id = id;
        this.quality = quality;
        this.suffixQuality = suffixQuality;
    }
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Item))
            return false;
        return id == ((Item)obj).id;
    }
    @Override
    public int compareTo(Item obj) {
        return id - obj.id;
    }
}