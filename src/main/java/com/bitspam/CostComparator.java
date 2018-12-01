package com.bitspam;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class CostComparator implements Comparator<Tuple2<Integer, Double>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
        if(o1._2 < o2._2) {
            return -1;
        }
        else if(o1._2 > o2._2) {
            return 1;
        }
        return 0;
    }
}
