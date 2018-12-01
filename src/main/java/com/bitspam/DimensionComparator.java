package com.bitspam;

import java.io.Serializable;
import java.util.Comparator;

public class DimensionComparator implements Comparator<Point>, Serializable {
	
	private int iDim;
	
	public DimensionComparator(int iDim) {
		this.iDim = iDim;
	}
	public int compare(Point o1, Point o2) {
		if(o1.getAttr()[iDim] < o2.getAttr()[iDim]) {
			return -1;
		}
		else if(o1.getAttr()[iDim] > o2.getAttr()[iDim]) {
			return 1;
		}
		return 0;
	}

}
