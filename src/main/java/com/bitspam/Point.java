package com.bitspam;

import java.io.Serializable;

public class Point implements Serializable {
	private double[] attr = null;
	private int dimension;

	public Point(int dimension) {
		this.attr = new double[dimension];
		this.dimension = dimension;
	}

	public int getDimension() {
		return dimension;
	}
	
	public double[] getAttr() {
		return attr;
	}

	public void setAttr(double[] ptrs) {
		this.attr = ptrs;
	}

	public String toString() {
		String result = "";

		for (int i = 0; i < attr.length; i++) {
			if (i != attr.length - 1)
				result += attr[i] + ",";
			else
				result += attr[i];
		}

		return result;
	}

	public boolean isSamePoint(Point other) {
		int count = 0;

		for (int i = 0; i < this.attr.length; i++) {
			double[] temp = other.getAttr();
			if (this.attr[i] == temp[i]) {
				count++;
			}
		}
		if (count == this.attr.length) {
			return true;
		}
		return false;
	}
}
