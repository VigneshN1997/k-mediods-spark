package com.bitspam;

import java.io.Serializable;
// hashcode,equals, compareto, add, subtract, multiply, getnorm
public class Point implements Serializable {
	private double[] attr = null;
	private int dimension;
	private double weight = 1;

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

	public double getWeight() {
		return this.weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
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

	public static Point multiply(Point a, double k) {
        Point res = new Point(a.getDimension());
        res.add(a);
        res.multiply(k);
        return res;
	}
	
	public Point add(Point other) {
		double[] otherAttr = other.getAttr();
        for (int i = 0; i < dimension; i++) {
        	attr[i] += otherAttr[i];
        }
        return this;
	}

	public static Point subtraction(Point a, Point b) {
		Point res = new Point(a.getDimension());
		double[] resAttr = new double[a.getDimension()];
		double[] aAttr = a.getAttr();
		double[] bAttr = b.getAttr();
        for (int i = 0; i < a.getDimension(); i++) {
            resAttr[i] = aAttr[i] - bAttr[i];
		}
		res.setAttr(resAttr);
        return res;
	}
	
	public Point multiply(double k) {
        for (int i = 0; i < dimension; i++) {
        	attr[i] *= k;
        }
        return this;
	}
	
	public double getNormToThe2() {
        double res = 0;
        for (int i = 0; i < dimension; i++) {
            res += attr[i]*attr[i];
        }
        return res;
    }

    // euclidean distance as norm
    public double getNorm() {
        return Math.sqrt(getNormToThe2());
    }

	public int compareTo(Point point) {
		double[] pointAttr = point.getAttr();
        for (int i = 0; i < dimension; i++) {
            if (attr[i] != pointAttr[i]) {
                double dif = attr[i] - pointAttr[i];
                if (dif > 0d) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }
        return 0;
    } 
}
