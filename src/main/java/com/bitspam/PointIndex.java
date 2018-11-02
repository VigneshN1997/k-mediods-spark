package com.bitspam;

import java.io.Serializable;

public class PointIndex implements Serializable {
	private StringBuilder key = new StringBuilder("");
	private int index = -1;

	public PointIndex(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public StringBuilder getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key.append(key);
	}

	public String toString() {
		String result = "Key:";
		result += this.key + " Value:" + this.index;
		return result;
	}
}
