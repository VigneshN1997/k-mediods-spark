package com.bitspam;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public final class BITSPAM {
	public static String eleDivider = ",";

	/**
	 * Read and parsing input files
	 * 
	 * @param sc
	 * @param inputPath
	 * @param numOfCores
	 * @return dataSet
	 */
	public static JavaRDD<Point> readFile(JavaSparkContext sc, String inputPath, int numOfCores) {
		// read input file(s) and load to RDD
		JavaRDD<String> lines = sc.textFile(inputPath, numOfCores); // numOfCores = minPartitions
		JavaRDD<Point> dataSet = lines.map(new BITSPAM.ParsePoint()).persist(StorageLevel.MEMORY_AND_DISK_SER()); // why
																													// is
																													// persist
																													// needed
																													// here
		return dataSet;
	}

	/**
	 * PsrsePoint desc : parsing text to Point object.
	 */
	public static class ParsePoint implements Function<String, Point> {

		public Point call(String line) {
			String[] toks = line.toString().split(eleDivider);
			Point pt = new Point(toks.length);
			for (int j = 0; j < toks.length; j++)
				pt.getAttr()[j] = (Float.parseFloat(toks[j]));
			return pt;
		}
	}
}