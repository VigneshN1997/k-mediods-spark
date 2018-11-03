package com.bitspam;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public final class BITSPAM {
	public static String eleDivider = "\t";

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
		JavaRDD<Point> dataSet = lines.map(new BITSPAM.ParsePoint());
		return dataSet;
	}
	
	public static JavaPairRDD<String, Integer> initializeRDD(JavaSparkContext sc, int numPoints) {
		List<Integer> indicesList = IntStream.rangeClosed(0, numPoints -1).boxed().collect(Collectors.toList());
		JavaPairRDD<String,Integer> indicesRDD = sc.parallelize(indicesList).mapToPair(new BITSPAM.createPointIndex()).persist(StorageLevel.MEMORY_AND_DISK_SER());
		return indicesRDD;
	}
	
	public static class createPointIndex implements PairFunction<Integer, String, Integer> {
		
		
		public Tuple2<String, Integer> call(Integer index) {
			return new Tuple2<String, Integer>("", index);
		}
	}

	/**
	 * PsrsePoint desc : parsing text to Point object.
	 */
	public static class ParsePoint implements Function<String, Point> {

		public Point call(String line) {
			String[] toks = line.toString().split(eleDivider);
			Point pt = new Point(toks.length);
			for (int j = 0; j < toks.length; j++)
				pt.getAttr()[j] = (Double.parseDouble(toks[j]));
			return pt;
		}
	}
}