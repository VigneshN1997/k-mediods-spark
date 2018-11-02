package com.bitspam;

//check imports
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MainDriver {
	public static void main(String[] args) throws IOException {

		// read arguments
		String inputPath = args[0];
		// int numOfClusters = Integer.parseInt(args[1]);
		// int numOfSampledObjects = Integer.parseInt(args[2]);
		// int numOfSamples = Integer.parseInt(args[3]);
		int numOfCores = Integer.parseInt(args[1]);
		// int numOfIteration = Integer.parseInt(args[5]);

		// setup Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("k-mediods-BITS");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// set-up output path
		FileWriter fw = new FileWriter("PAMAE_OUTPUT.txt");
		BufferedWriter bw = new BufferedWriter(fw);

		JavaRDD<Point> dataSetRDD = BITSPAM.readFile(sc, inputPath, numOfCores);
		List<Point> dataSetList = dataSetRDD.collect();
		for (int i = 0; i < dataSetList.size(); i++) {
			System.out.println(dataSetList.get(i));
		}
	}
}