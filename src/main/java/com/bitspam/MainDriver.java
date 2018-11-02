package com.bitspam;

//check imports
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MainDriver {
	public static void main(String[] args) throws IOException {

		// read arguments
		int i;
		String inputPath = args[0];
		// int numOfClusters = Integer.parseInt(args[1]);
		// int numOfSampledObjects = Integer.parseInt(args[2]);
		// int numOfSamples = Integer.parseInt(args[3]);
		int numOfCores = Integer.parseInt(args[1]);
		int tau = Integer.parseInt(args[2]);
		// int numOfIteration = Integer.parseInt(args[5]);

		// setup Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("k-mediods-BITS");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// set-up output path
		FileWriter fw = new FileWriter("PAMAE_OUTPUT.txt");
		BufferedWriter bw = new BufferedWriter(fw);

		JavaRDD<Point> dataSetRDD = BITSPAM.readFile(sc, inputPath, numOfCores);
		List<Point> dataSetList = dataSetRDD.collect();
		

		int dimension = dataSetList.get(0).getDimension();
		int numPoints = dataSetList.size();

		JavaRDD<PointIndex> indicesRDD = BITSPAM.initializeRDD(sc, numPoints);
		double[] minGridPoint = new double[dimension];
		double[] maxGridPoint = new double[dimension];

		for(i = 0; i < dimension; i++) {
			minGridPoint[i] = dataSetRDD.min(new DimensionComparator(i)).getAttr()[i];
			maxGridPoint[i] = dataSetRDD.max(new DimensionComparator(i)).getAttr()[i];
		}
		Gridding.initializeGridding(dimension, minGridPoint, maxGridPoint, dataSetList);
		
		Gridding.findOptCellSize(tau, numPoints, dimension, minGridPoint, maxGridPoint);
		
		JavaRDD<PointIndex> uniformRDD = indicesRDD.map(new Gridding.assignKeyToPointUG());
		
		for (i = 0; i < dataSetList.size(); i++) {
			System.out.println(dataSetList.get(i));
		}
		
	}
}