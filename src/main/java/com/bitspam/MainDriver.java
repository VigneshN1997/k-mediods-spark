package com.bitspam;

//check imports
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
		double[] minGridPoint = new double[dimension];
		double[] maxGridPoint = new double[dimension];

		for(i = 0; i < dimension; i++) {
			minGridPoint[i] = dataSetRDD.min(new DimensionComparator(i)).getAttr()[i];
			maxGridPoint[i] = dataSetRDD.max(new DimensionComparator(i)).getAttr()[i];
		}
		Gridding.initializeGridding(dimension, minGridPoint, maxGridPoint, dataSetList, tau);
		Gridding.findOptCellSize(tau, numPoints);
		Gridding.applyUniformGridding();
		PAM.initializePAM(dataSetList, dimension);

		JavaPairRDD<String, Integer> uniformRDD = BITSPAM.initializeRDD(sc, numPoints).mapToPair(new Gridding.assignKeyToPointUG());
		int count = Gridding.getNumberOfKeys();
		double numPointsPerCell = (double)numPoints / (double)count;
		System.out.println("count: " + count + "  numPointsPerCell:" + numPointsPerCell);
		uniformRDD.partitionBy(new HashPartitioner(numOfCores));
		Map<String, Long> cellCount = uniformRDD.countByKey();
		JavaPairRDD<String, Integer> adaptiveRDD = Gridding.applyAdaptiveGridding(sc, uniformRDD.collect(), cellCount);
		// Gridding.printHashMaps();
		double avgNumPointsPerCell = (double)Gridding.getNumberOfKeys() / numPoints;
		JavaRDD<Integer> samplePointsRDD = sc.parallelize(adaptiveRDD.mapToPair(new Gridding.mapToList())
											.reduceByKey(new Gridding.reduceLists())
											.mapToPair(new PAM.OriginalPAM(avgNumPointsPerCell))
											.values().reduce(new Gridding.reduceLists()));

		
		List<Tuple2<String, Integer>> uniformList = adaptiveRDD.collect();
		for (i = 0; i < numPoints; i++) {
			System.out.println(uniformList.get(i)._1 + "  " + uniformList.get(i)._2);
		}
	}
}