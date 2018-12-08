package com.bitspam;

//check imports
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
		String master = args[0];
		String inputPath = args[1];
		// int numOfSampledObjects = Integer.parseInt(args[2]);
		// int numOfSamples = Integer.parseInt(args[3]);
		int numOfCores = Integer.parseInt(args[2]);
		int tau = Integer.parseInt(args[3]);
		int numOfClusters = Integer.parseInt(args[4]);
		int numOfIterations = Integer.parseInt(args[5]);

		// setup Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("k-mediods-BITS");
//		JavaSparkContext sc = new JavaSparkContext(master, "k-mediods-BITS", sparkConf);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		// set-up output path
//		FileWriter fw = new FileWriter("OUTPUT.txt", true);
		System.out.println("\n\n\n result for: " + inputPath + " k:" + numOfClusters + " tau: " + tau + "\n");
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
		System.out.println("ug done, cell count: " + count + " avg  numPointsPerCell:" + numPointsPerCell + "\n");
		uniformRDD.partitionBy(new HashPartitioner(numOfCores));
		Map<String, Long> cellCount = uniformRDD.countByKey();
		JavaPairRDD<String, Integer> adaptiveRDD = Gridding.applyAdaptiveGridding(sc, uniformRDD.collect(), cellCount);
		System.out.println("adaptive gridding done, num cells:" + Gridding.getNumberOfKeys() + "\n");
		// Gridding.printHashMaps();
		double avgNumPointsPerCell = (double)numPoints / Gridding.getNumberOfKeys();
		List<Integer> samplePoints = adaptiveRDD.mapToPair(new Gridding.mapToList())
											.reduceByKey(new Gridding.reduceLists())
											.mapToPair(new PAM.OriginalPAM(avgNumPointsPerCell))
											.values().reduce(new Gridding.reduceLists());
		System.out.println("sample size: " + samplePoints.size() + "\n");

		ParallelPAM.initializeParallelPAM(samplePoints, dataSetList, dimension, numOfClusters);
		ParallelPAM.calculateDistancesBetweenPoints(sc);
		List<Integer> medoidIndices = ParallelPAM.applyParallelPAM(sc);
		List<Point> bestSeed = new ArrayList<Point>();
		for(Integer mIndex: medoidIndices) {
			bestSeed.add(dataSetList.get(mIndex));
		}

		double totalCost = PAM.finalClusteringError(bestSeed);

		
//		fw.write("final medoids:");
//		for(i = 0; i < numOfClusters; i++) {
//			System.out.print(medoidIndices.get(i) + ",");
//		}
//		System.out.println();
		System.out.println("total cost after phase 1:" + totalCost + "\n");
		Weiszfeld.initializeWeiszfeld(dataSetList, numPoints, 0.01, numOfClusters, dimension, -1);
		
		List<Point> finalMedoids = null;
		for(i = 0; i < numOfIterations; i++) {
			finalMedoids = Weiszfeld.refinement(sc, bestSeed);
			for(int j = 0; j < numOfClusters; j++) {
				bestSeed.set(j, finalMedoids.get(j));
			}
			double cost = PAM.finalClusteringError(finalMedoids);
			System.out.println("total cost after phase 2 iteration:" + i + " : " + cost + "\n");
		}
//		fw.close();


		// List<Tuple2<String, Integer>> uniformList = adaptiveRDD.collect();
		// for (i = 0; i < numPoints; i++) {
		// 	System.out.println(uniformList.get(i)._1 + "  " + uniformList.get(i)._2);
		// }
	}
}