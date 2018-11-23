package com.bitspam;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class ParallelPAM {
    private static double[][] preCalcResult;
    private static List<Integer> pointIndices;
    private static int numPoints;
    private static List<Point> dataList;
	private static int dimension;
    private static int k;

    public static void initializeParallelPAM(List<Integer> pointIndices,List<Point> dataList, int dimension, int k) {
        ParallelPAM.pointIndices = pointIndices;
        ParallelPAM.k = k;
        ParallelPAM.numPoints = pointIndices.size();
        ParallelPAM.preCalcResult = new double[ParallelPAM.numPoints][ParallelPAM.numPoints];
        ParallelPAM.dataList = dataList;
		ParallelPAM.dimension = dimension;
    }
    public static void calculateDistancesBetweenPoints(JavaSparkContext sc) {
        List<Integer> indicesList = new ArrayList<Integer>();
        for(int i = 0; i < numPoints; i++) {
            indicesList.add(i);
        }
        sc.parallelize(indicesList).map(new calculateDistancesForAPoint()).collect();
    }

    // public static List<Integer> applyParallelPAM() {
    //     List<Integer> medoidIndices = new ArrayList<Integer>();
    //     for(int i = 0; i < k; i++) {
    //         medoidIndices.add(i);
    //     }
    //     int iterations = 0;
    //     List<Integer> newMedoidsIndices = null;
    //     do {
    //         iterations++;
    //         for(int i = 0; i < numPoints; i++) {
    //             if(!medoidIndices.contains(i)) {
    //                 double minDistance = Double.MAX_VALUE;
    //                 for(int j = 0; j < k; j++) {
    //                     double distance = calculateDistance(i, medoidIndices.get(j), preCalculatedResult, indices);
    //                     if(distance < minDistance) {
    //                         minDistance = distance;
    //                     }
    //                 }
    //             }
    //         }

    //         if (iterations == 1) {
    //             List<Integer> oldMedoidIndices = new ArrayList<Integer>();
    //             for (int i = 0; i < k; ++i) {
    //                 oldMedoidIndices.add(medoidIndices.get(i));
    //             }
    //             newMedoidsIndices = calculateNewMedoids(oldMedoidIndices, preCalculatedResult, indices);
    //             // free(oldMedoidsIndex);
    //         }

    //         else {
    //             newMedoidsIndices = calculateNewMedoids(newMedoidsIndices, preCalculatedResult, indices);
    //         }
    //     } while(!stopIterations(newMedoidsIndices, medoidIndices));
        
    //     List<Integer> finalCellMedoids = new ArrayList<Integer>();
    //     for(int i = 0; i < k; i++) {
    //         finalCellMedoids.add(indices.get(newMedoidsIndices.get(i)));
    //     }
    // }

    public static class calculateDistancesForAPoint implements Function<Integer, Integer> {

        @Override
        public Integer call(Integer ind) throws Exception {
            for(int i = 0; i < numPoints; i++) {
                if(ind == i) {
                    continue;
                }
                preCalcResult[ind][i] = calculateDistance(dataList.get(pointIndices.get(ind)).getAttr(), dataList.get(pointIndices.get(i)).getAttr());
            }
            return ind;
        }

        public double calculateDistance(double[] point1, double[] point2) {
            double distance = 0;
            for(int ind = 0; ind < dimension; ind++) {
                distance += (point1[ind] - point2[ind]) * (point1[ind] - point2[ind]);
            }
            return (double)Math.sqrt(distance);
        }
	} 

}