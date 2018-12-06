package com.bitspam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PAM {
	private static List<Point> dataList;
	private static int dimension;
	public static void initializePAM(List<Point> dataList, int dimension) {
		PAM.dataList = dataList;
		PAM.dimension = dimension;
	}

	public static double finalClusteringError(List<Point> kMedoids) {
		double totalCost = 0;
		int numOfClusters = kMedoids.size();
		int numPoints = dataList.size();
		for (int i = 0; i < numPoints; ++i) {
			double cost = Double.MAX_VALUE;
			double[] point1 = dataList.get(i).getAttr();
            for (int j = 0; j < numOfClusters; ++j) {
				double tempCost = 0;
				double[] point2 = kMedoids.get(j).getAttr();
				for(int ind = 0; ind < dimension; ind++) {
					tempCost += (point1[ind] - point2[ind]) * (point1[ind] - point2[ind]);
				}
				tempCost = (double)Math.sqrt(tempCost);
                if (tempCost < cost) {
                    cost = tempCost;
                }
            }
            totalCost += cost;
		}
		return totalCost;
	}

	public static class OriginalPAM implements PairFunction<Tuple2<String, List<Integer>>, String, List<Integer>> { // UG means uniform gridding
        double avgNumPointsPerCell;

		public OriginalPAM(double avgNumPointsPerCell) {
			this.avgNumPointsPerCell = avgNumPointsPerCell;
        }
        
		public Tuple2<String, List<Integer>> call(Tuple2<String, List<Integer>> griddedCell) {
			List<Integer> indices = griddedCell._2;
            int numPoints = indices.size();
            int k = (int)Math.ceil(avgNumPointsPerCell/ numPoints);
			double[][] preCalculatedResult = new double[numPoints][numPoints];
			Collections.shuffle(indices);
			List<Integer> medoidIndices = new ArrayList<Integer>();
			for(int i = 0; i < k; i++) {
				medoidIndices.add(i);
			}
			
            int iterations = 0;
            List<Integer> newMedoidsIndices = null;
            do {
				iterations++;
				// for(int i = 0; i < numPoints; i++) {
				// 	if(!medoidIndices.contains(i)) {
				// 		double minDistance = Double.MAX_VALUE;
				// 		for(int j = 0; j < k; j++) {
				// 			double distance = calculateDistance(i, medoidIndices.get(j), preCalculatedResult, indices);
				// 			if(distance < minDistance) {
				// 				minDistance = distance;
				// 			}
				// 		}
				// 	}
				// }

				if (iterations == 1) {
					List<Integer> oldMedoidIndices = new ArrayList<Integer>();
					for (int i = 0; i < k; ++i) {
						oldMedoidIndices.add(medoidIndices.get(i));
					}
					newMedoidsIndices = calculateNewMedoids(oldMedoidIndices, preCalculatedResult, indices);
					// free(oldMedoidsIndex);
				}

				else {
					newMedoidsIndices = calculateNewMedoids(newMedoidsIndices, preCalculatedResult, indices);
				}
            } while(!stopIterations(newMedoidsIndices, medoidIndices));
            
            List<Integer> finalCellMedoids = new ArrayList<Integer>();
            for(int i = 0; i < k; i++) {
                finalCellMedoids.add(indices.get(newMedoidsIndices.get(i)));
            }
            return new Tuple2<String, List<Integer>>(griddedCell._1, finalCellMedoids);
		}

		private double calculateDistance(int i, int j, double[][] preCalculatedResult, List<Integer> indiceList) {
			if(preCalculatedResult[i][j] == 0) {
				double distance = 0;
				double[] point1 = dataList.get(indiceList.get(i)).getAttr();
				double[] point2 =  dataList.get(indiceList.get(j)).getAttr();
				for(int ind = 0; ind < dimension; ind++) {
					distance += (point1[ind] - point2[ind]) * (point1[ind] - point2[ind]);
				}
				preCalculatedResult[i][j] = (double)Math.sqrt(distance);
			}
			return preCalculatedResult[i][j];
		}
	
		private boolean stopIterations(List<Integer> newMedoids, List<Integer> oldMedoids) {
			boolean stopIterations = true;
			
			for(int i = 0; i < newMedoids.size(); i++){
				if(!newMedoids.contains(oldMedoids.get(i))) {
					stopIterations = false;
				}			
				oldMedoids.set(i, newMedoids.get(i));
			}
			return stopIterations;
		}
	
		private List<Integer> calculateNewMedoids(List<Integer> oldMedoidsIndex, double[][] preCalcResult, List<Integer> indices) {
			for (int i = 0; i < oldMedoidsIndex.size(); ++i) {
				double oldTotalCost = getTotalCost(oldMedoidsIndex, preCalcResult, indices);
				double newTotalCost = Double.MAX_VALUE;
				int oriMedoidIndex = oldMedoidsIndex.get(i);
				int candidateMedoidIndex = -1;

				for (int j = 0; j < indices.size(); ++j) {
					if(!oldMedoidsIndex.contains(j)) {
						oldMedoidsIndex.set(i, j); // TODO: check first if j is not in medoid list
						double tempTotalCost = getTotalCost(oldMedoidsIndex, preCalcResult, indices);
						if (tempTotalCost < newTotalCost) {
							newTotalCost = tempTotalCost;
							candidateMedoidIndex = j;
						}
					}
					
				}
				if (newTotalCost < oldTotalCost) {
					oldMedoidsIndex.set(i, candidateMedoidIndex);
				}
				else {
					oldMedoidsIndex.set(i, oriMedoidIndex);
				}
			}
			return oldMedoidsIndex;
		}
		private double getTotalCost(List<Integer> newMedoidsIndex, double[][] preCalcResult, List<Integer> indices) {
			double totalCost = 0;
			for (int i = 0; i < indices.size(); ++i) {
				double cost = Double.MAX_VALUE;
				for (int j = 0; j < newMedoidsIndex.size(); ++j) {
					double tempCost = calculateDistance(i, newMedoidsIndex.get(j), preCalcResult, indices);
					if (tempCost < cost) {
						cost = tempCost;
					}
				}
				totalCost += cost;
			}
			return totalCost;
		}
	}
}