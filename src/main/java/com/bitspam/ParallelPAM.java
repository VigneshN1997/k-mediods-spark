package com.bitspam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

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

    public static List<Integer> applyParallelPAM(JavaSparkContext sc) {
        List<Integer> indicesList = IntStream.rangeClosed(0, numPoints -1).boxed().collect(Collectors.toList());
        Collections.shuffle(indicesList);
        List<Integer> medoidIndices = new ArrayList<Integer>();
        for(int i = 0; i < k; i++) {
            medoidIndices.add(indicesList.get(i));
        }
        int iterations = 0;
        List<Integer> newMedoidsIndices = new ArrayList<Integer>();
        for(int i = 0; i < k; i++) {
            newMedoidsIndices.add(medoidIndices.get(i));
        }
        do {
            iterations++;
            newMedoidsIndices = calculateNewMedoids(newMedoidsIndices, indicesList, sc);
        } while(!stopIterations(newMedoidsIndices, medoidIndices));
        
        List<Integer> finalCellMedoids = new ArrayList<Integer>();
        for(int i = 0; i < k; i++) {
            finalCellMedoids.add(pointIndices.get(newMedoidsIndices.get(i)));
        }
        return finalCellMedoids;
    }

    public static class initKeyVal implements PairFunction<Integer, Integer, Double> {
		public Tuple2< Integer, Double> call(Integer index) {
			return new Tuple2<Integer, Double>(index, Double.MAX_VALUE);
		}
    }
    
    public static class ReplaceMedoid implements PairFunction<Tuple2<Integer, Double>, Integer, Double> {
        
        List<Integer> medoidIndices;
        int indexToReplace;

		public ReplaceMedoid(List<Integer> medoidIndices, int indexToReplace) {
            this.medoidIndices = medoidIndices; // will this cause problem in parallel?
            this.indexToReplace = indexToReplace;
        }

        @Override
        public Tuple2<Integer, Double> call(Tuple2<Integer, Double> tup) throws Exception {
            double cost = Double.MAX_VALUE;
            if(!medoidIndices.contains(tup._1)) {
                medoidIndices.set(indexToReplace, tup._1);
                cost = getTotalCost(medoidIndices);
            }
            return new Tuple2<Integer, Double>(tup._1, cost);
        }
	}

    private static List<Integer> calculateNewMedoids(List<Integer> oldMedoidsIndex, List<Integer> indicesList, JavaSparkContext sc) {
        for (int i = 0; i < oldMedoidsIndex.size(); ++i) {

            double oldTotalCost = getTotalCost(oldMedoidsIndex);
            int oriMedoidIndex = oldMedoidsIndex.get(i);

            Tuple2<Integer, Double> minCostTuple = sc.parallelize(indicesList)
                                                    .mapToPair(new ParallelPAM.initKeyVal())
                                                    .mapToPair(new ParallelPAM.ReplaceMedoid(oldMedoidsIndex, i))
                                                    .min(new CostComparator());
            
            double newTotalCost = minCostTuple._2;
            int candidateMedoidIndex = minCostTuple._1;
            
            if (newTotalCost < oldTotalCost) {
                oldMedoidsIndex.set(i, candidateMedoidIndex);
            }
            else {
                oldMedoidsIndex.set(i, oriMedoidIndex);
            }
        }
        return oldMedoidsIndex;
    }

    private static double getTotalCost(List<Integer> medoidIndices) { // ask if this should be parallelized
        double totalCost = 0;
        for (int i = 0; i < numPoints; ++i) {
            double cost = Double.MAX_VALUE;
            for (int j = 0; j < k; ++j) {
                double tempCost = preCalcResult[i][medoidIndices.get(j)];
                if (tempCost < cost) {
                    cost = tempCost;
                }
            }
            totalCost += cost;
        }
        return totalCost;
    }

    private static boolean stopIterations(List<Integer> newMedoids, List<Integer> oldMedoids) {
        boolean stopIterations = true;
        
        for(int i = 0; i < newMedoids.size(); i++){
            if(!newMedoids.contains(oldMedoids.get(i))) {
                stopIterations = false;
            }			
            oldMedoids.set(i, newMedoids.get(i));
        }
        return stopIterations;
    }

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