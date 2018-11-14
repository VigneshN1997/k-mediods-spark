package com.bitspam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Gridding {


    private static HashMap<String, Integer> globalPositioningIndex = new HashMap<String, Integer>();
    private static HashMap<String, Tuple2<Double[], Double[]>> keyToCell = new HashMap<String, Tuple2<Double[], Double[]>>();
    private static double initialCellSize;
    private static int globalIndex = 0;
    private static double[] minGridPoint = null;
    private static double[] maxGridPoint = null;
    private static int dimension;
    private static List<Point> dataSetList;
    private static int tau;

    public static void initializeGridding(int dimension, double[] minGridPoint, double[] maxGridPoint, List<Point> dataSetList, int tau) {
        Gridding.dimension = dimension;
        Gridding.minGridPoint = minGridPoint;
        Gridding.maxGridPoint = maxGridPoint;
        Gridding.dataSetList = dataSetList;
        Gridding.tau = tau;
    }

    public static void findOptCellSize(int tau, int numPoints) {
        double volume = 1;
        double cellVolume;
        int i;
        for(i=0; i < dimension;i++) {
            volume *= (maxGridPoint[i] - minGridPoint[i]);
        }
        cellVolume = volume * tau / numPoints;
        initialCellSize = Math.pow(cellVolume,1.0/dimension);
    }

    // This function is a recursive function which is used for filling the hash map data structure with cell numbers and cell bounds 
    public static void getCellKeys(int currDimNum, double[] minPointAcc, double[] maxPointAcc, int[] cellNumArr) {
        if(currDimNum == dimension) {
            String cellNumStr = convertCellNumArrToString(cellNumArr);
            Double[] minPointArr = new Double[dimension];
            Double[] maxPointArr = new Double[dimension];
            for(int i = 0; i < dimension; i++) {
                minPointArr[i] = minPointAcc[i];
                maxPointArr[i] = maxPointAcc[i];
            }
            keyToCell.put(Integer.toString(globalIndex), new Tuple2<Double[],Double[]>(minPointArr, maxPointArr));
            globalPositioningIndex.put(cellNumStr, globalIndex++);
            return;
        }
        int numPartitions = (int)Math.ceil((maxGridPoint[currDimNum] - minGridPoint[currDimNum]) / initialCellSize);
        double partitionStart = minGridPoint[currDimNum];
        double partitionEnd = partitionStart + initialCellSize;
        for(int i = 0; i < numPartitions; i++) {
            minPointAcc[currDimNum] = partitionStart;
            maxPointAcc[currDimNum] = partitionEnd;
            cellNumArr[currDimNum] = i;
            if(i == numPartitions - 1) {
                maxPointAcc[currDimNum] = maxGridPoint[currDimNum];
            }
            getCellKeys(currDimNum+1, minPointAcc, maxPointAcc, cellNumArr);
            partitionStart = partitionEnd;
            partitionEnd = partitionStart + initialCellSize;
        }
    }

    public static class assignKeyToPointUG implements PairFunction<Tuple2<String, Integer>, String, Integer> { // UG means uniform gridding

		public Tuple2<String, Integer> call(Tuple2<String, Integer> pi) {
            int[] cellNumArr = new int[dimension];
            for(int i = 0; i < dimension; i++) {
                cellNumArr[i] = (int)Math.floor((dataSetList.get(pi._2).getAttr()[i] - minGridPoint[i]) / initialCellSize);
            }
            return new Tuple2<String,Integer>(globalPositioningIndex.get(convertCellNumArrToString(cellNumArr)).toString(), pi._2);
		}
	}

    public static String convertCellNumArrToString(int[] cellNumArr) {
        StringBuilder str = new StringBuilder("");
        for(int i = 0; i < dimension; i++) {
            str.append(cellNumArr[i]);
            str.append(",");
        }
        String ret = new String(str.toString());
        return ret;
    }

    // this function applies uniform gridding to points and assigns a cell number to each point
    public static void applyUniformGridding() {
        int currDimNum = 0;
        double[] minPointAcc = new double[dimension];
        double[] maxPointAcc = new double[dimension];
        int[] cellNumArr = new int[dimension];
        
    	getCellKeys(currDimNum, minPointAcc, maxPointAcc, cellNumArr);
    }

    public static void printHashMaps() {
        System.out.println("Global Positioning index:");
        for (Map.Entry<String, Integer> entry : globalPositioningIndex.entrySet()) {
            System.out.println(entry.getKey() + ":::::" + entry.getValue());
        }
    }

    public static JavaPairRDD<String, Integer> applyAdaptiveGridding(JavaSparkContext sc, List<Tuple2<String, Integer>> adaptiveRDDList, Map<String, Long> cellCount) {
        double[] minPointAcc = new double[dimension];
        double[] maxPointAcc = new double[dimension];
        String cellKey;
        Tuple2<Double[], Double[]> pair;
        Double[] minGridDim, maxGridDim;
        int[] cellNumArr = new int[dimension];
        List<String> keysToBeRemoved = new ArrayList<String>();
        
        int itr = 0;
        while(true) {
            boolean adaptiveGriddingDone = true;
            System.out.println("cellcount in itr:" + itr);
            for(Map.Entry<String, Long> entry: cellCount.entrySet()) {
                System.out.print(entry.getKey() + "    " + entry.getValue() + "  " + (entry.getValue() <= tau) + "  ");
                cellKey = entry.getKey();
                pair = keyToCell.get(cellKey);
                minGridDim = pair._1;
                maxGridDim = pair._2;
                for(int j = 0; j < dimension; j++) {
                    System.out.print(minGridDim[j] + ",");
                }
                System.out.print("  ");
                for(int j = 0; j < dimension; j++) {
                    System.out.println(maxGridDim[j] + ",");
                }
                System.out.println();
                if(entry.getValue() > tau) {
                    keysToBeRemoved.add(entry.getKey());
                    adaptiveGriddingDone = false;
                    
                    
                    getCellKeysAdaptive(0, minPointAcc, maxPointAcc, cellNumArr, minGridDim, maxGridDim, cellKey);
                }
            }
            if(adaptiveGriddingDone) {
                return JavaPairRDD.fromJavaRDD(sc.parallelize(adaptiveRDDList));
            }
            JavaPairRDD<String, Integer> adaptiveGridRDD = JavaPairRDD.fromJavaRDD(sc.parallelize(adaptiveRDDList)).mapToPair(new Gridding.adaptiveGridding(cellCount));
            adaptiveRDDList = adaptiveGridRDD.collect();
            // System.out.print("finished iteration:" + itr);
            cellCount = adaptiveGridRDD.countByKey();
            itr++;
            for(String key: keysToBeRemoved) {
                keyToCell.remove(key);
            }
            keysToBeRemoved.clear();
        }
    }
    
    public static void getCellKeysAdaptive(int currDimNum, double[] minPointAcc, double[] maxPointAcc, int[] cellNumArr, Double[] minGridDim, Double[] maxGridDim, String existingKey) {
        if(currDimNum == dimension) {
            String cellNumStr = convertCellNumArrToString(cellNumArr);
            Double[] minPointArr = new Double[dimension];
            Double[] maxPointArr = new Double[dimension];
            for(int i = 0; i < dimension; i++) {
                minPointArr[i] = minPointAcc[i];
                maxPointArr[i] = maxPointAcc[i];
            }
            Integer newKeyToAppend = globalPositioningIndex.get(cellNumStr);
            keyToCell.put(existingKey + "." + newKeyToAppend.toString(), new Tuple2<Double[],Double[]>(minPointArr, maxPointArr));
            return;
        }
        int numPartitions = 2;
        double length = (maxGridDim[currDimNum] - minGridDim[currDimNum]) / 2;
        double partitionStart = minGridDim[currDimNum];
        double partitionEnd = partitionStart + length;
        for(int i = 0; i < numPartitions; i++) {
            minPointAcc[currDimNum] = partitionStart;
            maxPointAcc[currDimNum] = partitionEnd;
            cellNumArr[currDimNum] = i;
            if(i == numPartitions - 1) {
                maxPointAcc[currDimNum] = maxGridDim[currDimNum];
            }
            getCellKeysAdaptive(currDimNum+1, minPointAcc, maxPointAcc, cellNumArr, minGridDim, maxGridDim, existingKey);
            partitionStart = partitionEnd;
            partitionEnd = partitionStart + length;
        }
    }

    public static class adaptiveGridding implements PairFunction<Tuple2<String, Integer>, String, Integer> { // UG means uniform gridding
        private Map<String, Long> cellCount;

        public adaptiveGridding(Map<String, Long> cellCount) {
            this.cellCount = cellCount;
        }

		public Tuple2<String, Integer> call(Tuple2<String, Integer> pi) {
            // System.out.println("size:" + cellCount.size());
            if(cellCount.get(pi._1) <= tau) {
                return new Tuple2<String, Integer>(pi._1, pi._2);
            }
            else {
            	// System.out.println("key accessed :" + pi._1);
                Tuple2<Double[], Double[]> pair = keyToCell.get(pi._1);
                // System.out.println("in adaptive" + pair);
                Double[] minGridDim = pair._1;
                Double[] maxGridDim = pair._2;
                double[] point = dataSetList.get(pi._2).getAttr();
                int[] cellNumArr = new int[dimension];
                for(int i = 0; i < dimension; i++) {
                    cellNumArr[i] = point[i] >= (maxGridDim[i] - minGridDim[i])/2 ? 1:0;
                }
                return new Tuple2<String,Integer>(pi._1 + "." + globalPositioningIndex.get(convertCellNumArrToString(cellNumArr)), pi._2);
            }
		}
	} 
}
