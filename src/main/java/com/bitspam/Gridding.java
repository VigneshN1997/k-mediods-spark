package com.bitspam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;


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

    public static void initializeGridding(int dimension, double[] minGridPoint, double[] maxGridPoint, List<Point> dataSetList) {
        Gridding.dimension = dimension;
        Gridding.minGridPoint = minGridPoint;
        Gridding.maxGridPoint = maxGridPoint;
        Gridding.dataSetList = dataSetList;
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

    public static class assignKeyToPointUG implements Function<Tuple2<String, Integer>, Tuple2<String, Integer>> { // UG means uniform gridding

		public Tuple2<String, Integer> call(Tuple2<String, Integer> pi) {
            int[] cellNumArr = new int[dimension];
            for(int i = 0; i < dimension; i++) {
                cellNumArr[i] = (int)Math.floor((dataSetList.get(pi._2).getAttr()[i] - minGridPoint[i]) / initialCellSize);
            }
            // globalPositioningIndex.get(convertCellNumArrToString(cellNumArr))
			return new Tuple2<String,Integer>(pi._1 + globalPositioningIndex.get(convertCellNumArrToString(cellNumArr)), pi._2);
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
}
