package com.bitspam;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Weiszfeld {
    private static List<Point> dataList;
    private static int numPoints;
    private static double epsilon = 0.01;
    private static int k;
    private static int dimension;
    private static int numIterations;

    public static void initializeWeiszfeld(List<Point> dataList, int numPoints, double epsilon, int k, int dimension, int numIterations) {
        Weiszfeld.dataList = dataList;
        Weiszfeld.numPoints = numPoints;
        Weiszfeld.epsilon = epsilon;
        Weiszfeld.k = k;
        Weiszfeld.dimension = dimension;
        Weiszfeld.numIterations = numIterations;
    }

    public static List<Point> refinement(JavaSparkContext sc, List<Point> kMedoids) {
        // System.out.println("num:" + kMedoids.size());
        List<Integer> indicesList = IntStream.rangeClosed(0, numPoints -1).boxed().collect(Collectors.toList());
        List<Point> newKMedoids = sc.parallelize(indicesList)
                                    .mapToPair(new Weiszfeld.AssignCluster(kMedoids))
                                    .mapToPair(new Weiszfeld.initList())
                                    .reduceByKey(new Weiszfeld.combineSameClusterPoints())
                                    .mapToPair(new Weiszfeld.WeiszfeldAlgorithm(kMedoids))
                                    .values().collect();
        
        // JavaPairRDD<Integer, List<Integer>> rdd = sc.parallelize(indicesList)
        //                             .mapToPair(new Weiszfeld.AssignCluster(kMedoids))
        //                             .mapToPair(new Weiszfeld.initList())
        //                             .reduceByKey(new Weiszfeld.combineSameClusterPoints());
        
        // System.out.print("clusters: ");
        // for(Integer i: rdd.keys().collect()) {
        //     System.out.print(i + " ");
        // }
        // System.out.println();
        // List<Point> newKMedoids = rdd.mapToPair(new Weiszfeld.WeiszfeldAlgorithm(kMedoids))
        //                             .values().collect();
        return newKMedoids;
    }

    public static class AssignCluster implements PairFunction<Integer, Integer, Integer> {
        List<Point> kMedoids;

        public AssignCluster(List<Point> kMedoids) {
            this.kMedoids = kMedoids;
        }
        
        public Tuple2< Integer, Integer> call(Integer index) {
            double minDist = Double.MAX_VALUE;
            int classLabel = -1;
            for(int j = 0; j < k; j++) {
                double distance = calculateDistance(kMedoids.get(j),dataList.get(index));
                if(distance < minDist) {
                    classLabel = j;
                    minDist = distance;
                }
            }
            return new Tuple2<Integer, Integer>(classLabel, index);
        }
        
        public double calculateDistance(Point p1, Point p2) { 
            double[] point1 = p1.getAttr();
            double[] point2 = p2.getAttr();
            double tempCost = 0;
            for(int ind = 0; ind < dimension; ind++) {
                tempCost += (point1[ind] - point2[ind]) * (point1[ind] - point2[ind]);
            }
            return (double)Math.sqrt(tempCost);
        }
    }

    public static class initList implements PairFunction<Tuple2<Integer, Integer>, Integer, List<Integer>> {

		public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, Integer> pi) {
			List<Integer> ls = new ArrayList<Integer>();
			ls.add(pi._2);
			return new Tuple2<Integer,List<Integer>>(pi._1, ls);
		}
	} 
	
	public static class combineSameClusterPoints implements Function2<List<Integer>, List<Integer>, List<Integer>> {

		public List<Integer> call(List<Integer> l1, List<Integer> l2) {
			List<Integer> ls = new ArrayList<Integer>(l1);
			ls.addAll(l2);
			return ls;
		}
    }
    
    public static class WeiszfeldAlgorithm implements PairFunction<Tuple2<Integer, List<Integer>>, Integer, Point> {

        List<Point> kMedoids;

        public WeiszfeldAlgorithm(List<Point> kMedoids) {
            this.kMedoids = kMedoids;
        }

        @Override
        public Tuple2<Integer, Point> call(Tuple2<Integer, List<Integer>> t) throws Exception {
            List<Integer> clusterPoints = t._2;
            TreeMap<Point, Double> map = new TreeMap<Point, Double>(new PointComp()); // change it
            for(Integer i: clusterPoints) {
                Point p = dataList.get(i);
                if(map.containsKey(p)) {
                    map.put(p, map.get(p) + p.getWeight());
                }
                else {
                    map.put(p, p.getWeight());
                }
            }

            List<Point> aPoints = new ArrayList<>(map.size());
            for (Map.Entry<Point, Double> entry : map.entrySet()) {
                Point wPoint = entry.getKey();
                wPoint.setWeight(entry.getValue());
                aPoints.add(wPoint);
            }
            int maxIterations = Integer.MAX_VALUE;
            if(numIterations != -1) {
                maxIterations = numIterations;
            }
            // System.out.println("t._1:" +t._1);
            Point x = kMedoids.get(t._1);
            Point lastX;
            double error;
            int itr = 0;
            
            do {
                lastX = x;
                if (map.containsKey(x)) {
                    Point rj = R(x, aPoints);
                    double wj = map.get(x);
                    if (rj.getNorm() > wj) {
                        x = operatorS(x, wj, rj, aPoints);
                    }
                } else {
                    x = operatorT(x, aPoints);
                }
                error = Point.subtraction(x, lastX).getNorm();
                itr++;
            } while (error > epsilon && itr < maxIterations);
            return new Tuple2<Integer, Point>(t._1, x);
        }

        private Point operatorT(Point x, List<Point> aPoints) {
            Point result = new Point(dimension);
    
            double weightsSum = 0;
            for (Point a: aPoints) {
                double w = a.getWeight();
                double curWeight = w/Point.subtraction(x,a).getNorm();
                Point cur = Point.multiply(a, curWeight);
    
                weightsSum += curWeight;
                result.add(cur);
            }
    
            return result.multiply(1d/weightsSum);
        }

        private Point operatorS(Point aj, double wj, Point rj, List<Point> aPoints) {
            double rjNorm = rj.getNorm();        
            Point dj = new Point(dimension);
            dj.add(rj);
            dj.multiply(-1.0/rjNorm);
            
            // calculating tj (stepsize) taken from Vardi and Zhang
            double lj = operatorL(aj, aPoints);
            double tj = (rjNorm - wj)/lj;
            
            dj.multiply(tj);
            dj.add(aj);
            
            return dj;
        }

        private Point R(Point aj, List<Point> aPoints) {
            Point result = new Point(dimension);
            
            for (Point ai: aPoints) {
                if (ai.compareTo(aj) != 0) {
                    double w = ai.getWeight();
                    Point dif = Point.subtraction(ai, aj);
                    double factor = w/dif.getNorm();
                    dif.multiply(factor);
    
                    result.add(dif);
                }
            }
            
            return result;
        }

        private double operatorL(Point aj, List<Point> aPoints) {
            double res = 0;
            for (Point ai: aPoints) {
                if (aj.compareTo(ai) != 0) {
                    Point dif = Point.subtraction(aj, ai);
                    res += ai.getWeight()/dif.getNorm();
                }
            }
            return res;
        }

        private double evaluateF(Point x, List<Point> aPoints) {
            double res = 0;
            for (Point ai: aPoints) {
                res += ai.getWeight() * Point.subtraction(ai, x).getNorm();
            }
            return res;
        }
        

    }
}

class PointComp implements Comparator<Point> {
    public int compare(Point p1, Point p2) {
        for (int i = 0; i < p1.getDimension(); i++) {
            if (p1.getAttr()[i] != p2.getAttr()[i]) {
                double dif = p1.getAttr()[i] - p2.getAttr()[i];
                if (dif > 0d) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }
        return 0;
    }
}