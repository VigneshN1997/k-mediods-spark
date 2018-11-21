package com.bitspam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Test {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("k-mediods-BITS");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Tuple2<String, Integer>> ls = new ArrayList<>();
		ls.add(new Tuple2<String, Integer>("a.b.c", 0));
		ls.add(new Tuple2<String, Integer>("b.d.e", 2));
		ls.add(new Tuple2<String, Integer>("b.d.e", 5));
		ls.add(new Tuple2<String, Integer>("b.d.e", 6));
		ls.add(new Tuple2<String, Integer>("b.d", 7));
		ls.add(new Tuple2<String, Integer>("a.b.c", 1));
		ls.add(new Tuple2<String, Integer>("a.b.c", 3));
		ls.add(new Tuple2<String, Integer>("a.b.c", 4));
		JavaPairRDD<String, Integer> rdd = JavaPairRDD.fromJavaRDD(sc.parallelize(ls));
		JavaPairRDD<String, List<Integer>> rdd1 = rdd.mapToPair(new Test.abc());
		JavaPairRDD<String, List<Integer>> rdd2 = rdd1.reduceByKey(new Test.def());
		List<Integer>  x = rdd2.values().reduce(new Test.ghi());
		// List<Tuple2<String, List<Integer>>> lis = rdd2.collect();
		// for(Tuple2<String, List<Integer>> elem : lis) {
		// 	System.out.print(elem._1 + "  ");
		// 	for(Integer i: elem._2) {
		// 		System.out.print(i + ",");
		// 	}
		// 	System.out.println();
		// }
		// List<Tuple2<String, List<Integer>>> lis2 = rdd2.collect();
		// for(Tuple2<String, List<Integer>> elem : lis2) {
		// 	System.out.print(elem._1 + "  ");
		// 	for(Integer i: elem._2) {
		// 		System.out.print(i + ",");
		// 	}
		// 	System.out.println();
		// }
		for(Integer i : x) {
			System.out.println(i);
		}
	}

	public static class abc implements PairFunction<Tuple2<String, Integer>, String, List<Integer>> { // UG means uniform gridding

		public Tuple2<String, List<Integer>> call(Tuple2<String, Integer> pi) {
			List<Integer> ls = new ArrayList<Integer>();
			ls.add(pi._2);
			return new Tuple2<String,List<Integer>>(pi._1, ls);
		}
	} 
	
	public static class def implements Function2<List<Integer>, List<Integer>, List<Integer>> { // UG means uniform gridding

		public List<Integer> call(List<Integer> l1, List<Integer> l2) {
			List<Integer> ls = new ArrayList<Integer>(l1);
			ls.addAll(l2);
			return ls;
		}
	} 

	public static class ghi implements Function2<List<Integer>, List<Integer>, List<Integer>> { // UG means uniform gridding

		public List<Integer> call(List<Integer> l1, List<Integer> l2) {
			List<Integer> ls = new ArrayList<Integer>(l1);
			ls.addAll(l2);
			return ls;
		}
	} 
}
