package com.home.bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class RddFundamentals_1 {
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		//Spark session is unified entry point for a spark application from 2.x onwards
		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();
		//JavaSparkContext is a java wrapper over sparkContext.sparkContext is specific for scala/python
         JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
		
         //reduce a double rdd 
		List<Double> list = new ArrayList<Double>();
		list.add(1.2);
		list.add(2.2);
		list.add(3.2);
		
		JavaRDD<Double> doubleRdd = sc.parallelize(list);
		Double sum = doubleRdd.reduce((a,b)->(a+b));
		System.out.println("Total by reduce is:"+sum);
		
		
		// Map operation and print results to console
		List<Integer> intList = new ArrayList<Integer>();
		intList.add(1);
		intList.add(2);
		intList.add(3);
		
		JavaRDD<Integer> intRdd = sc.parallelize(intList);
		
		JavaRDD<Double> sqrtRdd = intRdd.map(a->Math.sqrt(a));
		
		System.out.println("each element after sqrt");
		
		sqrtRdd.collect().forEach(a->System.out.println(a));
		
		Integer count = sqrtRdd.map(a-> 1).reduce((a,b)->(a+b));
		
		System.out.println("total elementrs are " + count);
		
		JavaRDD<String> rddX = sc.parallelize(
                Arrays.asList("spark rdd example", "sample example"),
                2);
        
        // map operation will return List of Array in following case
		
		System.out.println(" using map");
        JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
        String[] first = rddY.first();
        
        System.out.println("print first element"+Arrays.toString(first));
        System.out.println("size "+rddX.count());
        
        
        System.out.println(" using flat map");
        // flatMap operation will return list of String in following case
        JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        
        System.out.println("size "+rddY2.count());
        
        rddY2.collect().forEach(a->System.out.println(a));
		
		
		sc.close();
		
	}

}
