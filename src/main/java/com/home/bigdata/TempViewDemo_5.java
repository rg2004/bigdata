package com.home.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TempViewDemo_5 {
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
	 
		Dataset<Row> studentsDf = sparkSession.read().option("header", "true").csv("src/main/resources/students.csv");
		

		studentsDf.createTempView("myStudentsView");
	    
		
		Dataset<Row> subjectsDf = sparkSession.sql("select subject from myStudentsView");
		
		subjectsDf.show();
	    
		sparkContext.close();
		sparkSession.close();
	}

}
