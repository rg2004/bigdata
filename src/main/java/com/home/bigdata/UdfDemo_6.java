package com.home.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UdfDemo_6 {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
	 
		Dataset<Row> studentsDf = sparkSession.read().option("header", "true").csv("src/main/resources/students.csv");
		
		sparkSession.udf().register("hasPassed", (String grade)-> grade.equals("A+"),DataTypes.BooleanType);

	  // use select to add a column and  lit function to add a value to it
	 //   studentsDf = studentsDf.withColumn("pass", lit("Yes"));
	    
	    studentsDf = studentsDf.withColumn("pass", callUDF("hasPassed", col("grade")));
	    
	    studentsDf.show();
	    
		sparkContext.close();
		sparkSession.close();
	}

}
