package com.home.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameFromFileAndFilters_4 {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
	 
		Dataset<Row> studentsDf = sparkSession.read().option("header", "true").csv("src/main/resources/students.csv");
		
		/*long count = studentsDf.count();
		System.out.println("count->"+count);
	    
	    String suject = studentsDf.first().getAs("subject").toString();
	    System.out.println("subject->"+suject);
	    
	    Dataset<Row> filterByModernArtSubjectAndYear = studentsDf.filter("subject='Modern Art' and year>2000");
	    filterByModernArtSubjectAndYear.show();
	    
	    Dataset<Row> filterUsingLambda = studentsDf.filter(row -> row.getAs("subject").equals("Modern Art"));
	    filterUsingLambda.show();
	    
	    
	    Column subjectColumn = studentsDf.col("subject");
	    
	    Dataset<Row> filterUsingCol = studentsDf.filter(subjectColumn.equalTo("Modern Art"));
	    filterUsingCol.show();
	    
*/
	    Dataset<Row> selectedColumns = studentsDf.select("student_id","score").groupBy("student_id").count();
	    
	    selectedColumns.show();
	    
		sparkContext.close();
		sparkSession.close();
	}
	
	
	
}
