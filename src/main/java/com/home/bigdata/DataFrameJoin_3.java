package com.home.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.home.bigdata.model.Department;
import com.home.bigdata.model.Employee;

import helper.ModelCreator;

public class DataFrameJoin_3 {
	
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<Employee> employeeRDD= sparkContext.parallelize(ModelCreator.getEmployees());	
		JavaRDD<Department> departmentRDD= sparkContext.parallelize(ModelCreator.getDepartments());
		Dataset<Row> empDf = sparkSession.createDataFrame(employeeRDD, Employee.class);	
		Dataset<Row> deptDf = sparkSession.createDataFrame(departmentRDD, Department.class);
		
		Dataset<Row> joinedDf = empDf.join(deptDf,empDf.col("deptId").equalTo(deptDf.col("deptId")));
		joinedDf.show();
	
		sparkContext.close();
		sparkSession.close();
	}
	

}
