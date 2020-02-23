package com.home.bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InMemoryData_4 {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

		List<Row> inMemory = new ArrayList<Row>();

		inMemory.add(RowFactory.create("WARN", "12 Dec 2018"));
		inMemory.add(RowFactory.create("ERROR", "13 July 2020"));
		inMemory.add(RowFactory.create("ERROR", "11 May 2018"));
		inMemory.add(RowFactory.create("INFO", "10 Aug 2018"));

		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()) };

		StructType schema = new StructType(fields);
		Dataset<Row> df = sparkSession.createDataFrame(inMemory, schema);
		df.createTempView("logs");
		
		Dataset<Row> groupedByDf = sparkSession.sql("select level,collect_list(datetime),count(1) from logs group by level");
		
		groupedByDf.show();
		
		sparkSession.close();
	}

}
