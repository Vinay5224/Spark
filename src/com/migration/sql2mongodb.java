package com.migration;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

import java.util.Properties;



public class sql2mongodb {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL data sources example").master("local")
					.config("spark.mongodb.input.uri", "mongodb://localhost:27017/Migration.SqlMongo")
					.config("spark.mongodb.output.uri","mongodb://localhost:27017/Migration.SqlMongo")
			.getOrCreate();
		Properties prop = new Properties();
prop.put("user","sotero");
prop.put("password","Sotero!234");
		Dataset<Row> inputSql = spark.read().jdbc("jdbc:sqlserver://quaero-stg.cqzcaawwqsmw.us-east-1.rds.amazonaws.com;databaseName=exafluence_tst", "individual_profile_schema", prop);
		inputSql.createOrReplaceTempView("VINAY");
		
		Dataset<Row> insert = spark.sql("select * from VINAY limit 10");
		MongoSpark.write(insert).option("collection", "success").mode("append").save();
		System.out.println("insertion  completed");

	}

}
