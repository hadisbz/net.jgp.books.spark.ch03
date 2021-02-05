
import org.apache.spark.sql.functions.{col, concat, lit, split}
import org.apache.spark.sql.{Dataset, SparkSession, Row, SaveMode}
import java.util.Properties

object DataframeUnionScalaApp2 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
                            .appName("Union of two dataframes")
                            .master("local")
                            .getOrCreate()
    
    var df1 = spark.read
                   .format("csv")
                   .option("header", "true")
                   .load("data/Restaurants_in_Wake_County_NC.csv")
                   
    var df2 = spark.read
                   .format("json")
                   .load("data/Restaurants_in_Durham_County_NC.json")
                   
    val dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"

    val prop = new Properties
                   prop.setProperty("driver", "org.postgresql.Driver")
                   prop.setProperty("user", "postgres")
                   prop.setProperty("password", "1")
                   
    val wakeRestaurantsDf = buildWakeRestaurantsDataframe(df1)
    val durhamRestaurantsDf = buildDurhamRestaurantsDataframe(df2)
    
    var aaa = combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf)
       
  }
  
  private def buildWakeRestaurantsDataframe(df: Dataset[Row]) = {
    val drop_cols = List("OBJECTID", "GEOCODESTATUS", "PERMITID")
    var df1 = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumn("dateEnd", lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop(drop_cols:_*)
     df1 = df1.withColumn("id",
             concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
     df1
  }
  
  private def buildDurhamRestaurantsDataframe(df: Dataset[Row]) = {
    val drop_cols=List("fields", "geometry", "record_timestamp", "recordid")
    var df1 = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", col("fields.id"))
                .withColumn("name", col("fields.premise_name"))
                .withColumn("address1", col("fields.premise_address1"))
                .withColumn("address2", col("fields.premise_address2"))
                .withColumn("city", col("fields.premise_city"))
                .withColumn("state", col("fields.premise_state"))
                .withColumn("zip", col("fields.premise_zip"))
                .withColumn("tel", col("fields.premise_phone"))
                .withColumn("dateStart", col("fields.opening_date"))
                .withColumn("dateEnd", col("fields.closing_date"))
                .withColumn("type", split(col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", col("fields.geolocation").getItem(0))
                .withColumn("geoY", col("fields.geolocation").getItem(1))
                .drop(drop_cols:_*)

    df1 = df1.withColumn("id",
          concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
    // I left the following line if you want to play with repartitioning
    // df1 = df1.repartition(4);
    df1
  }
  
  private def combineDataframes(df1: Dataset[Row], df2: Dataset[Row]): Unit = {
    val df= df1.unionByName(df2)
    df.printSchema()
    println("We have " + df.count + " records.")
    val partitionCount = df.rdd.getNumPartitions
    println("Partition count: " + partitionCount)
      
  }
}