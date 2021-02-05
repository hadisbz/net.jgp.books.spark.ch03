
import org.apache.spark.sql.functions.{col, concat, lit, split}
import org.apache.spark.sql.SparkSession


object JsonIngestionSchemaManipulationScalaApp2 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
                            .appName("Restaurants in Durham County, NC")
                            .master("local[*]")
                            .getOrCreate
                            
    var df = spark.read
                  .format("json")
                  .load("data/Restaurants_in_Durham_County_NC.json")
    df.show(2,50)
    df.printSchema()
    println("We have "+df.count+" records")
    
    df = df.withColumn("county" , lit("Durham"))
           .withColumn("datasetId" , col("fields.id"))
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
           
    val cols_list = List(col("state"),lit("_"),col("county"),lit("_"), col("datasetID"))
    
    df = df.withColumn("id", concat(cols_list:_*))
    
    df.show(2,50)
    df.printSchema()        
           
  }
}