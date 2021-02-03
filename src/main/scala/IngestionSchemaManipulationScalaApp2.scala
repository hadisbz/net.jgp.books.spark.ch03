
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partition


object IngestionSchemaManipulationScalaApp2 {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder
                .appName("Restaurants in Wake County, NC")
                .master("local[*]")
                .getOrCreate()
                
    var df = spark.read
                  .format("csv")
                  .option("header", "true")
                  .load("data/Restaurants_in_Wake_County_NC.csv")
                  
    println("*** Right after ingestion")
    /**
    *df.show(5)
    *df.printSchema()
    *println("we have " + df.count() + " rows")
    */
    
    df = df.withColumn("county", lit("Wake"))
           .withColumnRenamed("HSISID", "datasetId")
           .withColumnRenamed("NAME", "name")
           .withColumnRenamed("ADDRESS1", "address1")
           .withColumnRenamed("ADDRESS2", "address2")
           .withColumnRenamed("CITY", "city")
           .withColumnRenamed("STATE", "state")
           .withColumnRenamed("POSTALCODE", "zip")
           .withColumnRenamed("PHONENUMBER", "tel")
           .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
           .withColumnRenamed("FACILITYTYPE", "type")
           .withColumnRenamed("X", "geoX")
           .withColumnRenamed("Y", "geoY")
           .drop("OBJECTID","PERMITID","GEOCODESTATUS")
           
     df = df.withColumn("id", concat(df.col("state"),lit("_"),df.col("county"),
         lit("_"),df.col("datasetId")))
         
     // Shows at most 5 rows from the dataframe
     println("*** Dataframe transformed")
          
     val drop_cols = List("address2","zip","tel","dateStart",
                  "geoX","geoY","address1","datasetId")
     var dfUsedForBook = df.drop(drop_cols:_*)
     
     dfUsedForBook.show(5,30)
     println("we have " + dfUsedForBook.count + " records")
     
     println("*** Looking at partitions")
     val partitionCount2 = dfUsedForBook.rdd.getNumPartitions
     println("num of current partition is " + partitionCount2)
     val partitions = dfUsedForBook.rdd.partitions
     val partitionCount = partitions.length
     println("Partition count before repartition: " + partitionCount)

     dfUsedForBook = dfUsedForBook.repartition(4)
     println("Partition count after repartition: " + dfUsedForBook.rdd.partitions.length)
    
    
     dfUsedForBook.printSchema() /*---1*/
     
     
     val schema2 = dfUsedForBook.schema  /*---2*/
     schema2.printTreeString()
     
     val schemaAsString2 = schema2.mkString
     println("*** schema as string: " + schemaAsString2)
     
     val schemaAsJson2 = schema2.prettyJson
     println("*** Schema as JSON: " + schemaAsJson2)
    
     
  }
}