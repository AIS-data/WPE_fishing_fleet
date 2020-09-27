import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, _}

//Author - Michał Bis, Statistics Poland

object FF_registry_statistics extends App {

  val spark = SparkSession.builder
    //.master("yarn-client")
    .master("local[*]")
    .appName("Fishing Fleet")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //register - pl
  val df_register_0 = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true")).csv("hdfs:///user/mbis/NL_register.csv")
  val df_register = df_register_0.withColumn("mmsi", col("mmsi").cast(IntegerType))
  //df_register.printSchema()


  //start statistics by register - FISHING FLEET BY PORTS OF REGISTRATION

  val raport1_csv = df_register.groupBy("port_code")
    .agg(
      count("*").as("Number of ships"),
      sum("ton_gt").cast(DataTypes.createDecimalType(32,2)).as("Gross tonnage (GT) in thousands"),
      sum("power_main").cast(DataTypes.createDecimalType(32,2)).as("Power in thousand kW")
    )
    .orderBy("port_code")
    .toDF()
    .repartition(5).write.option("header","true").mode("overwrite").csv("hdfs:///user/mbis/stat_register_fishing_fleet.csv")
}