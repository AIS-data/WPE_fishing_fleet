import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{unix_timestamp, _}
import org.apache.spark.sql.types.{IntegerType, StructType, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

//Author - Michał Bis, Statistics Poland

object FF_analysis_fishing_activity extends App {

  val spark = SparkSession.builder
    //.master("yarn-client")
    .master("local[*]")
    .appName("Fishing Fleet")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // ** datasource source ** EC-Data-Platform

  val structureSchema = new StructType()
    .add("mmsi",IntegerType,true)
    .add("lon",DoubleType,true)
    .add("lat",DoubleType,true)
    .add("accuracy",IntegerType,true)
    .add("speed",DoubleType,true)
    .add("course",DoubleType,true)
    .add("rotation",DoubleType,true)
    .add("status",IntegerType,true)
    .add("timestamp",TimestampType,true)

  // mmsi,lon,lat,accuracy,speed,course,rotation,status,timestamp
  val df_location = spark.read.schema(structureSchema).options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("hdfs:///user/hadoop/AIS/Locations/20160101*.csv.gz")
  df_location.filter(x=> x(0)!="mmsi")

  //mmsi,imo,name,callsign,destination,draught,dim_a,dim_b,dim_c,dim_c,ship_type,timestamp
  val df_messages_0 = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("hdfs:///user/hadoop/AIS/Messages/20160101*.csv.gz")
  val df_messages = df_messages_0.withColumn("draught", expr("case when draught is null then 0.0 else draught end").cast(DoubleType))


  def defineLocation_FF(list_FF_mmsi: List[(Int)]): DataFrame = {
    val df_location_FF = df_location.filter(x => list_FF_mmsi.contains(x(0)))
    return df_location_FF
  }

  def defineMessages_FF(list_FF_mmsi: List[(Int)]): DataFrame = {
    val df_location_FF = df_messages.filter(x => list_FF_mmsi.contains(x(0)))
    return df_location_FF
  }


  def defineInPort_FF(pointA: (Double, Double), pointB: (Double, Double)): DataFrame = {
    val port = df_location_FF.filter(df_location_FF("lon") >= pointA._1 && df_location_FF("lon") <= pointB._1 && df_location_FF("lat") >= pointA._2 && df_location_FF("lat") <= pointB._2)
    return port
  }

  def defineOutPort_FF(pointA: (Double, Double), pointB: (Double, Double)): DataFrame = {
    val port = df_location_FF.filter((df_location_FF("lon") < pointA._1 || df_location_FF("lon") > pointB._1) || (df_location_FF("lat") < pointA._2 || df_location_FF("lat") > pointB._2))
    return port
  }

  def preparationLocationForAnalysis(df: DataFrame): DataFrame = {
    val windowSpec1  = Window.partitionBy("mmsi").orderBy("timestamp").rowsBetween(1,1)
    val df1 = df.filter(x=>list_FF_mmsi.contains(x(0)))
    val df2 = df1.withColumn("time_diff_next",(unix_timestamp(lead(df("timestamp"),1).over(windowSpec1))-unix_timestamp(df("timestamp")))).toDF()
    val df3 = df2.withColumn("time",unix_timestamp(df("timestamp")))
    val df4 = df3.withColumn("time",df3("time").cast("Int"))
    return df4
  }


  //start analysis - fishing activity (trip)

  //define fishing vessel
  val list_FF_mmsi = List(244256000)

  val df_location_FF = defineLocation_FF(list_FF_mmsi)
  val df_messages_FF = defineMessages_FF(list_FF_mmsi)


  //define parameters - port
  val PointA = (4.05205, 51.49605)
  val PointB = (4.05736, 51.50257)

  val df_port_in_FF = defineInPort_FF(PointA, PointB)
  val df_port_out_FF = defineOutPort_FF(PointA, PointB)

  val df_location_FF_Analysis = preparationLocationForAnalysis(df_port_out_FF)


  val df1 = df_location_FF_Analysis
    .filter(df_location_FF_Analysis("mmsi") === "244256000")
    .sort("time")
    .repartition(5).write.option("header","true").mode("overwrite").csv("hdfs:///user/mbis/analysis_fishing_vessel.csv")
}