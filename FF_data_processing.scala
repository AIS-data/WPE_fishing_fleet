import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

//Author - Michał Bis, Statistics Poland

object FF_data_processing extends App {

  val spark = SparkSession.builder
    //.master("yarn-client")
    .master("local[*]")
    .appName("Fishing Fleet")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

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

  //register
  val df_register_0 = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true")).csv("hdfs:///user/mbis/NL_register.csv")
  val df_register = df_register_0.withColumn("mmsi", col("mmsi").cast(IntegerType))

  def defineList_FF_mmsi(df: DataFrame): List[(Int)] = {
    val list_FF_mmsi: List[(Int)] = df_register.select("mmsi").distinct().map(_.getAs[Int]("mmsi")).collect.toList
    return list_FF_mmsi
  }

  def defineList_FF_mmsi_select_port(df: DataFrame): List[(Int)] = {
    val list_mmsi_filter = df_register
      .select("mmsi")
      .distinct
      .filter("port_code == 'YE'")
      .filter("mmsi is not null")
      .toDF()

    val list_FF_mmsi_select_port: List[(Int)] = list_mmsi_filter.select("mmsi").distinct().map(_.getAs[Int]("mmsi")).collect.toList
    return list_FF_mmsi_select_port
  }

  def defineLocation_FF(list_FF_mmsi: List[(Int)]): DataFrame = {
    val df_location_FF = df_location.filter(x => list_FF_mmsi.contains(x(0)))
    return df_location_FF
  }

  def defineMessages_FF(list_FF_mmsi: List[(Int)]): DataFrame = {
    val df_location_FF = df_messages.filter(x => list_FF_mmsi.contains(x(0)))
    return df_location_FF
  }

  def defineLocation_FF_mmsi_select_port(list_FF_mmsi: List[(Int)]): DataFrame = {
    val df_location_FF_mmsi_select_port = df_location.filter(x => list_FF_mmsi_select_port.contains(x(0)))
    return df_location_FF_mmsi_select_port
  }

  def defineInPort_FF(pointA: (Double, Double), pointB: (Double, Double)): DataFrame = {
    val port = df_location_FF.filter(df_location_FF("lon") >= pointA._1 && df_location_FF("lon") <= pointB._1 && df_location_FF("lat") >= pointA._2 && df_location_FF("lat") <= pointB._2)
    return port
  }

  def defineOutPort_FF(pointA: (Double, Double), pointB: (Double, Double)): DataFrame = {
    val port = df_location_FF.filter((df_location_FF("lon") < pointA._1 || df_location_FF("lon") > pointB._1) || (df_location_FF("lat") < pointA._2 || df_location_FF("lat") > pointB._2))
    return port
  }

  def timeInPort(df: DataFrame): DataFrame = {
    val windowSpec1 = Window.partitionBy("mmsi").orderBy("timestamp").rowsBetween(1, 1)
    val df2 = df.withColumn("time_diff_next", (unix_timestamp(lead(df("timestamp"), 1).over(windowSpec1)) - unix_timestamp(df("timestamp"))))
    val df3 = df2.filter("speed<4.0")
    val df4 = df3.groupBy("mmsi").sum("time_diff_next")
    val df5 = df4.withColumnRenamed("sum(time_diff_next)", "time_in_port_s")
    val df6 = df5.withColumn("time_in_port_h", round(df5("time_in_port_s") * 0.000277777778,2))
    val df7 = df6.withColumn("time_out_port_or_no_signal_h", round(abs(df6("time_in_port_h")-24),2))
    return df7
  }


  def timeFishingActivity(df: DataFrame): DataFrame = {
    val windowSpec1 = Window.partitionBy("mmsi").orderBy("timestamp").rowsBetween(1, 1)
    val df1 = df.withColumn("time_diff_next", (unix_timestamp(lead(df("timestamp"), 1).over(windowSpec1)) - unix_timestamp(df("timestamp"))))
    val df2 = df1.filter("speed>0.5 AND speed<20")
    val df3 = df2.groupBy("mmsi").sum("time_diff_next").toDF()
    val df4 = df3.withColumnRenamed("sum(time_diff_next)", "time_fishing_activity_s")
    val df5 = df4.withColumn("time_fishing_activity_h", round(df4("time_fishing_activity_s") * 0.000277777778,2))
    return df5
  }

  def preparationLocationForAnalysis(df: DataFrame): DataFrame = {
    val windowSpec1  = Window.partitionBy("mmsi").orderBy("timestamp").rowsBetween(1,1)
    val df1 = df.filter(x=>list_FF_mmsi_select_port.contains(x(0)))
    val df2 = df1.withColumn("time_diff_next",(unix_timestamp(lead(df("timestamp"),1).over(windowSpec1))-unix_timestamp(df("timestamp")))).toDF()
    val df3 = df2.withColumn("time",unix_timestamp(df("timestamp")))
    val df4 = df3.withColumn("time",df3("time").cast("Int"))
    return df4
  }

  def checkDraught(ship: Int, df: DataFrame): Int = {
    val filtered = df.filter(df.col("mmsi") === ship)

    val df1 = filtered.groupBy("mmsi")
      .agg(
        //max("draught").as("draught_max").cast(DoubleType)/10,
        //min("draught").as("draught_min").cast(DoubleType)/10,
        avg("draught").cast(DoubleType)/10
      )
      .as("draught_avg_meters").toDF()

    df1.show(false)

    return ship
  }

  //haversine method - source - https://davidkeen.com/blog/2013/10/calculating-distance-with-scalas-foldleft/
  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    val out = 6371.009 * greatCircleDistance // 6371.009 km , 3958.761 mil
    if((pointB._2 - pointA._2).abs<0.5 && (pointB._1 - pointA._1).abs<0.5 && ((pointA._1>0 && pointB._1>0) || ((pointA._1<0 && pointB._1<0))) && ((pointA._2>0 && pointB._2>0) || ((pointA._2<0 && pointB._2<0)))) out else 0.0
  }

  def trackDistance(points: List[(Double, Double)]): Double = {
    // calculates the disntance of the track as sum of the distances between the point
    points match {
      case head :: tail => tail.foldLeft(head, 0.0)((accum, elem) => (elem, accum._2 + haversineDistance(accum._1, elem)))._2
      case Nil => 0.0
    }
  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case _: Throwable => None
  }

  def parseInt(s: String) = try {
    Some(s.toInt)
  } catch {
    case _: Throwable => None
  }

  def processing(ship: Int, df: DataFrame): Double = {
    val filtered = df.filter(df.col("mmsi") === ship)
      .sort("time")
      .dropDuplicates(Seq("time","mmsi","lon","lat"))
      .withColumn("speed", expr("case when speed>20 then 0.0 else speed end").cast(DoubleType))

    val test = filtered.groupBy("mmsi")
      .agg(
        max("speed").as("speed_max"),
        min("speed").as("speed_min"),
        avg("speed").as("speed_avg")
      )
      .toDF()

    test.show(false)

    val test2 = filtered.groupBy("mmsi")
      .agg(
        max("lat").as("lat_max"),
        min("lat").as("lat_min"),
        max("lon").as("lon_max"),
        min("lon").as("lon_min")
      )
      .toDF()

    test2.show(false)

    //val max_speed: Double = test.select("speed_max")
    val firstRow_test = test.head()
    val speed_test = firstRow_test.getAs[Double]("speed_max")

    if (speed_test > 1)
    {
      println("Number of location data (records):" + filtered.count())

      val firstRow = filtered.head()
      val lastRow = filtered.reduce {
        (a, b) => if (a.getAs[Int]("time") > b.getAs[Int]("time")) a else b
      }

      val A_lat = firstRow.getAs[Double]("lat")
      val A_long = firstRow.getAs[Double]("lon")

      val B_lat = lastRow.getAs[Double]("lat")
      val B_long = lastRow.getAs[Double]("lon")

      val routePoints = filtered.select(filtered.col("lat"), filtered.col("lon")).rdd.map(r => (r.getDouble(0), r.getDouble(1))).collect.toList
      val routeDist: Double = trackDistance(routePoints)

      println("Calculated distance is" + routeDist  + " km")
    }
    else
    {
      println("Calculated distance is 0 km, the fishing vessel was in a different port")
    }

    return 0
  }

  //start generating statistics

  val list_FF_mmsi0 = defineList_FF_mmsi(df_register)
  val list_FF_mmsi = list_FF_mmsi0.filter(_ > 1)

  val list_FF_mmsi_select_port = defineList_FF_mmsi_select_port(df_register)

  val df_location_FF = defineLocation_FF(list_FF_mmsi)

  val df_location_FF_mmsi_select_port = defineLocation_FF_mmsi_select_port(list_FF_mmsi_select_port)

  val list_FF_mmsi_select_port_activ = df_location_FF_mmsi_select_port.select("mmsi").distinct().map(_.getInt(0)).collect.toList

  val df_messages_FF = defineMessages_FF(list_FF_mmsi)

  //parameters - port
  // YE  - val PointA = (4.05205, 51.49605),  val PointB = (4.05736, 51.50257)

  val PointA = (4.05205, 51.49605)
  val PointB = (4.05736, 51.50257)

  val df_port_in_FF = defineInPort_FF(PointA, PointB)
  val df_port_out_FF = defineOutPort_FF(PointA, PointB)

  //start statistics

  val df_time_in_port_FF = timeInPort(df_port_in_FF) //.show(1000,false)
  df_time_in_port_FF.repartition(5).write.option("header","true").mode("overwrite").csv("hdfs:///user/mbis/df_time_in_port_FF.csv")

  val df_location_FF_Analysis = preparationLocationForAnalysis(df_port_out_FF)

  val df_time_fishing = timeFishingActivity(df_location_FF_Analysis) //.show(10000,false)
  df_time_fishing.repartition(5).write.option("header","true").mode("overwrite").csv("hdfs:///user/mbis/df_time_fishing.csv")


  val mmsi: List[(Int)] = df_location_FF_Analysis.select("mmsi").distinct().map(_.getInt(0)).collect.toList

  mmsi.foreach {
    case (mmsi) => {
        println("------------------------------------------------------------------")
        println(mmsi)

        val A = checkDraught(mmsi, df_messages_FF)
        println(A)

        val B = processing(mmsi, df_location_FF_Analysis)
        println(B)

        println("------------------------------------------------------------------")
    }
  }
}
