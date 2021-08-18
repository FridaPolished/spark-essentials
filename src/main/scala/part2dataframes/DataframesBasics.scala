package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataframesBasics extends App {

  // create a Spark session
  val spark = SparkSession.builder()
    .appName("Dataframesbasics")
    .config("spark.master", "local")
    .getOrCreate()

    // reading a DF
    val df = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

  // showing a DF
  df.show()
  df.printSchema()

  // get rows
  df.take(10).foreach(println)

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSchema = df.schema
  //  println(carsDFSchema)

  // read a DF with schema
  val carsDFwithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val row = Row("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) /// schema auto-inferred

  // create DF with implicits
  import spark.implicits._

  val manualCarsDFWithImplicits = cars.toDF("name", "MPG", "cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOfOrigin")
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()


  /**
    * Exercise:
    * 1) create a manual dataframe describing smartphones
    * - brand
    * - model
    * - screen size
    * - camera megaapixels
    */

  val phones = Seq(
    ("iphone", "sampleModel1", 10, 1053),
    ("samsung", "sampleModel2", 6, 2053),
    ("tcl", "sampleModel3", 13, 1253),
    ("Huawei", "sampleModel4", 12, 1053),
    ("LG", "sampleModel5", 9, 1553)
  )

//  val df2 = spark.createDataFrame(phones)
  val df2 = phones.toDF("Brand", "Model", "Screen size", "camera megapixels")

  df2.show()
  df2.printSchema()

  /**
    * 2. Read another file from the data/folder
    *  - print schema
    *  -count the number of rows, call count()
    * */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()
  println(s"Number of movies rows in the DF ${moviesDF.count()}")

  val x = moviesDF.count()
  println(x)
}
