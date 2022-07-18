import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

object zep_templates {
  case class Person(id:Int, name:String, age:Int, country:String)

  val spark = SparkSession
    .builder
    .appName("Spark Queries")
    .master("spark://<hostaddr>:7077") // change hostaddr
    .config("spark.master", "local[*]")
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("created spark session")

  def sqlQuery(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
    // call createOrReplaceTempView first if you want to query this DataFrame via sql
    df1.createOrReplaceTempView("people")
    // SparkSession.sql return DataFrame
    val df2 = spark.sql("select name, age from people")
    df2.show()

    // You need to register udf if you want to use it in sql
    spark.udf.register("udf1", (e: String) => e.toUpperCase)
    val df3 = spark.sql("select udf1(name), age from people")
    df3.show()

  }

  def createDfSpark(): Unit = {

    // create DataFrame from scala Seq. It can infer schema for you.
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
    df1.printSchema
    df1.show()

    // create DataFrame from scala case class
    val df2 = spark.createDataFrame(Seq(Person(1, "andy", 20, "USA"), Person(2, "jeff", 23, "China"), Person(3, "james", 18, "USA")))
    df2.printSchema
    df2.show()

    import spark.implicits._
    // you can also create Dataset from scala case class
    val df3 = spark.createDataset(Seq(Person(1, "andy", 20, "USA"), Person(2, "jeff", 23, "China"), Person(3, "james", 18, "USA")))
    df3.printSchema
    df3.show()

  }

  def createDfDFReader(): Unit = {
    val SPARK_HOME = System.getenv("SPARK_HOME")
    // Read data from json file
    // link for this people.json (https://github.com/apache/spark/blob/master/examples/src/main/resources/people.json)
    // Use hdfs path if you are using hdfs
    val df1 = spark.read.json(s"file://$SPARK_HOME/examples/src/main/resources/people.json")
    df1.printSchema
    df1.show()

    // Read data from csv file. You can customize it via spark.read.options. E.g. In the following example, we customize the sep and header
    // Add . at the end of this line to indidate this is not the end of this line of code.
    val df2 = spark.read.options(Map("sep"->";", "header"-> "true")).
      csv(s"file://$SPARK_HOME/examples/src/main/resources/people.csv")
    df2.printSchema
    df2.show()

    // Specify schema for your csv file
    import org.apache.spark.sql.types._

    val schema = new StructType().
      add("name", StringType, nullable = true).
      add("age", IntegerType, nullable = true).
      add("job", StringType, nullable = true)
    val df3 = spark.read.options(Map("sep"->";", "header"-> "true")).
      schema(schema).
      csv(s"file://$SPARK_HOME/examples/src/main/resources/people.csv")
    df3.printSchema
    df3.show()

  }

  def addColumn(): Unit = {

    // withColumn could be used to add new Column
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
    val df2 = df1.withColumn("age2", col("age") + 1)
    df2.show()

    // the new column could replace the existing the column if the new column name is the same as the old column
    val df3 = df1.withColumn("age", col("age") + 1)
    df3.show()

    // Besides using expression to create new column, you could also use udf to create new column
    val df4 = df1.withColumn("name", upper(col("name")))
    df4.show()

  }

  def remColumn(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
    // drop could be used to remove Column
    val df2 = df1.drop("id")
    df2.show()

  }

  def selSubset(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
    // select can accept a list of string of the column names
    val df2 = df1.select("id", "name")
    df2.show()

    // select can also accept a list of Column. You can create column via $ or udf
    val df3 = df1.select(col("id"), upper(col("name")), col("age") + 1)
    df3.show()

  }

  def rowFilter(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")

    // filter accept a Column
    val df2 = df1.filter(col("age") >= 20)
    df2.show()

    // To be noticed, you need to use "===" for equal instead of "=="
    val df3 = df1.filter(col("age") >= 20 && col("country") === "China")
    df3.show()

  }

  def createUDF(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")

    // [String, String] The first String is return type of this UDF, and the second String is the UDF argument type
    val udf1 = udf[String, String]((e:String) => e.toUpperCase)
    val df2 = df1.select(udf1(col("name")))
    df2.show()

    // UDF could also be used in filter, in this case the return type must be Boolean
    val udf2 = udf[Boolean, Int]((e:Int) => e >= 20)
    val df3 = df1.filter(udf2(col("age")))
    df3.show()

    // UDF could also accept more than 1 argument.
    val udf3 = udf[String, String, String]((e1:String, e2:String) => e1 + "_" + e2)
    val df4 = df1.select(udf3(col("name"), col("country")).as("name_country"))
    df4.show()

  }

  def groupBy(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")

    // You can call agg function after groupBy directly, such as count/min/max/avg/sum
    val df2 = df1.groupBy("country").count()
    df2.show()

    // Pass a Map if you want to do multiple aggregation
    val df3 = df1.groupBy("country").agg(Map("age"->"avg", "id" -> "count"))
    df3.show()

    // Or you can pass a list of agg function
    val df4 = df1.groupBy("country").agg(avg("age").as("avg_age"), count("id").as("count"))
    df4.show()

    // You can not pass Map if you want to do multiple aggregation on the same column as the key of Map should be unique. So in this case
    // you have to pass a list of agg functions
    val df5 = df1.groupBy("country").agg(avg("age").as("avg_age"), max("age").as("max_age"))
    df5.show()

  }

  def joinSingleField(): Unit = {
    val df1 = spark.createDataFrame(Seq((1, "andy", 20, 1), (2, "jeff", 23, 2), (3, "james", 18, 3))).toDF("id", "name", "age", "c_id")
    df1.show()

    val df2 = spark.createDataFrame(Seq((1, "USA"), (2, "China"))).toDF("c_id", "c_name")
    df2.show()

    // You can just specify the key name if join on the same key
    val df3 = df1.join(df2, "c_id")
    df3.show()

    // Or you can specify the join condition explicitly in case the key is different between tables
    val df4 = df1.join(df2, df1("c_id") === df2("c_id"))
    df4.show()

    // You can specify the join type after the join condition, by default it is inner join
    val df5 = df1.join(df2, df1("c_id") === df2("c_id"), "left_outer")
    df5.show()

  }

  def joinMultiField(): Unit = {
    val df1 = spark.createDataFrame(Seq(("andy", 20, 1, 1), ("jeff", 23, 1, 2), ("james", 12, 2, 2))).toDF("name", "age", "key_1", "key_2")
    df1.show()

    val df2 = spark.createDataFrame(Seq((1, 1, "USA"), (2, 2, "China"))).toDF("key_1", "key_2", "country")
    df2.show()

    // Join on 2 fields: key_1, key_2

    // You can pass a list of field name if the join field names are the same in both tables
    val df3 = df1.join(df2, Seq("key_1", "key_2"))
    df3.show()

    // Or you can specify the join condition explicitly in case when the join fields name is differetnt in the two tables
    val df4 = df1.join(df2, df1("key_1") === df2("key_1") && df1("key_2") === df2("key_2"))
    df4.show()

  }

}
