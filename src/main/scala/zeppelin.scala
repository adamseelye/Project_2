import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

object zeppelin {
  // main zeppelin functions will go in here
  // as far as I can tell, we will be manipulating
  // data in spark on a local server and then
  // passing the query to zeppelin using
  // ZeppelinContext (z.show)

  case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

  def demo(): Unit = {
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

    val sc = spark.sparkContext

    import spark.implicits._


    // load bank data
    val bankText = sc.parallelize(
      IOUtils.toString(
        new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
        Charset.forName("utf8")).split("\n"))


    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt
      )
    ).toDF()

    bank.createOrReplaceTempView("bank")

    bank.show()

  }

}
