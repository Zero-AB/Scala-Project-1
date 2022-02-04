import org.apache.spark.sql.SparkSession

object hivetest5 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    val sampleSeq = Seq((1, "spark"), (2, "Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("Course id", "course name")
    df.show()
    df.write.format("csv").save("sampleSeq")

  }
}