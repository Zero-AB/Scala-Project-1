
import org.apache.spark.sql.SparkSession

object hivetest6 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("hive test6")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
   // spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")

    //spark.sql("create table test1(Beverage String,Branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE test1")
    spark.sql("SELECT * FROM test1").show()
  }
}