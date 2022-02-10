import org.apache.spark.sql.SparkSession
import menu.main_menu
import Table_Management._
import Scen_1_thru_5._
import Scen_6_Future_Problem.problem


object Project_One {

  def main(args: Array[String]): Unit = {


    // create a spark session for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ProjectOne")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")

    //create or drop tables as needed
    //create_Tables(spark)
    //drop_Tables(spark)

    //run the program
    while (0 < 1) {
      main_menu(spark)
    }

  }

}
