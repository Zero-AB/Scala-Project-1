import org.apache.spark.sql.SparkSession

object Scen_6_Future_Problem {

  def problem(spark: SparkSession): Unit = {
    /*
    Problem Scenario 6
    Add future query
    The likelihood of a newly created drinks popularity based on trends of current beverages.
    So if mocha drinks and espresso drinks are very popular with a branch, creating a drink that mixes the
    two could be very well received and a good investment for the branch. New data was added for this query
    using excel and placed in a table. Then Apache Zeppelin was used for visuals.
     */

    println("The most consumed drinks of Branch 1 over 3 months were: ")
    spark.sql("Select beverage, consumed from p6_fullbevcombined where branch = 'Branch1' group by beverage, consumed order by consumed desc limit 10").show()

    println()

    println("The most consumed drinks of Branch 2 over 3 months were: ")
    spark.sql("Select beverage, consumed from p6_fullbevcombined where branch = 'Branch2' group by beverage, consumed order by consumed desc limit 10").show()

    println()

    println("The most consumed drinks of Branch 6 over 3 months were: ")
    spark.sql("Select beverage, consumed from p6_fullbevcombined where branch = 'Branch6' group by beverage, consumed order by consumed desc limit 10").show()





  }
}
