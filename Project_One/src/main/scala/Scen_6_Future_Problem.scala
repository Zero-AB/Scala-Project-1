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
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch1' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 2 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch2' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 3 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch3' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 4 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch4' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 5 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch5' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 6 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch6' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 7 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch7' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 8 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch8' group by beverage order by total_consumed desc) t limit 2").show()

    println()

    println("The most consumed drinks of Branch 9 over 3 months were: ")
    spark.sql("Select t.bev as Beverage, t.total_consumed as Most_Consumed from (Select Beverage as bev, sum(consumed) as total_consumed from p6_fullbevcombined where branch = 'Branch9' group by beverage order by total_consumed desc) t limit 2").show()





  }
}
