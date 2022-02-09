import org.apache.spark.sql.SparkSession

object Scen_1_thru_5 {
  def Problem_Scen_1(spark: SparkSession): Unit = {
    /*
    Problem Scenario 1
    What is the total number of consumers for Branch1?
    What is the number of consumers for the Branch2?
    Type 1: Creating single physical table with sub queries.
    Type 2: Creating multiple physical tables
    "use any one type which you are comfortable"
     */

    //below was used for testing
    //spark.sql("SELECT * FROM BevBranchFull").show(600)
    //spark.sql("SELECT * FROM BevConscountFull").show(600)
    print("The total number of consumers for Branch 1 = ")
    val totalConsBA = spark.sql("Select sum(consumed) as Total_Consumers from BevFullCombined where branch = 'Branch1'").collect()(0).getLong(0)
    println(totalConsBA)

    //alternate syntax if physical table was not created to sub query
    //spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch1'").show()

    print("The total number of consumers for Branch 2 = ")
    val totalConsBB = spark.sql("Select sum(consumed) as Total_Consumers from BevFullCombined where branch = 'Branch2'").collect()(0).getLong(0)
    println(totalConsBB)
    println()

    //alternate syntax if physical table was not created to sub query
    //spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()
  }
  def Problem_Scen_2(spark: SparkSession): Unit = {
    /*
    Problem Scenario 2
    What is the most consumed beverage on Branch1
    What is the least consumed beverage on Branch2
    What is the Average consumed beverage of  Branch2
     */

    //nested selects, group by, order by, sum(*)
    println("The most consumed beverage on Branch 1 is:  ")
    spark.sql("Select first(bev) as Beverage,Max(total_consumed) as Most_Consumed from (Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch1' Group By a.Beverage order by total_consumed desc)").show()
    //below used for testing output of all bevs
    //spark.sql("Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch1' Group By a.Beverage order by total_consumed desc").show()

    println("The least consumed beverage on Branch 2 is:  ")
    spark.sql("Select first(bev) as Beverage,min(total_consumed) as Least_Consumed from (Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed asc)").show()
    //below used for testing output of all bevs
    //spark.sql("Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed desc").show()

    print("The average consumed beverage of Branch 2 is:  ")

    //average consumed beverage is assumed to be the average of the most consumed beverage of a branch on a daily basis.
    //Below used for testing
    //spark.sql("Select a.Beverage as bev, sum(consumed) as totalConsumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by totalConsumed desc").show()

    val re = spark.sql("Select a.Beverage as bev, sum(consumed) as totalConsumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by totalConsumed desc").collect()(0).getString(0)

    println(s"$re" + " at an average daily consumption rate of ")
    spark.sql(s"Select avg(consumed) as Daily_Average from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' and a.beverage = '$re'").show()
    println()
    //spark.sql("Select a.branch, b.beverage, b.consumed from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()
  }
  def Problem_Scen_3(spark: SparkSession): Unit = {
    /*
    Problem Scenario 3
    What are the beverages available on Branch10, Branch8, and Branch1?
    What are the common beverages available in Branch4,Branch7?
     */

    println("The beverages available on Branch 10 are: ")
    spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch10' order by b.beverage asc").show(60)

    println("The beverages available on Branch 8 are: ")
    spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch8' order by b.beverage asc").show(60)

    println("The beverages available on Branch 1 are: ")
    spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch1' order by b.beverage asc").show(60)

    //then join the 3 with distinct beverages

    println("The beverages available on all 3 branches are: ")
    spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch10' or a.branch = 'Branch8' or a.branch = 'Branch1'order by b.beverage asc").show(60)

    println("The common beverages available between Branch 4 and Branch 7 are: ")
    //spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch4' order by b.beverage asc").show(100)
    //spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch7' order by b.beverage asc").show(100)
    spark.sql("Select x.beverage from (Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch4' order by b.beverage asc) x " +
      "inner join (Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch7' order by b.beverage asc) y on " +
      "x.beverage= y.beverage order by x.beverage asc").show(100)
  }

}
