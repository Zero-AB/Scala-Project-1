import org.apache.spark.SparkContext

import org.apache.spark.sql.{SparkSession, functions}


object Project1  {


  def main(args: Array[String]): Unit = {

    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Project1")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("created spark session")

  /*
    spark.sql("create table IF NOT EXISTS BevBranchA(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BevBranchA")
    //spark.sql("SELECT * FROM BevBranchA").show(100)

    spark.sql("create table IF NOT EXISTS BevBranchB(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BevBranchB")
    //spark.sql("SELECT * FROM BevBranchB").show(200)

    spark.sql("create table IF NOT EXISTS BevBranchC(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BevBranchC")
    //spark.sql("SELECT * FROM BevBranchC").show(300)

    spark.sql("create table IF NOT EXISTS BevBranchFull(Beverage String,Branch String)")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchA")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchB")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchC")
    spark.sql("SELECT * FROM BevBranchFull").show(600)



    spark.sql("create table IF NOT EXISTS BevConscountA(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE BevConscountA")
    //spark.sql("SELECT * FROM BevConscountA").show(100)

    spark.sql("create table IF NOT EXISTS BevConscountB(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE BevConscountB")
    //spark.sql("SELECT * FROM BevConscountB").show(200)

    spark.sql("create table IF NOT EXISTS BevConscountC(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE BevConscountC")
    //spark.sql("SELECT * FROM BevConscountC").show(300)

    spark.sql("create table IF NOT EXISTS BevConscountFull(Beverage String,Consumed Int)")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountA")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountB")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountC")
    spark.sql("SELECT * FROM BevConscountFull").show(600)
  */

    /*
    spark.sql("Drop TABLE BevBranchA")
    spark.sql("Drop TABLE BevBranchB")
    spark.sql("Drop TABLE BevBranchC")
    spark.sql("Drop TABLE BevBranchFull")



    spark.sql("Drop TABLE BevConscountA")
    spark.sql("Drop TABLE BevConscountB")
    spark.sql("Drop TABLE BevConscountC")
    spark.sql("Drop TABLE BevConscountFull")


     */

    /*
    Problem Scenario 1
    What is the total number of consumers for Branch1?
    What is the number of consumers for the Branch2?
    Type 1: Creating single physical table with sub queries.
    Type 2: Creating multiple physical tables
    "use any one type which you are comfortable"
     */

    //spark.sql("SELECT * FROM BevBranchFull").show(600)
   // spark.sql("SELECT * FROM BevConscountFull").show(600)

   // println("The total number of consumers for Branch 1 = ")
   // spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch1'").show()

   // println("The total number of consumers for Branch 2 = ")
   // spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()


    /*
    Problem Scenario 2
    What is the most consumed beverage on Branch1
    What is the least consumed beverage on Branch2
    What is the Average consumed beverage of  Branch2
     */

    //nested selects, group by, order by, sum(*)
    println("The most consumed beverage on Branch 1 is:  ")
   // spark.sql("Select first(bev) as Beverage,Max(total_consumed) as Most_Consumed from (Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch1' Group By a.Beverage order by total_consumed desc)").show()

    //spark.sql("Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch1' Group By a.Beverage order by total_consumed desc").show()


    println("The least consumed beverage on Branch 2 is:  ")

    //spark.sql("Select first(bev) as Beverage,min(total_consumed) as Most_Consumed from (Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed asc)").show()

    //spark.sql("Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed desc").show()

    println("The average consumed beverage of Branch 2 is:  ")

   // spark.sql("Select a.Beverage as bev, count(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed desc").show(60)

    //spark.sql("Select a.branch, b.beverage, b.consumed from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()


    /*
    Problem Scenario 3
    What are the beverages available on Branch10, Branch8, and Branch1?

    what are the common beverages available in Branch4,Branch7?
     */

    println("The beverages available on Branch 10 are: ")
   // spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch10' order by b.beverage asc").show(30)

    println("The beverages available on Branch 8 are: ")
    //spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch8' order by b.beverage asc").show(30)

    println("The beverages available on Branch 1 are: ")
   // spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch1' order by b.beverage asc").show(30)

    println("The common beverages available between Branch 4 and Branch 7 are: ")
    //spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch4' order by b.beverage asc").show(100)
    //spark.sql("Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch7' order by b.beverage asc").show(100)
    spark.sql("Select x.beverage from (Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch4' order by b.beverage asc) x " +
      "inner join (Select Distinct b.beverage from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch7' order by b.beverage asc) y on " +
      "x.beverage= y.beverage order by x.beverage asc").show(100)

    /*
    Problem Scenario 4
    create a partition,
    View for the scenario3.
     */

    //probably want to partition by beverage name and/or branch

    /*
    Problem Scenario 5
    Alter the table properties to add "note","comment"
    Remove a row from the any Scenario.
     */

    /*
    Problem Scenario 6
    Add future query
    the likelihood of a newly created drinks popularity based on trends of current beverages.
    So if mocha drinks and espresso drinks are very popular with a branch, creating a drink that mixes the
    two could be very well received and a good investment for the branch.
     */

    
    println()

  }

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

    println("The total number of consumers for Branch 1 = ")
    spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch1'").show()

    println("The total number of consumers for Branch 2 = ")
    spark.sql("Select sum(b.consumed) from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()

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
    spark.sql("Select first(bev) as Beverage,min(total_consumed) as Most_Consumed from (Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed asc)").show()
    //below used for testing output of all bevs
    //spark.sql("Select a.Beverage as bev, sum(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed desc").show()

    println("The average consumed beverage of Branch 2 is:  ")

    // spark.sql("Select a.Beverage as bev, count(consumed) as total_consumed from BevConscountFull a join BevBranchFull b on a.beverage = b.beverage where b.branch = 'Branch2' Group By a.Beverage order by total_consumed desc").show(60)

    //spark.sql("Select a.branch, b.beverage, b.consumed from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage where a.branch = 'Branch2'").show()
  }

  def Problem_Scen_3(spark: SparkSession): Unit = {



  }

  def Problem_Scen_4(spark: SparkSession): Unit = {



  }

  /*
  def Problem_Scen_5(spark: SparkSession): Unit = {



  }

  def Problem_Scen_6(spark: SparkSession): Unit = {



  }
  */


}