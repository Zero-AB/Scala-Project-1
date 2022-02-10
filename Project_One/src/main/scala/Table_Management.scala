import org.apache.spark.sql.SparkSession

object Table_Management {
  def create_Tables(spark: SparkSession): Unit = {
    spark.sql("create table IF NOT EXISTS BevBranchA(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BevBranchA")
    //spark.sql("SELECT * FROM BevBranchA").show(100)
    spark.sql("create table IF NOT EXISTS BevBranchB(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BevBranchB")
    //spark.sql("SELECT * FROM BevBranchB").show(200)
    spark.sql("create table IF NOT EXISTS BevBranchC(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BevBranchC")
    //spark.sql("SELECT * FROM BevBranchC").show(300)

    spark.sql("create table IF NOT EXISTS BevConscountA(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE BevConscountA")
    //spark.sql("SELECT * FROM BevConscountA").show(100)
    spark.sql("create table IF NOT EXISTS BevConscountB(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE BevConscountB")
    //spark.sql("SELECT * FROM BevConscountB").show(200)
    spark.sql("create table IF NOT EXISTS BevConscountC(Beverage String,Consumed Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE BevConscountC")
    //spark.sql("SELECT * FROM BevConscountC").show(300)

    spark.sql("create table IF NOT EXISTS BevBranchFull(Beverage String,Branch String)")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchA")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchB")
    spark.sql("Insert INTO TABLE BevBranchFull Select * from BevBranchC")
    //val a = spark.sql("Select * from BevBranchFull")
    //a.write.format("csv").save("FullBevBranch")
    //a.show(600)
    //spark.sql("SELECT * FROM BevBranchFull").show(600)

    spark.sql("create table IF NOT EXISTS BevConscountFull(Beverage String,Consumed Int)")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountA")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountB")
    spark.sql("Insert INTO TABLE BevConscountFull Select * from BevConscountC")
    //val b = spark.sql("Select * from BevConscountFull")
    //b.write.format("csv").save("FullBevConscount")
    //b.show(600)
    //spark.sql("SELECT * FROM BevConscountFull").show(600)

    //below table created for Scenario 1
    spark.sql("create table IF NOT EXISTS BevFullCombined(BeverageB String,Branch String, BeverageC String, Consumed Int)")
    println("Table Created")
    spark.sql("Insert INTO TABLE BevFullCombined Select * from BevBranchFull a join BevConscountFull b on a.beverage = b.beverage")
    println("Data Inserted")

    //below table created for Scenario 5
    spark.sql("create table IF NOT EXISTS P5_Table(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE P5_Table")

    //below tables created for Scenario 6
    spark.sql("create table IF NOT EXISTS p6_fullBevBranch(Beverage String,Branch String,Day Int,City String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/p6_FullBevBranch.txt' INTO TABLE p6_fullBevBranch")
    //spark.sql("Select * from p6_fullBevBranch").show(700)
    spark.sql("create table IF NOT EXISTS p6_fullBevCombined(Beverage String,Branch String,Day Int,City String,Consumed Int)")
    spark.sql("Insert INTO TABLE p6_fullBevCombined Select a.beverage, a.branch, a.day, a.city, b.consumed from p6_fullbevbranch a join BevConscountFull b on a.beverage = b.beverage")
  }

  def drop_Tables(spark: SparkSession): Unit = {
    spark.sql("Drop TABLE BevBranchA")
    spark.sql("Drop TABLE BevBranchB")
    spark.sql("Drop TABLE BevBranchC")
    spark.sql("Drop TABLE BevBranchFull")

    spark.sql("Drop TABLE BevConscountA")
    spark.sql("Drop TABLE BevConscountB")
    spark.sql("Drop TABLE BevConscountC")
    spark.sql("Drop TABLE BevConscountFull")

    //for dropping table created for Scenario 1
    spark.sql("Drop TABLE BevFullCombined")
    //for dropping partition table
    spark.sql("Drop TABLE branch_Partitions")
    //for dropping Scenario 5 table
    spark.sql("Drop TABLE P5_Table")
    //for dropping Scenario 6 tables
    spark.sql("Drop TABLE p6_fullBevBranch")
    spark.sql("Drop TABLE p6_fullBevCombined")
  }

}
