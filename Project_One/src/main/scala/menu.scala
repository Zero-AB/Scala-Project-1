import org.apache.spark.sql.SparkSession
import Table_Management._
import Scen_1_thru_5._
import Scen_6_Future_Problem.problem

import scala.Console.{BOLD, RESET, UNDERLINED}
import scala.annotation.tailrec
import scala.io.StdIn.readInt

object menu {
  def main_menu(spark: SparkSession): Unit = {
    val Name = "Project One"
    println("********************************")
    print("**********")
    print(s"$BOLD $UNDERLINED$Name$RESET")
    println(" *********")
    println("********************************")
    println()
    println(" 1: Problem 1 (Consumers)")
    println(" 2: Problem 2 (Favorite Beverages)")
    println(" 3: Problem 3 (Beverage Availability)")
    println(" 4: Problem 4 (Partitions and Views for P3)")
    println(" 5: Problem 5 (Notes and Row Removal)")
    println(" 6: Problem 6 (Future Query)")
    println(" 0: Exit")
    println()

    val userInput = getNumberInput

    if (userInput == 1) {
      println("Problem 1 Selected" + "\n")
      Scen_1_thru_5.Problem_Scen_1(spark)
    } else if (userInput == 2) {
      println("Problem 2 Selected" + "\n")
      Scen_1_thru_5.Problem_Scen_2(spark)
    } else if (userInput == 3) {
      println("Problem 3 Selected" + "\n")
      Scen_1_thru_5.Problem_Scen_3(spark)
    } else if (userInput == 4) {
      println("Problem 4 Selected" + "\n")
      Scen_1_thru_5.Problem_Scen_4(spark)
    } else if (userInput == 5) {
      println("Problem 5 Selected" + "\n")
      Scen_1_thru_5.Problem_Scen_5(spark)
    } else if (userInput == 6) {
      println("Problem 6 Selected" + "\n")
      Scen_6_Future_Problem.problem(spark)
    } else if (userInput == 0) {
      println("Exit selected" + "\n" + "\n")
      sys.exit(0)
    } else {
      println("I'm sorry, but that is not a valid entry!")
      main_menu(spark)
    }
  }


  @tailrec
  def getNumberInput: Int = {
    print("Please enter the number of the problem you would like to select (enter 0 to exit): ")
    try {
      var inputInt = readInt()
      while(inputInt < 0 || inputInt > 6) {
        print("Please enter a valid number: ")
        inputInt = readInt()
      }
      inputInt
    }catch {
      case _: NumberFormatException =>
        println("That is not a valid entry!")
        getNumberInput
    }
  }

}
