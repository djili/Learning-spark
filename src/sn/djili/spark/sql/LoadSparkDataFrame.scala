

package sn.djili.spark.sql

import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author Abdou Khadre DIOP
 * @version 1.0
 * @since 20/04/2017
 * for download the dataset
 * @see <a href="https://github.com/words-sdsc/coursera/blob/master/big-data-4/daily_weather.csv">link</a>
 * load a simple csv file
 */
object LoadSparkDataFrame {
  def main(args:Array[String]){
    
    // to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // initialize a spark context
    val sc=new SparkContext("local[*]","LoadSparkDataFrame")
    
    // initialize the spark sql context
    val sqlsc=new SQLContext(sc)
    
    //read daily_weather.csv
    val df= sqlsc.read.option("header", "true")
                  .format("com.databricks.spark.csv")
                  .option("inferSchema", "true")
                  .load("daily_weather.csv")
                  
    //print csv schema
    df.printSchema()
    
    //print csv file
    df.show()
    
    //print number of line
    println(df.count())
    
    //erase row with a null value 
    df.na.drop()
    
    //print a column with caracteristics
    df.describe("air_temp_9am").show()
   
    //correlation between two variable
    val corr =df.stat.corr("relative_humidity_9am", "relative_humidity_3pm")
    println("correlation between relative_humidity_9am and relative_humidity_3pm : "+corr)
  }
                  
}