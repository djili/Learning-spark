package sn.djili.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Logger,Level}


/**
 * use the K-mean algorithme
 * @author Abdou Khadre DIOP
 * @since 10/05/2017
 * @version 1.0
 * for download the dataset
 * @see <a href="https://github.com/words-sdsc/coursera/blob/master/big-data-4/minute_weather.csv.gz">link</a>
 */
object Clustering {
  
  def main(args: Array[String]){
    
    //to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //create a sparkContext and a sqlContext
    val sc=new SparkContext("local[*]","Clustering with k-means")
    val sqlSc=new SQLContext(sc)
    
    //load the dataset
    val df=sqlSc.read
                 .option("inferSchema","true")
                 .option("header","true")
                 .format("com.databricks.spark.csv")
                 .load("minute_weather.csv")
    //println("total number of row : "+df.count())
    
    df.printSchema()
    
    //get 10% of the dataset
    val filteredDF1=df.filter(x=>x.getInt(0) < 158926)
    println("number of row (10% of data) : "+filteredDF1.count())
    
    //get row with rain accumulation equal 0
    val filteredDF2=filteredDF1.filter(x=>x.getDouble(10)!=null)
    println("number of row (rain accumulation at 0) : "+filteredDF2.count())
    
    //get row with rain duration equal 0
    val filteredDF3=filteredDF2.filter(x=>x.getDouble(11)!=null)
    println("number of row (rain duration at 0) : "+filteredDF2.count())
    
  }
  
}