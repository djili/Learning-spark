package sn.djili.spark.sql

import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SQLContext

/**
 * @author Abdou Khadre DIOP
 * @version 1.0
 * @since 03/05/2017
 * connect spark to a postgresql database
 */
object SparkSql {
  def main(args:Array[String]){
    
    // to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // initialize spark context and sql context
    val sc=new SparkContext("local[*]","SparkSql")
    val scSql=new SQLContext(sc)
    
    // create the spark dataframe load gameclicks in dbtable database 
    val df= scSql.read
                 .format("jdbc")
                 .option("url", "jdbc:postgresql://localhost/cloudera?user=cloudera")
                 .option("dbtable", "gameclicks")
                 .load()
                 
    // print the table schema, count number of lines and show 5 rows of table
    df.printSchema()
    df.count()
    df.show(5)
    
    //select userid, teamlevel and print
    df.select("userid","teamlevel").show(5)
    
       
  }
}