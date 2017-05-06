package sn.djili.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.log4j.{Level,Logger}

/**
 * @author Abdou Khadre DIOP
 * @version 1.0
 * @since 03/05/2017
 * read int streaming wind direction from 
 * @see <a href="rtd.hpwren.ucsd.edu">link</a>
 */
object WindDirection {
  
  def main(args:Array[String]){
    
    // to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc =new SparkContext("local[*]","Wind direction")
    val sSc=new StreamingContext(sc,Seconds(1))
    val lines =sSc.socketTextStream("rtd.hpwren.ucsd.edu", 12028)
    val window=lines.window(Seconds(10),Seconds(5))
    /**
     * 
     * 
     * 
     */
    sSc.start()
  }
  
  
}