package sn.djili.spark

import org.apache.spark.SparkContext
import org.apache.log4j.{Logger,Level}

/**
 * @author Abdou Khadre DIOP
 * @version 1.0
 * @since 20/04/2017
 * count occurence of words in a textFile
 */
object WordCount {
  def main (args: Array[String]){
    
    //to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //create a context 
    val sc=new SparkContext("local[*]","WordCount")
    
    //take the text
    val book=sc.textFile("your textfile path")
    
    //use regex to split text into words
    val words=book.flatMap(line=>line.split("\\W+"))
    
    //make words into lowerCase
    val min=words.map(word=>word.toLowerCase())
    
    //give the value 1 to each word
    val map=min.map(x=>(x,1))
    
    //count by key words   
    val count=map.reduceByKey((x,y)=>(x+y))
    
  }
}