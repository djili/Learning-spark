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
    
    //mettre le niveau de log a erreur par defaut il est en INFO
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //creation du context
    val sc=new SparkContext("local[*]","WordCount")
    
    //chargement du text
    val book=sc.textFile("your textfile path")
    
    //rdd par mot utilisant une regex 
    val words=book.flatMap(line=>line.split("\\W+"))
    
    //normaliser tous less mots en minusccule
    val min=words.map(word=>word.toLowerCase())
    
    //donner la valeur 1 a chaque mot pour un futur comptage par mot
    val map=min.map(x=>(x,1))
    
    //comptage   
    val count=map.reduceByKey((x,y)=>(x+y))
    
    //inverser l'affichage en (valeur,cle)
    val inverserCount=count.map(x=>(x._2,x._1))
    
    //tri 
    val inverseCountSorted=inverserCount.sortByKey()
    
    //affichage
    for (result <- inverseCountSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}