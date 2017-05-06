package sn.djili.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Logger,Level}
import org.apache.spark.ml.feature.{VectorAssembler,Binarizer}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics

/**
 * use the DecisionTree to determine if day will be wet or not
 * @author Abdou Khadre DIOP
 * @since 30/04/2017
 * @version 1.0
 * for download the dataset
 * @see <a href="https://github.com/words-sdsc/coursera/blob/master/big-data-4/daily_weather.csv">link</a>
 */
object DecisionTree {
  def main(args:Array[String]){
    
    // to see just error in logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // initialize a spark context
    val sc=new SparkContext("local[*]","DecisionTree")
    
    // initialize the spark sql context
    val sqlsc=new SQLContext(sc)
    
    //read daily_weather.csv  
    val doc=sqlsc.read
                 .option("inferSchema","true")
                 .option("header","true")
                 .format("com.databricks.spark.csv")
                 .load("daily_weather.csv")
    
    //specify my columns features             
    val featureColumns=Array("air_pressure_9am","air_temp_9am","avg_wind_direction_9am","avg_wind_speed_9am",
        "max_wind_direction_9am","max_wind_speed_9am","rain_accumulation_9am",
        "rain_duration_9am")  
    
    //supress the number columns    
    val newDoc=doc.drop("number")
    
    //drop all rows will a null value
    val cleanDoc=newDoc.na.drop()
    
    //print the new number of row and colums
    println("number of row : "+cleanDoc.count()+" | number of column : "+cleanDoc.columns.length)
    
    //tranform the relative humidity at 3pm into a categorial variable(0 if relative humidity is less than 25% and 1 if it more than 25%)
    val binarizer=new Binarizer()
                      .setInputCol("relative_humidity_3pm")
                      .setOutputCol("label")
                      .setThreshold(24.99999)
    val binarizedDoc=binarizer.transform(cleanDoc)
    
    //see some element of transformation of relative humidity
    binarizedDoc.select("relative_humidity_3pm", "label").show(5)
    
    //aggregate my features variables
    val assembler=new VectorAssembler()
                      .setInputCols(featureColumns)
                      .setOutputCol("features")
    val assembled=assembler.transform(binarizedDoc)
    
    // split my data 80% for training set and 20% for testing set
    val splitData= assembled.randomSplit(Array(0.8,0.2),13234)
    val trainingData=splitData(0)
    val testingData=splitData(1)
    println("training dataset : "+trainingData.count()+" | testing data : "+testingData.count()) 
    
    //initialize my decision tree with gini indexing max depth at 5 and minimum instance per node at 20
    val decisionTree= new DecisionTreeClassifier()
                          .setLabelCol("label")
                          .setFeaturesCol("features")
                          .setImpurity("gini")
                          .setMaxDepth(5)
                          .setMinInstancesPerNode(20)                    
    
    // train my decision tree with trainning data and make a model                      
    val pipeline=new Pipeline().setStages(Array(decisionTree))
    val model=pipeline.fit(trainingData)
    
    // test my model with testing dataset
    val prediction=model.transform(testingData)
    prediction.select("prediction", "label").show(20)
    
    //evaluate accurancy of my model and print error rate (from 0 to 1)
    val evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol("label")
                        .setPredictionCol("prediction")
                        .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(prediction)
    println("Accuracy rate = "+accuracy)
    println("Test Error = " + (1.0 - accuracy))
    
    //see confusion matrix
    val metrics =new MulticlassMetrics(prediction.rdd.map(x=>(x,1))
    
    
    // write result real results and results predicted (from testing dataset) into csv file
    /*prediction.select("prediction", "label")
              .coalesce(1)
              .write
              .format("com.databricks.spark.csv")
              .option("header", "true")
              .save("../resultDecisionTreeWeather")*/
  }
}