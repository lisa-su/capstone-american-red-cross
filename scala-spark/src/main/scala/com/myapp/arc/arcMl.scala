package com.myapp.arc

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

class arcMl {
  var train_pct = .75
  var seed = 52.toLong
  var feature_set = Array[String]()
  var string_features = Array[String]()
  var id_column = "arc_id"
  
  def setTrainPct(p: Double) = {
    train_pct = p
  }
  
  def getTrainPct(): Double = {
    return train_pct
  }
  
  def setSeet(s: Long) = {
    seed = s
  }
  
  def setFeatureSet(l: Array[String]) = {
    feature_set = l
  }
  
  def getFeatureSet(): Array[String] = {
    return feature_set
  }
  
  def setStringFeatureList(l: Array[String]) = {
    string_features = l
  }
  
  def setStringFeatureList(): Array[String] = {
    return string_features
  }
  
  def setIdColumn(s: String) = {
    id_column = s
  }
  
  def getIdColumn(): String = {
    return id_column
  }
  
  def transorm(d: DataFrame): DataFrame = {
    var data = d
    if (string_features.length != 0) {
      var stringIndexer = new StringIndexer()
      for (f <- string_features){
        stringIndexer = stringIndexer.setInputCol(f).setOutputCol(f + "_index")
        data = stringIndexer.fit(data).transform(data)
        feature_set(feature_set.indexOf(f)) = f + "_index"
      }
    }
    
    val assembler = new VectorAssembler().setInputCols(feature_set).setOutputCol("features")
    
    data = assembler.transform(data)
    
    return data
  }
  
  def kmean(df: DataFrame, k: Int) = {
    // Trains a k-means model
    val kmeans = new KMeans().setK(k)
  
    val model = kmeans.fit(df)
    
    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
  }
  
  
}