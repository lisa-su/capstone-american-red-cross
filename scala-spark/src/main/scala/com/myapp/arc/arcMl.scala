package com.myapp.arc

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import java.io._

class arcMl {
  private var _target = ""
  private var _train_pct = .75
  private var _seed = 52.toLong
  private var _feature_set = Array[String]()
  private var _string_features = Array[String]()
  private var _id_column = "arc_id"
  private var _cv_fold = 10 
  
  //------ Setters ------
  def setTrainPct_(p: Double): this.type = {
    this._train_pct = p
    this
  }

  def setSeet_(s: Long): this.type = {
    this._seed = s
    this
  }
  
  def setCvFold_(n: Int): this.type = {
    this._cv_fold = n
    this
  }
  
  def setFeatureSet_(l: Array[String]): this.type = {
    this._feature_set = l
    this
  }
  
  def setStringFeatureList_(l: Array[String]): this.type = {
    this._string_features = l
    this
  }
  
  def setIdColumn_(s: String): this.type = {
    this._id_column = s
    this
  }
  
  def setTarget_(s: String): this.type = {
    this._target = s
    this
  }
  
  //------ Getters ------
  def getTrainPct = _train_pct
  
  def getCvFold = _cv_fold
  
  def getFeatureSet = _feature_set
  
  def getStringFeatureList = _string_features
  
  def getIdColumn = _id_column
  
  def getTarget = _target
  
  //start_time is an index for folder names
  def transform(d: DataFrame): DataFrame = {
    
    var data = d
    
    if (_string_features.length != 0) {
      data = data.na.fill("__null__", _string_features)
      var stringIndexer = new StringIndexer()
      var stringIndex = Array[String]()
      
      for (f <- _string_features){
        stringIndexer = stringIndexer.setInputCol(f).setOutputCol(f + "_index")
        data = stringIndexer.fit(data).transform(data)
        _feature_set(_feature_set.indexOf(f)) = f + "_index"
      }
    }

    val assembler = new VectorAssembler().setInputCols(_feature_set).setOutputCol("features")
    
    data = assembler.transform(data)
    
    return data
  }
  
  // start_time is an index for folder names
  def kmean(df: DataFrame, k: Int, iterNum: Int): KMeansModel = {
    // Trains a k-means model
    val kmeans = new KMeans().setK(k).setMaxIter(iterNum).setSeed(_seed)
  
    val model = kmeans.fit(df)
    
//    // Evaluate clustering by computing Within Set Sum of Squared Errors.
//    val WSSSE = model.computeCost(df)
//    println(s"Within Set Sum of Squared Errors = $WSSSE")
//    
//    // Shows the result
//    println("Final Centers: ")
//    model.clusterCenters.foreach(println)
    
    return model
  }
  
  
}