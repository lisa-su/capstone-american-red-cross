package com.myapp.arc

import java.util.Date
import org.apache.commons.lang.StringUtils.leftPad
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import net.liftweb.json._
import java.io._
import org.apache.log4j.{Logger, Level}
import org.apache.spark.ml.clustering._
import org.apache.commons.io.IOUtils


object thisIsTheArc extends preprocess with HDFSFileService{
  
  def main(args: Array[String]) = {
    // Start time
    val start_time = System.currentTimeMillis()
    
    var master = ""
    if (sys.env("USER").equals("lisa")) {
      master = "local"
    } else {
      master = "yarn-cluster"
    }
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("group2-scala-app")
      .setMaster(master)
      .set("spark.cores.max", "70")
      .set("spark.rdd.compress", "true")
      .set("spark.app.id", "group2-arc" + start_time.toString())
      .set("spark.yarn.am.memory", "350g")
      .set("spark.driver.memory", "350g")
      .set("spark.executor.memory", "350g")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.io.maxRetries", "10")
      .set("spark.reducer.maxSizeInFlight", "5g")
      .set("spark.driver.maxResultSize", "50g")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "5")
      
    val sc = new SparkContext(conf)
    
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    import sqlcontext.implicits._
    import org.apache.spark.sql.functions.udf
    
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    
    if (args.length == 0) {
      println("Arguments required: <task name: preprocess, kmeans or others> <task arguments>")
      System.exit(0)
    }
    
    val splitted_args = args.splitAt(1)
    val task = splitted_args._1(0)
    val task_args = splitted_args._2
     
    //--------------------------------------------------------------
    //          Preprocess the original data from Red Cross
    //--------------------------------------------------------------
    
    if (task.toLowerCase().equals("preprocess")){
      if (task_args.length != 6) {
      println("Preprocess arguments required: <path of states data> <path of summary data> <path of schema folder> <path of output folder> <percentage of sample size> <random seed>")
      System.exit(0)
      }
      
      //------ Set parameters ------
      val states_path = task_args(0); val summary_path = task_args(1); val schemas_folder = task_args(2)
      val output_path = task_args(3); val sample_pct = task_args(4).toFloat; val seed = task_args(5).toLong
      
      if (sample_pct > 1) {
        println("Sampling percentage should be less than 1")
        System.exit(0)
      }
      
      //--------------------------------------------------------------
      //          First process the csv file with RDD
      //--------------------------------------------------------------
      
      //------- Read some example file to a data RDD -------
      val data_text = sc.textFile(states_path, 75)
      val data_header = data_text.first()
      val data = data_text.filter { line => line != data_header }.sortBy(x => x).map { line => line.split(",") }
      val summary_data_text = sc.textFile(summary_path, 75)
      val summary_header = summary_data_text.first()
      val summary_data = summary_data_text.filter { line => line != summary_header }.map { line => line.split(",") }
      
      //------- Load schemas for datasets -------
      val states_schema_source = getFile(schemas_folder + "states_schema.json")
      val states_schema_string = try IOUtils.toString(states_schema_source, "UTF-8") finally states_schema_source.close()
      val states_schema = parse(states_schema_string)
      val summary_schema_source = getFile(schemas_folder + "summary_schema.json")
      val summary_schema_string = try IOUtils.toString(summary_schema_source, "UTF-8") finally summary_schema_source.close()
      val summary_schema = parse(summary_schema_string)
      
      //------- Count times of donations by arc_id -------
      val donate_cnt = data.map { line => (line(0), 1) }.reduceByKey(_ + _).cache() //Sum all of the value with same key
      
      //------- Store arc_id into separate lists of repeat donor and one-time donor -------
      val repeat_donor = donate_cnt.filter(v => v._2 > 1 ).map(v => v._1)
      val onetime_donor = donate_cnt.filter(v => v._2 == 1 ).map(v => v._1)
      
      //------- Split the lines with comma, change the delimeter if needed -------
      val data_cleaned = data
      .map { row => clean(row, states_schema) } // convert the values to corresponding data type
      .map { x => x :+ cntDonateOrder(x(0).toString())} // add "donation order" of the transactions of the same donor
      .map { x => Row.fromSeq(x.toSeq) }.cache()
      
      val summary_data_cleaned = summary_data
      .map { row => clean(row, summary_schema) }
      .map { x => Row.fromSeq(x.toSeq) }.cache()
      
      var sample_data = data_cleaned
      
      if (sample_pct < 1){
        //------- Create lists of sample repeat and one-time donor -------
        val donate_cnt_2000 = data_cleaned
        .filter { row => row.get(1).asInstanceOf[java.sql.Date].toLocalDate().getYear >= 2000}
        .map { line => (line(0), 1) }.reduceByKey(_ + _).cache()
        val repeat_donor_array = donate_cnt_2000.filter(v => v._2 > 1 ).map(v => v._1).sample(false, sample_pct, seed).collect()
        val onetime_donor_array = donate_cnt_2000.filter(v => v._2 == 1 ).map(v => v._1).sample(false, sample_pct, seed).collect()
        val sample_donors = repeat_donor_array ++ onetime_donor_array
        
        //------- Create sample data -------
        sample_data = data_cleaned.filter { row => sample_donors.contains(row.get(0)) }     
      } 
      
      //--------------------------------------------------------------
      //          Use SQL to merge states and summary data
      //--------------------------------------------------------------
      
      //------- Define udfs ------- 
      val udfCalAgeAtDonation= udf((donation_dt: java.sql.Date, birth_dt: java.sql.Date) => calAgeAtDonation(donation_dt, birth_dt))
      val udfCalDonateElapse = udf((arc_id: String, donation_dt: java.sql.Date) => calDonateElapse(arc_id, donation_dt))
      val udfGetDonateYear = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getYear)
      val udfGetDonateMonth = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getMonthValue)
      val udfGetDonateSeason = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getMonthValue match {
        case i if i >= 3 && i <= 5 => "Spring"
        case i if i >= 6 && i <= 8 => "Summer"
        case i if i >= 9 && i <= 11 => "Fall"
        case i if i == 12 || i== 1 || i == 2 => "Winter"
        case _ => "Unknown"
      })
      
      val udfIsRepeatDonor = udf((donation_cnt: Integer) => donation_cnt match {
        case i if i > 1 => 1
        case _ => 0
      })
      
      val udfGetAgeBucket = udf((age: Float) => age.round match {
        case i if i == 0 => "Unknown"
        case i if i >= 16 && i <= 18 => "High School"
        case i if i >= 19 && i <= 22 => "College"
        case i if i >= 23 && i <= 25 => "Post-College"
        case i if i >= 26 && i <= 30 => "late 20s"
        case i if i >= 31 && i <= 40 => "30s"
        case i if i >=41 => "old"
        case _ => "invalid" 
      })
      
      val udfGetFirst3Char = udf((zipc: String) => zipc match {
        case i if i.isInstanceOf[String] && i.length == 5 => i.substring(0, 3)
        case i if i.isInstanceOf[String] && i.length < 5 => leftPad(i,5,"0").substring(0, 3)
        case _ => "invalid"
        
      })
      
      val udfGetDonateBucket = udf((donation_cnt: Integer) => donation_cnt match {
        case i if i == 1 => "One-Time"
        case i if i > 1 && i <= 5 => "2-5"
        case i if i > 5 && i <= 10 => "6-10"
        case i if i > 10 => ">10"
        case _ => "invalid"
      })
      
      val udfGetActiveDonor = udf((avg_elapse: Float) => avg_elapse match {
        case i if i == 0 => "Not Applicable"
        case i if i <= 365 && i != 0 => "1"
        case i if i > 365 => "0"  
      })
      
      
      //------- Convert the schema JSON to StructType schema and add donation_cnt and repeat_donor for states data -------
      val states_structtype = createStructType(states_schema)
      .add(StructField("donation_order", IntegerType, true))
      
      val donate_cnt_strtype = StructType(List(StructField("cnt_arc_id", StringType, true), StructField("donation_cnt", IntegerType, true)))
      
      //------- Convert rdd to dataframe ------- 
      val states_df = sqlcontext.createDataFrame(sample_data, states_structtype)
      val summary_df = sqlcontext.createDataFrame(summary_data_cleaned, createStructType(summary_schema))
      val donate_cnt_df = sqlcontext.createDataFrame(donate_cnt.map(x => Row(x._1, x._2)), donate_cnt_strtype)
      
      
      states_df.registerTempTable("states")
      summary_df.registerTempTable("summary")
      
      val select_useful_summary = "SELECT arc_id AS summary_id, birth_dt, race, gender, zip5c FROM summary"
      val useful_summary_df = sqlcontext.sql(select_useful_summary)
      
      //------- Join states and summary data -------
      var states_summary_df = states_df.join(useful_summary_df, states_df("arc_id") === useful_summary_df("summary_id"), "left_outer").drop("summary_id")
      states_summary_df = states_summary_df.join(donate_cnt_df, states_summary_df("arc_id") === donate_cnt_df("cnt_arc_id"), "left_outer").drop("cnt_arc_id")
  
      //------- Add new columns to the dataframe ------- 
      states_summary_df = states_summary_df.withColumn("age_at_donation", udfCalAgeAtDonation(states_summary_df("donation_dt"), states_summary_df("birth_dt")))
      .withColumn("donation_year", udfGetDonateYear(states_summary_df("donation_dt")))
      .withColumn("donation_month", udfGetDonateMonth(states_summary_df("donation_dt")))
      .withColumn("donation_season", udfGetDonateSeason(states_summary_df("donation_dt")))
      .withColumn("site_zip3", udfGetFirst3Char(states_summary_df("site_zip")))
      .withColumn("donor_zip3", udfGetFirst3Char(states_summary_df("zip5c")))
      .withColumn("repeat_donor", udfIsRepeatDonor(states_summary_df("donation_cnt")))
      .withColumn("since_last_donation", udfCalDonateElapse(states_summary_df("arc_id"), states_summary_df("donation_dt")))
      
      states_summary_df = states_summary_df.withColumn("age_bucket", udfGetAgeBucket(states_summary_df("age_at_donation")))
      
      states_summary_df.registerTempTable("ssdata")
      
      val cal_frequency = "SELECT sub.arc_id AS freq_arc_id, CASE WHEN sub.donation_cnt > 1 THEN sub.oldest_to_recent/(sub.donation_cnt-1) ELSE 0 END AS avg_elapse FROM (SELECT arc_id, SUM(since_last_donation) as oldest_to_recent, donation_cnt FROM ssdata GROUP BY arc_id, donation_cnt) sub"
      var frequency_df = sqlcontext.sql(cal_frequency)
      frequency_df = frequency_df.withColumn("active", udfGetActiveDonor(frequency_df("avg_elapse")))
      states_summary_df = states_summary_df.join(frequency_df, states_summary_df("arc_id") === frequency_df("freq_arc_id"), "left_outer").drop("freq_arc_id")
      
      // TODO: Uncomment the following line if need to save mixed dataset
      if (sample_pct < 1){
        states_summary_df.write.parquet(output_path + start_time + sample_pct + "_" + seed + "_sample_data")  
      } else {
        states_summary_df.write.parquet(output_path + start_time + "_allstates_summary_data" )
      }
           
    
    } else if (task.toLowerCase().equals("kmeans")) { 
      //--------------------------------------------------------------
      //                    KMeans Clustering
      //--------------------------------------------------------------
      
      if (task_args.length != 3) {
        println("KMeans arguments required: <path of input data> <number of cluster> <number of iteration>")
        System.exit(0)
      }
      
      val path = task_args(0); val ncluster= task_args(1).toInt; val niter = task_args(2).toInt; 
      
      val data = sqlcontext.read.parquet(path+"part*.gz.parquet")
      data.registerTempTable("sample_data")
      
      //------ Spark ML ------
    
      //------ Interested in donor's first donation -----
      
//      var ml_result_path = output_path + start_time + "_mlResult/"
      
//      createFolder(ml_result_path)
      
      val features = Array("donation_year", "donation_season", "age_bucket", "walk_in_ind", "donation_type",
          "deferral_ind", "sponsor_category", "race", "gender", "StateAbbr", "repeat_donor")
          
      val string_features = Array("donation_season", "age_bucket", "donation_type", "sponsor_category", "race", "gender", "site_zip3")
      
      val ml_data = sqlcontext.sql(s"SELECT ${features.mkString(",")} FROM sample_data WHERE donation_year >= 2000 AND first_donat_ind = 1 AND donation_order = 1 AND age_bucket != \'Unknown\' ")
      
      val ml_km = new arcMl().setFeatureSet_(features).setStringFeatureList_(string_features)
      
      val transformed_ml_data = ml_km.transform(ml_data).cache()
      
//      transformed_ml_data.write.parquet(ml_result_path + "ml_data")
      
      var result = "K,WSSSE,Centers\n"
      
      var m = ml_km.kmean(transformed_ml_data, ncluster, niter)
      
      println(features.mkString("/t"))
      println("Number of Cluster: " + ncluster)
      println("Within Set Sum of Squared Errors: " + m.computeCost(transformed_ml_data).toString())
      println("Final Centers: ")
      m.clusterCenters.foreach(println)
      
//      for (i <- 15 to 30){
//        m = ml_km.kmean(transformed_ml_data, i, 10)
//        println("Number of Cluster: " + i)
//        println("Within Set Sum of Squared Errors: " + m.computeCost(transformed_ml_data).toString())
//        println("Final Centers: ")
//        m.clusterCenters.foreach(println)
//        
//        result = result + (i.toString() + "," + 
//            m.computeCost(transformed_ml_data).toString() + "," + 
//            "\"" + m.clusterCenters.mkString("\";\"") + "\"\n")
//      }
      
//      saveFile(ml_result_path + "kmeans_result.csv", result.getBytes)
      
    }
    
    
    
    
    //Stop the Spark context
    sc.stop
  }
  
}