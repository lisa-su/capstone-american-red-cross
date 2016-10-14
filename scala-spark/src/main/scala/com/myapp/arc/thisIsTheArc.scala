package com.myapp.arc

import java.util.Date
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import net.liftweb.json._
import java.io._
import org.apache.log4j.{Logger, Level}
import org.apache.spark.ml.clustering._


object thisIsTheArc extends preprocess {
  
  def main(args: Array[String]) = {
    // Start time
    val start_time = System.currentTimeMillis()
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("group2-scala-app")
      .setMaster("local") // TODO: Change to "yarn-cluster" before package the jar to lab server 
    val sc = new SparkContext(conf)
    
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    
    if (args.length != 6) {
      println("Arguments required: <path of states data> <path of summary data> <path of schema folder> <path of output folder> <percentage of sample size> <random seed>")
      System.exit(0)
    }
    
    //------ Set parameters ------
    val states_path = args(0); val summary_path = args(1); val schemas_folder = args(2)
    val output_path = args(3); val sample_pct = args(4).toFloat; val seed = args(5).toLong
    
    //--------------------------------------------------------------
    //          First process the csv file with RDD
    //--------------------------------------------------------------
    
    //------- Read some example file to a data RDD -------
    val data_text = sc.textFile(states_path)
    val data_header = data_text.first()
    val data = data_text.filter { line => line != data_header }.sortBy(x => x).map { line => line.split(",") }
    val summary_data_text = sc.textFile(summary_path)
    val summary_header = summary_data_text.first()
    val summary_data = summary_data_text.filter { line => line != summary_header }.map { line => line.split(",") }
    
    //------- Load schemas for datasets -------
    val states_schema_source = scala.io.Source.fromFile(schemas_folder + "states_schema.json")
    val states_schema_string = try states_schema_source.mkString finally states_schema_source.close()
    val states_schema = parse(states_schema_string)
    val summary_schema_source = scala.io.Source.fromFile(schemas_folder + "summary_schema.json")
    val summary_schema_string = try summary_schema_source.mkString finally summary_schema_source.close()
    val summary_schema = parse(summary_schema_string)
    
    //------- Count times of donations by arc_id -------
    val donate_cnt = data.map { line => (line(0), 1) }.reduceByKey(_ + _) //Sum all of the value with same key
    val donate_cnt_collected = donate_cnt.collect()
    
    //------- Store arc_id into separate lists of repeat donor and one-time donor -------
    val repeat_donor = donate_cnt.filter(v => v._2 > 1 ).map(v => v._1)
    val onetime_donor = donate_cnt.filter(v => v._2 == 1 ).map(v => v._1)
    
    //------- Split the lines with comma, change the delimeter if needed -------
    val data_cleaned = data
    .map { row => clean(row, states_schema) } // convert the values to corresponding data type
    .map { x => isRepeatDonor(x, donate_cnt_collected) } // add "repeat_donor_ind" and "total_donation_cnt"
    .map { x => x :+ cntDonateOrder(x(0).toString())} // add "donation order" of the transactions of the same donor
    //.map { x => x :+ x(1).asInstanceOf[java.sql.Date].toLocalDate().getYear} // add "donation_year"
    //.map { x => x :+ x(1).asInstanceOf[java.sql.Date].toLocalDate().getMonthValue } // add "donation_month"
    .map { x => Row.fromSeq(x.toSeq) }
    
    val summary_data_cleaned = summary_data
    .map { row => clean(row, summary_schema) }
    .map { x => Row.fromSeq(x.toSeq) }
    
    //------- Create lists of sample repeat and one-time donor -------
    val donate_cnt_2000 = data_cleaned
    .filter { row => row.get(1).asInstanceOf[java.sql.Date].toLocalDate().getYear >= 2000}
    .map { line => (line(0), 1) }.reduceByKey(_ + _)
    val repeat_donor_array = donate_cnt_2000.filter(v => v._2 > 1 ).map(v => v._1).sample(false, sample_pct, seed).collect()
    val onetime_donor_array = donate_cnt_2000.filter(v => v._2 == 1 ).map(v => v._1).sample(false, sample_pct, seed).collect()
    val sample_donors = repeat_donor_array ++ onetime_donor_array
    
    //------- Create sample data -------
    val sample_data = data_cleaned.filter { row => sample_donors.contains(row.get(0)) }
    
    //--------------------------------------------------------------
    //          Use SQL to merge states and summary data
    //--------------------------------------------------------------
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    import sqlcontext.implicits._
    import org.apache.spark.sql.functions.udf
    
    //------- Define udfs ------- 
    val udfCalAgeAtDonation= udf((donation_dt: java.sql.Date, birth_dt: java.sql.Date) => calAgeAtDonation(donation_dt, birth_dt))
    val udfGetDonateYear = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getYear)
    val udfGetDonateMonth = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getMonthValue)
    val udfGetDonateSeason = udf((donation_dt: java.sql.Date) => donation_dt.toLocalDate().getMonthValue match {
      case i if i >= 3 && i <= 5 => "Spring"
      case i if i >= 6 && i <= 8 => "Summer"
      case i if i >= 9 && i <= 11 => "Fall"
      case i if i == 12 || i== 1 || i == 2 => "Winter"
      case _ => "Unknown"
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
      case i if i.isInstanceOf[String] => i.substring(0, 3)
      case _ => "invalid"
      
    })
    
    
    //------- Convert the schema JSON to StructType schema and add donation_cnt and repeat_donor for states data -------
    val states_structtype = createStructType(states_schema)
    .add(StructField("donation_cnt", IntegerType, true))
    .add(StructField("repeat_donor", IntegerType, true))
    .add(StructField("donation_order", IntegerType, true))
    
    //------- Convert rdd to dataframe ------- 
    val states_df = sqlcontext.createDataFrame(sample_data, states_structtype)
    val summary_df = sqlcontext.createDataFrame(summary_data_cleaned, createStructType(summary_schema))

    states_df.registerTempTable("states")
    summary_df.registerTempTable("summary")
    
    val select_useful_summary = "SELECT arc_id AS summary_id, birth_dt, race, gender, zip5c FROM summary"
    val useful_summary_df = sqlcontext.sql(select_useful_summary)
    
    //------- Join states and summary data -------
    var states_summary_df = states_df.join(useful_summary_df, states_df("arc_id") === useful_summary_df("summary_id"), "left_outer")
    .drop("summary_id")

    //------- Add new columns to the dataframe ------- 
    states_summary_df = states_summary_df.withColumn("age_at_donation", udfCalAgeAtDonation(states_summary_df("donation_dt"), states_summary_df("birth_dt")))
    .withColumn("donation_year", udfGetDonateYear(states_summary_df("donation_dt")))
    .withColumn("donation_month", udfGetDonateMonth(states_summary_df("donation_dt")))
    .withColumn("donation_season", udfGetDonateSeason(states_summary_df("donation_dt")))
    .withColumn("site_zip3", udfGetFirst3Char(states_summary_df("site_zip")))
    .withColumn("donor_zip3", udfGetFirst3Char(states_summary_df("zip5c")))
    
    states_summary_df = states_summary_df.withColumn("age_bucket", udfGetAgeBucket(states_summary_df("age_at_donation")))
    
    // TODO: Uncomment the following line if need to save mixed dataset
    states_summary_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true")
    .save(output_path + start_time + "_" + sample_pct + "_" + seed + "_sample_data")
    
    states_summary_df.registerTempTable("sample_data")
    
    
    //------ Spark ML ------
    
    //------ Interested in donor's first donation -----
    
    var ml_result_path = output_path + start_time + "_mlResult/"
    
    if (!new File(ml_result_path).isDirectory()) {
      new File(ml_result_path).mkdirs()
    }
    
    val features = Array("donation_year", "donation_season", "age_bucket", "walk_in_ind", "donation_type",
        "deferral_ind", "sponsor_category", "race", "gender", "site_zip3", "repeat_donor")
        
    val string_features = Array("donation_season", "age_bucket", "donation_type", "sponsor_category", "race", "gender", "site_zip3")
    
    val ml_data = sqlcontext.sql(s"SELECT ${features.mkString(",")}, repeat_donor FROM sample_data WHERE first_donat_ind = 1 AND donation_order = 1")
    
    val ml_km = new arcMl().setFeatureSet_(features).setStringFeatureList_(string_features)
    
    val transformed_ml_data = ml_km.transform(ml_data)
    
    transformed_ml_data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(ml_result_path + "ml_data")
    
    val pw = new PrintWriter(new File(ml_result_path + "kmeans_result.csv" ))
    pw.write("K,WSSSE,Centers\n")
    
    var m = ml_km.kmean(transformed_ml_data, 2, 10)
    for (i <- 15 to 30){
      m = ml_km.kmean(transformed_ml_data, i, 10)
      println("Number of Cluster: " + i)
      pw.write(i.toString() + ",")
      println("Within Set Sum of Squared Errors: " + m.computeCost(transformed_ml_data).toString())
      pw.write(m.computeCost(transformed_ml_data).toString() + ",")
      pw.write("\"" + m.clusterCenters.mkString("\";\"") + "\"")
    }
    
    pw.close
   
    
    
    
    
    //Stop the Spark context
    sc.stop
  }
  
}