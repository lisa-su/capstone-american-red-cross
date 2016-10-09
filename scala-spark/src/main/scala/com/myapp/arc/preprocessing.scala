package com.myapp.arc

import java.util.Date
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import net.liftweb.json._


object preprocessing {
  case class field_dtype(field: String, dtype: String)
  case class DataTypeInvalidException(smth:String)  extends Exception(smth)
  val get_year = new java.text.SimpleDateFormat("yyyy") 
  
  def parse_dtype(schema: JValue): List[preprocessing.field_dtype] = {
    val fields = for {
      JArray(fields) <- schema \ "fields"
      JObject(field) <- fields
      JField("name", JString(name)) <- field
      JField("type", JString(dtype)) <- field
    } yield field_dtype(name, dtype)
    
    return fields
  
  } 
  
  def get_datatype(dtype: String): DataType = {
    if (dtype.equals("integer")){
      return IntegerType
    } else if (dtype.equals("float")) {
      return FloatType
    } else if (dtype.equals("date")) {
      return DateType
    } else if (dtype.equals("boolean")) {
      return BooleanType
    } else {
      return StringType
    }
    
  }
  
  def create_structtype(schema: JValue): StructType = {
    val fields = parse_dtype(schema)
    val struct_schema = StructType(fields.map { x => StructField(x.field, get_datatype(x.dtype), true) })
    
    
    return struct_schema
  }
  
  def clean(row: Array[String], schema: JValue): Array[_] = {
    val null_val = List("na", "none", "")
    val fields = parse_dtype(schema)
    
    if (row.size != fields.size){
      println("------")
      println("Something's Wrong")
      println("row size: " + row.size)
      println("field size: " + fields.size)
      println("------")
      throw new Exception()
    }
    
    def validate(x: (String, preprocessing.field_dtype) ): Any = { // x: (value, field_dtype)
      var new_x = x._1.trim()
      val d = x._2
      if (null_val.contains(new_x.toLowerCase()) || new_x == null) {
        return null
      } else {
        if (d.dtype == "string" && !d.field.contains("abbr")) {
          return new_x.capitalize
        } else if (List("Latitude","Longitude").contains(d.field) && new_x == "0"){
          return null
        } else if (d.dtype != "string") {
          if (d.dtype == "integer"){
            return new_x.toInt
          } else if (d.dtype == "float") {
            return new_x.toFloat
          } else if (d.dtype == "date" && new_x.contains("-")) {
            val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
//            val date = new java.sql.Date(format.parse(new_x).getDate)
            val date = java.sql.Date.valueOf(new_x)
            return date
          } else if (d.dtype == "date" && new_x.contains("/")) {
            val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
            val date = new java.sql.Date(format.parse(new_x).getDate)
            return date
          } else {
            throw new DataTypeInvalidException("\n------\n" + d + "\nvalue: " + x + "\n------")
          }
        } else {
          return new_x
        }
      }
    }
    val matched = row zip fields
    var new_row = for { m <- matched} yield validate(m)
    
    return new_row
  
  } 
  
  def is_repeat_donor(row: Array[_], donate_count: Array[(Any, Int)]): Array[_] = {
    val this_id = row.toList(0)
    val this_donor_cnt = donate_count.filter(p => this_id.equals(p._1))
    try {
      if (this_donor_cnt(0)._2 > 1){
        return row :+ this_donor_cnt(0)._2 :+ 1
      } else if (this_donor_cnt(0)._2 == 1) {
        return row :+ this_donor_cnt(0)._2 :+ 0
      } else {
        return row :+ null :+ null
      }
    } catch{
      case e: java.lang.ArrayIndexOutOfBoundsException => {
        println("=====Caught ArrayIndexOutOfBoundsException here!=====")
        println("this_donor_cnt length = " + this_donor_cnt.length)
        if (this_donor_cnt.length > 0 ){
          println(this_donor_cnt.deep.mkString(","))
        }
        return row :+ null :+ null
        }
    }
  
  }
  
  def cal_age_at_donation(donation_dt: java.sql.Date, birth_dt: java.sql.Date): Float = {
    if (donation_dt != null && birth_dt != null) {
      return (donation_dt.getTime - donation_dt.getTime) / (1000 * 60 * 60 * 24 * 365)
    } else {
      return 0  
    }
    
  }
        
        
  
  def main(args: Array[String]) = {
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("preprocessing")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    //------- Read some example file to a data RDD -------
    //TODO: change the data source when running on the cluster
    val data = sc.textFile("data/state13_first_1k.csv")
    val data_header = data.first()
    val summary_data = sc.textFile("data/summary_first_1k.csv")
    val summary_header = summary_data.first()
    
    //------- Load schemas for datasets -------
    val states_schema_source = scala.io.Source.fromFile("data/states_schema.json")
    val states_schema_string = try states_schema_source.mkString finally states_schema_source.close()
    val states_schema = parse(states_schema_string)
    val summary_schema_source = scala.io.Source.fromFile("data/summary_schema.json")
    val summary_schema_string = try summary_schema_source.mkString finally summary_schema_source.close()
    val summary_schema = parse(summary_schema_string)
    
    //------- Split the lines with comma, change the delimeter if needed -------
    var data_splitted = data.filter { line => line != data_header }
    .map { line => line.split(",") }
    .map { row => clean(row, states_schema) }
    var summary_data_splitted = summary_data.filter { line => line != summary_header }
    .map { line => line.split(",") }
    .map { row => clean(row, summary_schema) }
    .map { x => Row.fromSeq(x.toSeq) }
   
    //-------Create a list of arc_id that the donor has activity after 2000-------
    //TODO: Currently only works for subtracting data after YYYY and it can also running through loops to create subsets
    val donor_after_2000 = data_splitted
    .filter { row => row(1).asInstanceOf[java.sql.Date].toLocalDate().getYear >= 2000}
    .map { row => row(0) }.collect() 
    
    //-------Preparing data of donors that have activities after 2000-------
    var data_after_2000 = data_splitted.filter( row => donor_after_2000.contains(row(0)))
    
    // Count times of donations by arc_id
    val donate_cnt = data_after_2000.map { line => (line(0), 1) }.reduceByKey(_ + _) //Sum all of the value with same key
    val donate_cnt_collected = donate_cnt.collect()
    
    // Save arc_id into separate lists of repeat donor and one-time donor
    val repeat_donor = donate_cnt.filter(v => v._2 > 1 ).map(v => v._1)
    val onetime_donor = donate_cnt.filter(v => v._2 == 1 ).map(v => v._1)
    
    // TODO: Uncomment following 2 lines to save the donor acr id list
    // repeat_donor.saveAsTextFile("data/repeat_donor_list")
    // onetime_donor.saveAsTextFile("data/onetime_donor_list")
     
    // Get lists of sample repeat and one-time donor 
    val repeat_donor_array = repeat_donor.sample(false, 0.01, 100).collect()
    val onetime_donor_array = onetime_donor.sample(false, 0.01, 100).collect()
    val sample_donors = repeat_donor_array ++ onetime_donor_array
    
    //-------Create sample data and added is repeat donor and total donation count column-------
    val sample_data = data_after_2000.filter { row => sample_donors.contains(row(0)) }
    .map { x => is_repeat_donor(x, donate_cnt_collected) }
    .map { x => Row.fromSeq(x.toSeq) }
    
    // TODO: Uncomment this line to save the sample data
    // sample_data.map { x => x.mkString(",") }.saveAsTextFile("data/sample_data")
    
    //-------Use SQL to merge states and summary data----
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    import sqlcontext.implicits._
    
    // Convert the schema JSON to StructType schema and add donation_cnt and repeat_donor for states data
    val final_states_schema = create_structtype(states_schema)
    .add(StructField("donation_cnt", IntegerType, true))
    .add(StructField("repeat_donor", IntegerType, true))

    // Convert rdd to dataframe
    // TODO: change states_df's source RDD, now is sample_data, if need to use whole states data or other subsets
    val states_df = sqlcontext.createDataFrame(sample_data, final_states_schema)
    val summary_df = sqlcontext.createDataFrame(summary_data_splitted, create_structtype(summary_schema))
    // Register the data frames as tables
    states_df.registerTempTable("states")
    summary_df.registerTempTable("summary")
    
    val select_useful_summary = "SELECT arc_id AS summary_id, birth_dt, race, gender, zip5c FROM summary"
    val useful_summary_df = sqlcontext.sql(select_useful_summary)
    
    // Join states and summary data
    var states_summary_df = states_df.join(useful_summary_df, states_df("arc_id") === useful_summary_df("summary_id"), "left_outer")
    .drop("summary_id")
   
    // Define udf for calculating the age at donation
    import org.apache.spark.sql.functions.udf

    val udf_cal_age_at_donation= udf((donation_dt: java.sql.Date, birth_dt: java.sql.Date) => 
      cal_age_at_donation(donation_dt, birth_dt))

    // Add new column "age_at_donation" to the dataframe 
    states_summary_df = states_summary_df.withColumn("age_at_donation", udf_cal_age_at_donation(states_summary_df("donation_dt"), states_summary_df("birth_dt")))
    // TODO: Uncomment the following line if need to save mixed dataset
    // states_summary_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("data/sample_data_mixed")
    
       
    //Stop the Spark context
    sc.stop
  }
}