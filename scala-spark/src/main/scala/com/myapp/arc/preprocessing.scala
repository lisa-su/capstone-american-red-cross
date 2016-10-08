package com.myapp.arc
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
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
            return format.parse(new_x)
          } else if (d.dtype == "date" && new_x.contains("/")) {
            val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
            return format.parse(new_x)
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
  
  def is_repeat_donor(row: Array[_], onetime: Array[_], repeat: Array[_]): Array[Any] = {
    val this_id = row.toList(0)
    
    if (onetime contains this_id) {
      return row :+ 0
    } else if (repeat contains this_id) {
      return row :+ 1
    } else {
      return row :+ null
    }
  
  }
  
  def main(args: Array[String]) = {
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("preprocessing")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //Read some example file to a data RDD
    val data = sc.textFile("data/state13_first_1k.csv")
    val data_header = data.first()
    val summary_data = sc.textFile("data/summary_first_1k.csv")
    val summary_header = summary_data.first()
    
    //Load schemas for datasets
    val states_schema_source = scala.io.Source.fromFile("data/states_schema.json")
    val states_schema_string = try states_schema_source.mkString finally states_schema_source.close()
    val states_schema = parse(states_schema_string)
    val summary_schema_source = scala.io.Source.fromFile("data/summary_schema.json")
    val summary_schema_string = try summary_schema_source.mkString finally summary_schema_source.close()
    val summary_schema = parse(summary_schema_string)
    
    var data_splitted = data.filter { line => line != data_header }
    .map { line => line.split(",") }
    .map { row => clean(row, states_schema) }
    var summary_data_splitted = summary_data.filter { line => line != summary_header }
    .map { line => line.split(",") }
    .map { row => clean(row, summary_schema) }
   
    // Create a list of arc_id that the donor has activity after 2000
    val donor_after_2000 = data_splitted.filter { row => get_year.format((row(1))).toInt >= 2000}
    .map { row => row(0) }.collect() 
    
    // data of donors that have activities after 2000
    var data_after_2000 = data_splitted.filter( row => donor_after_2000.contains(row(0)))
    
    //Count times of donations by arc_id
    val donate_cnt = data_after_2000.map { line => (line(0), 1) }.reduceByKey(_ + _) //Sum all of the value with same key
    //Save arc_id into separate lists of repeat donor and one-time donor
    val repeat_donor = donate_cnt.filter(v => v._2 > 1 ).map(v => v._1)
    repeat_donor.saveAsTextFile("data/repeat_donor_list")
    val onetime_donor = donate_cnt.filter(v => v._2 == 1 ).map(v => v._1)
    onetime_donor.saveAsTextFile("data/onetime_donor_list")
    
    val repeat_donor_sample = repeat_donor.sample(false, 0.01, 100)
    val repeat_donor_array = repeat_donor_sample.collect()
    val onetime_donor_sample = onetime_donor.sample(false, 0.01, 100)
    val onetime_donor_array = onetime_donor_sample.collect()
    val sample_donors = repeat_donor_array ++ onetime_donor_array
    var sample_data = data_after_2000.filter { row => sample_donors.contains(row(0)) }.map { x => is_repeat_donor(x, onetime_donor_array, repeat_donor_array) }
    
    sample_data.map { x => x.mkString(",") }.saveAsTextFile("data/sample_data")

    //Stop the Spark context
    sc.stop
  }
}