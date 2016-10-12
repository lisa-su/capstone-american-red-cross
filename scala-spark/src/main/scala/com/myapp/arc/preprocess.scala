package com.myapp.arc

import java.util.Date
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import net.liftweb.json._

trait preprocess {
  case class fieldDtype(field: String, dtype: String)
  case class DataTypeInvalidException(smth:String)  extends Exception(smth)
  
  //------- Function to parse schema JSON object to field_dtype(field_name, dtype) -------
  def parseDtype(schema: JValue): List[fieldDtype] = {
    val fields = for {
      JArray(fields) <- schema \ "fields"
      JObject(field) <- fields
      JField("name", JString(name)) <- field
      JField("type", JString(dtype)) <- field
    } yield fieldDtype(name, dtype)
    
    return fields
  } 
  
  //------- Function that will return DateType by specified string -------
  def getDatatype(dtype: String): DataType = {
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
  
  //------- Function that uses the schema JSON to create StructType -------
  def createStructType(schema: JValue): StructType = {
    val fields = parseDtype(schema)
    val struct_schema = StructType(fields.map { x => StructField(x.field, getDatatype(x.dtype), true) })

    return struct_schema
  }
  
  //------- Function to convert the columns to right data types by schema -------
  def clean(row: Array[String], schema: JValue): Array[_] = {
    val null_val = List("na", "none", "")
    val fields = parseDtype(schema)
    
    if (row.size != fields.size){
      println("------")
      println("Something's Wrong")
      println("row size: " + row.size)
      println("field size: " + fields.size)
      println("------")
      throw new Exception()
    }
    
    def validate(x: (String, fieldDtype) ): Any = { // x: (value, field_dtype)
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
  
  //------- Function to create the indicator of repeat donor -------
  def isRepeatDonor(row: Array[_], donate_count: Array[(String, Int)]): Array[_] = {
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
  
  //------- Function to calculate the donor's age at respective donation -------
  def calAgeAtDonation(donation_dt: java.sql.Date, birth_dt: java.sql.Date): Float = {
    if (donation_dt != null && birth_dt != null) {
      return (donation_dt.getTime - donation_dt.getTime) / (1000 * 60 * 60 * 24 * 365)
    } else {
      return 0  
    }
  }

  //------- Function to count the order of the donation from the same donor -------
  var id = ""
  var cnt = 0
  def cntDonateOrder(this_id: String): Integer = {
      if (!this_id.equals(id)) {
        id = this_id.toString()
        cnt = 1
      } else {
        cnt += 1
      }      
    return cnt
  }
  
}