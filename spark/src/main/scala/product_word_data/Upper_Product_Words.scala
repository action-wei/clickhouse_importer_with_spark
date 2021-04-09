package clickhouse_importer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.functions.broadcast 
import java.text.DecimalFormat
import java.text.SimpleDateFormat

object Upper_Product_Words_Importer {

    case class Record(word_base: String, relation: String, lower_word: String)

    def splitProdcutWords(line: String): collection.mutable.MutableList[Record] = {
        var arrRecordList = collection.mutable.MutableList[Record]()
        try {
            var arrItem = line.split("\t")
            var word_base = arrItem(0)
            var word_list = arrItem(1).split(",")
            for( item <- word_list)
            {
                var tmp = item.split(":")
                arrRecordList += new Record(word_base,"upper", tmp(0))
            }
        } catch {
            case e : Throwable => ""
        }
        arrRecordList
    }

    def main(args: Array[String]) {
        var input_path = ""
        var database = ""
        var table_name = ""
		
		if(args.length > 2)
		{
            input_path = args(0)
            database = args(1)
            table_name = args(2)
            println("input parameters: \ninput_path=" + input_path + " \ndatabase:" + database + " \ntable_name:" + table_name)
		}else{
            println("Error: input parameters.")
            System.exit(1)
        }
        var split_lst = input_path.split("/")
        var date = split_lst(split_lst.length - 3).split("=")(1)
        var category = split_lst(split_lst.length - 2).split("=")(1)
	val sparkConf = new SparkConf().setAppName("GraphDataImporter")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        val field_define = """
            product_version String,
            category String,
            word String,
            relation String,
            word_list String
        """
	val dateUdfMethod = udf((arg: String) => date)
        val categoryUdfMethod = udf((arg: String) => category)
        val textData = sc.textFile(input_path)
        val df:Dataset[Row] = textData.flatMap(line => splitProdcutWords(line)).toDF().withColumn("product_version", dateUdfMethod(col("word"))).withColumn("category", categoryUdfMethod(col("word"))).groupBy("lower_word","relation").agg(concat_ws(",", collect_list("word_base")) as "word_list").withColumnRenamed("lower_word","word").select("product_version","category","word", "relation", "word_list")

        val clickhouse: Clickhouse = new Clickhouse()
        clickhouse.setInitParame(database, table_name, field_define)

        clickhouse.process(df)
        println("spark task finished")
    }

}

 
