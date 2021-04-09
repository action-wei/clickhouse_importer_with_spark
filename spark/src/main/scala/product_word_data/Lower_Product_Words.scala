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

// 产品词下位关系表
object Lower_Product_Words_Importer {

    case class Record(word: String, relation: String, word_list: String)

    def splitProdcutWords(line: String): collection.mutable.MutableList[Record] = {
        var arrRecordList = collection.mutable.MutableList[Record]()
        try {
            var arrItem = line.split("\t")
            var word = arrItem(0)
            var lower_word_list = arrItem(1)
            var product = lower_word_list.split(",")
            var lower_words = new Array[String](product.length)
            for (i <- 0 to (product.length - 1)){
                var tmp = product(i).split(":")
                lower_words(i) = tmp(0)
            }
            var buf = new StringBuilder
            for (i <- 0 to (lower_words.length - 2)){
                buf ++= lower_words(i)
                buf += ','
            }
            buf ++= lower_words(lower_words.length - 1)
            var lower_word_list_string = buf.toString
            arrRecordList += new Record(word, "lower", lower_word_list_string)
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
        
        var split_string = input_path.split("/")
        var date = split_string(split_string.length - 3).split("=")(1)
        var category = split_string(split_string.length - 2).split("=")(1)
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
        val df:Dataset[Row] = textData.flatMap(line => splitProdcutWords(line)).toDF().withColumn("product_version",dateUdfMethod(col("word"))).withColumn("category", categoryUdfMethod(col("word")))

        val clickhouse: Clickhouse = new Clickhouse()
        clickhouse.setInitParame(database, table_name, field_define)

        clickhouse.process(df)
        println("spark task finished")
    }

}

 
