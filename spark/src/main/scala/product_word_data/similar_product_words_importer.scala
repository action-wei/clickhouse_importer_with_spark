package clickhouse_importer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

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

object Similar_Product_Words_Importer {

    case class Record(word: String, similar_words: String)

    def splitProdcutWords(line: String): collection.mutable.MutableList[Record] = {
        var arrRecordList = collection.mutable.MutableList[Record]()
        try {
            var arrItem = line.split("\t")
            var word = arrItem(0)
            var similar_words = arrItem(1)
            arrRecordList += new Record(word, similar_words)
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
            database = args(0)
            table_name = args(1)
            input_path = args(2)
            println("input parameters: \ndatabase:" + database + " \ntable_name:" + table_name + "\ninput_path=" + input_path)
		}else{
            println("Error: input parameters.")
            System.exit(1)
        }

		val sparkConf = new SparkConf().setAppName("GraphDataImporter")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        val field_define = """
            word String,
            similar_words String
        """

        val textData = sc.textFile(input_path)
        val df:Dataset[Row] = textData.flatMap(line => splitProdcutWords(line)).toDF()

        val clickhouse: Clickhouse = new Clickhouse()
        clickhouse.setInitParame(database, table_name, field_define)

        clickhouse.process(df)
        println("spark task finished")
    }

}

 
