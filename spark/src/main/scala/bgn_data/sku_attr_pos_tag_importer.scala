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

object Sku_Attr_PosTag_Importer {

    def main(args: Array[String]) {
        var database = ""
        var tableName = ""
        var dataDate = ""
        var sSQL = ""
		
		if(args.length > 2)
		{
            database = args(0)
            tableName = args(1)
            dataDate = args(2)
            println("input parameters: \ndataDate=" + dataDate)
		}else{
            println("Error: input parameters.")
            System.exit(1)
        }

        val sparkSession = SparkSession.builder().appName("ClickhouseDataImporter" + dataDate).enableHiveSupport().getOrCreate()
		import sparkSession.implicits._

        val fieldDefine = """
            id String, 
            sku_name String, 
            pos_tag String
        """

        sSQL = """select skuid as id, skuname as sku_name, pos_tag from ad_bgn.taxonomy_sku_pos_tag where dt='""" + dataDate + """'"""
  
  		val df: Dataset[Row] = sparkSession.sql(sSQL).na.fill(0)

        val clickhouse: Clickhouse = new Clickhouse()
        clickhouse.setInitParame(database, tableName, fieldDefine)

        clickhouse.process(df)
        println("spark task finished")
    }

}

 
