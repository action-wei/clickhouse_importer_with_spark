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

object Vertex_Sku_Importer {

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
            type String, 
            display String, 
            is_attr String, 
            fs_attr String, 
            ss_attr String, 
            bs_attr String
        """

        sSQL = """select concat('sku_', cast(item_sku_id as String)) as id, 
            'sku' as type, 
            sku_name as display, 
            concat('{',
                '\"brand_code\":', cast(brand_code as String), 
                ',\"shop_id\":', cast(shop_id as String), 
                ',\"cid1\":', cast(item_first_cate_cd as String), 
                ',\"cid2\":', cast(item_second_cate_cd as String), 
                ',\"cid3\":', cast(item_third_cate_cd as String), 
                ',\"main_sku_id\":',cast(main_sku_id as String),
            '}') as is_attr,
            concat('{', 
                '\"category\":\"', category, 
                '\",\"item_type\":\"',item_type,
                '\",\"colour\":\"', colour, 
                '\",\"size\":\"', size, 
                '\",\"product_words_list\":\"',product_words_list,
                '\",\"best_product\":\"',best_product,
                '\",\"shop_name\":\"',shop_name,
                '\",\"brandname_full\":\"',brandname_full, 
                '\",\"brandname_cn\":\"',brandname_cn, 
                '\",\"brandname_en\":\"',brandname_en,
                '\",\"cid1_name\":\"',item_first_cate_name,
                '\",\"cid2_name\":\"',item_second_cate_name,
                '\",\"cid3_name\":\"',item_third_cate_name,
                '\",\"itemtype_title\":\"',itemtype_title,
            '\"}') as ss_attr,
            '{}' as bs_attr, 
            '{}' as fs_attr
            from ad_bgn.ad_bgn_sku_attribute 
            where dt='""" + dataDate + """'"""
  
  		val df: Dataset[Row] = sparkSession.sql(sSQL).na.fill(0)

        val clickhouse: Clickhouse = new Clickhouse()
        clickhouse.setInitParame(database, tableName, fieldDefine)

        clickhouse.process(df)
        println("spark task finished")
    }

}