package clickhouse_importer

import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.sql.ResultSet

import org.apache.spark.sql.{Dataset, Row}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl, ClickHousePreparedStatement}

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray
import scala.util.matching.Regex

import java.util.ArrayList

class Clickhouse extends Serializable {

    val bulkSize = 20000

    var jdbcLink: String = "jdbc:clickhouse://%s:%d/%s"
    val host = "ckpub15.olap.jd.com"
    val port = 2000
    
    val username: String = "graphcompute_db_user"
    val password: String = "YgUEfO6kBED0sHd5TFhP"
    val properties: Properties = new Properties()

    var tableSchema: Map[String, String] = new HashMap[String, String]()
    var insertFields: java.util.List[String] = new ArrayList[String]
    
    var database = ""
    var writeTableName = ""

    val arrayPattern: Regex = "(Array.*)".r

    def log(line: String) {
	  println("|============================================|" + line)
	}

    def setInitParame(database:String, writeTableName:String, fieldDefine:String): Unit = {
        this.database = database
        this.writeTableName = writeTableName

        this.jdbcLink = jdbcLink.format(this.host, this.port, this.database)
        this.properties.put("user", this.username)
        this.properties.put("password", this.password)

        var fieldName=""
        val arrFieldDefine = fieldDefine.split(",");
        for( fd <- arrFieldDefine )
        {
            val arrFD = fd.trim().split(" ");
            if(arrFD.length == 2)
            {
                fieldName = arrFD(0).toString().trim()
                insertFields.add(fieldName)
                tableSchema += (fieldName -> arrFD(1).toString().trim())
            }
        }
    }

    def process(df: Dataset[Row]): Unit = {
        val dfFields = df.schema.fieldNames

        df.foreachPartition { iter =>
            val executorBalanced = new BalancedClickhouseDataSource(this.jdbcLink, this.properties)
            val dbConnect = executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
            val sqlstr = initPrepareSQL(database, writeTableName, insertFields)
            val statement = dbConnect.createClickHousePreparedStatement(sqlstr)

            var length = 0
            while (iter.hasNext) {
                val item: Row = iter.next()
                length += 1
                renderStatement(item, dfFields, statement)
                statement.addBatch()

                if (length >= bulkSize) {
                try {
                    statement.executeBatch()
                } catch {
                    case e : Throwable => log("Execute Exp=" + e)
                }
                    length = 0
                }
            }

            // 如果剩下的数据小于 bulkSize，则单独提交执行
            if(length > 0)
            {
                try {
                    statement.executeBatch()
                } catch {
                    case e : Throwable => log("Execute Exp=" + e)
                }
            }

            try {  
                statement.close()
            } catch {
                case e : Throwable => log("Close database Exp=" + e)
            }
            try	{
                if(dbConnect != null) dbConnect.close();
            }catch {
                case e : Throwable => log("Close dbconnection Exp=" + e)
            }
        }
    }

    private def getClickHouseSchema(conn: ClickHouseConnectionImpl, table: String): Map[String, String] = {
        val sql = String.format("desc %s", table)
        val resultSet = conn.createStatement.executeQuery(sql)
        var schema = new HashMap[String, String]()
        while (resultSet.next()) {
            schema += (resultSet.getString(1) -> resultSet.getString(2))
        }
        schema
    }

    private def initPrepareSQL(database:String, tableName: String, insertFields: java.util.List[String]): String = {
        val prepare = List.fill(insertFields.size)("?")
        val sql = String.format(
        "insert into %s.%s (%s) values (%s)",
        database,
        tableName,
        insertFields.mkString(","),
        prepare.mkString(","))
        sql
    }

    private def renderStringDefault(fieldType: String): String = {
        fieldType match {
        case "DateTime" =>
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            dateFormat.format(System.currentTimeMillis())
        case "Date" =>
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            dateFormat.format(System.currentTimeMillis())
        case "String" =>
            ""
        }
    }

    private def renderDefaultStatement(index: Int, fieldType: String, statement: ClickHousePreparedStatement): Unit = {
        fieldType match {
        case "DateTime" | "Date" | "String" =>
            statement.setString(index + 1, renderStringDefault(fieldType))
        case "Int8" | "UInt8" | "Int16" | "Int32" | "UInt32" | "UInt16" =>
            statement.setInt(index + 1, 0)
        case "UInt64" | "Int64" =>
            statement.setLong(index + 1, 0)
        case "Float32" => statement.setFloat(index + 1, 0)
        case "Float64" => statement.setDouble(index + 1, 0)
        case arrayPattern(_) => statement.setArray(index + 1, List())
        case _ => statement.setString(index + 1, "")
        }
    }

    private def renderStatement(
        item: Row,
        dsFields: Array[String],
        statement: ClickHousePreparedStatement): Unit = {
        for (i <- 0 until insertFields.size()) {
        val field = insertFields.get(i)
        val fieldType = tableSchema(field)
        if (dsFields.indexOf(field) == -1) {
            renderDefaultStatement(i, fieldType, statement)
        } else {
            fieldType match {
            case "Map" =>
                statement.setString(i + 1, hashMapToJson(item.getAs[HashMap[String, Any]](field)))
            case "DateTime" | "Date" | "String" =>
                statement.setString(i + 1, item.getAs[String](field))
            case "Int8" | "UInt8" | "Int16" | "UInt16" | "Int32" =>
                statement.setInt(i + 1, item.getAs[Int](field))
            case "UInt32" | "UInt64" | "Int64" =>
                statement.setLong(i + 1, item.getAs[Long](field))
            case "Float32" => statement.setFloat(i + 1, item.getAs[Float](field))
            case "Float64" => statement.setDouble(i + 1, item.getAs[Double](field))
            case arrayPattern(_) =>
                statement.setArray(i + 1, item.getAs[WrappedArray[AnyRef]](field))
            case _ => statement.setString(i + 1, item.getAs[String](field))
            }
        }
        }
    }

    private def hashMapToJson(hashMapObj: HashMap[String, Any]): String = {
        if(hashMapObj != null && hashMapObj.size() > 0)
        {
        var arrMap = new ArrayList[String]
        for((key:String, value:Any) <- hashMapObj)
        {
            value match {
            case value:Int    => arrMap.add("\"" + key + "\":" + value)
            case value:Long   => arrMap.add("\"" + key + "\":" + value)
            case value:Float  => arrMap.add("\"" + key + "\":" + value)
            case value:Double => arrMap.add("\"" + key + "\":" + value)
            case _ => arrMap.add("\"" + key + "\":\"" + value + "\"")
            }
        }
        "{" +  arrMap.mkString(",") + "}"
        }
        else "NULL"
    }

}
