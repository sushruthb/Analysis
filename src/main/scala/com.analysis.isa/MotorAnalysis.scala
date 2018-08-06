import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
object MotorAnalysis{

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("Motor Analysis").master("local[*]").getOrCreate()

    val data=spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/user/hdfs/motordata.csv")

    data.createTempView("test")

    spark.sql("select motor_id from test where speed ==(select max(speed) from test) ").show()

    //spark.sql("select max(speed) from test").show

  }
}