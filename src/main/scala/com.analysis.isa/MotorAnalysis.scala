import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
object MotorAnalysis{

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("Motor Analysis").master("local[*]").getOrCreate()

    val data=spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/user/hdfs/motordata.csv")

  //  data.createTempView("test")

 //   spark.sql("select motor_id from test where speed ==(select max(speed) from test) ").show()
   //spark.sql("select max(speed) from test").show

val table=data.createOrReplaceTempView("TempView")
    val maxSpeed= spark.sql("select motor_id,speed  from TempView where speed = (select max(speed) from TempView)").show()
    val maxTorque=spark.sql("select motor_id,torque from TempView where torque=(select max(torque) from TempView)").show()
    val maxPowerinwatt =spark.sql("select motor_id,powerinwatt from TempView where powerinwatt =(select max(powerinwatt) from TempView)").show()
  
  }
}
