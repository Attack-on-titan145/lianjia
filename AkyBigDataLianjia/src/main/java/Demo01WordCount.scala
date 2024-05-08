import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01WordCount {
  def main(args: Array[String]): Unit = {
    //配置SparkConf：就是配置Spark运行的基础环境，指定主机=Master=本地=local，核数=[2],程序名=AppName
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    //SparkContext必须要依赖于SparkConf所以传入Conf这个参数
    //将上面的配置拿下来传入SparkContext
    val sc:SparkContext = new SparkContext(conf)
    //sc.textFile,Spark中读取文件的方法函数
    val file: RDD[String] = sc.textFile("E:\\IDEA\\AkyBigDataLianjia\\src\\main\\Data\\Count.txt")
    //flatMap中自带了一个split切分的方法，它是每一行进行单独的切分。切分完以后变成你指定的分隔符为切分点，一行一行出现
    val split: RDD[String] = file.flatMap(_.split(","))
    //map中由你自己自定义你的数据的走向，是乘还是除还是加减又或者变成Key,Value使得使用ByKey方法
    val map: RDD[(String,Int)] = split.map((_,1))
    //指定Key不变进行Value的操作可以是加减乘除或者自定义的方法
    val wordcount: RDD[(String, Int)] = map.reduceByKey(_ + _)
    wordcount.foreach(println)
  }
}
