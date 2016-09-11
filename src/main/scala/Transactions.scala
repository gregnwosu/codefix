
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_list, concat_ws, col}
import org.apache.spark.mllib.fpm.FPGrowth
//Load data from HDFS

case class Transactions(
  Transaction_ID: String, Dept: String,
  Category: String, Company: String,
  Brand: String, Product_Size: String,
  Product_Measure: String, Purchase_Quantity: String,
  Purchase_Amount: String)

object Main extends App {
  val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
  val conf = {
    val env = Seq(("spark.files.overwrite" ,  "true"))
    val conf_ = new SparkConf()
    conf_.setAppName("Simple Application")
    conf_.setMaster("local[4]") // 4 threads
    conf_.set( "spark.akka.timeout", "300")
    conf_.set( "spark.broadcast.compress", "true")
    conf_.set( "spark.rdd.compress", "true")
    conf_.set( "spark.io.compression.codec", "snappy")
    conf_.set( "spark.kryoserializer.buffer.max", "256m")
    //Not sure why isn't the default as expected.
    conf_.set( "spark.yarn.executor.memoryOverhead", "3069")
    conf_.setExecutorEnv(env)
  }
  val sc = new SparkContext(conf)
  val data = sc.textFile("data/*")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  //Create data schema

  def csvToMyClass(line: String) = {
    val split = line.split(',')
    Transactions(
      split(0),
      split(1),
      split(2),
      split(3),
      split(4),
      split(5),
      split(6),
      split(7),
      split(8)
    )
  }

  val df = data.map(csvToMyClass).toDF(
    "Transaction_ID", "Dept",
    "Category", "Company",
    "Brand", "Product_Size",
    "Product_Measure", "Purchase_Quantity", "Purchase_Amount")
    .select("Transaction_ID", "Category").distinct()

  df.show

  //Get only categories fields to analyze the relationships between them
  val transactions =
    df.groupBy(collect_list("Transaction_ID"))
      .agg(collect_list(col("Category")) as "Category")
      .withColumn("Category", concat_ws(",", col("Category")))
      .select("Category")
      .rdd.saveAsTextFile("/user/cloudera/Groups_CategoriesASD")

  //Remove
  val input = sc
    .textFile("/user/cloudera/Groups_Categories")
    .flatMap { line => line.split("\n") }
    .distinct()
  input
    .map { vertex =>
      vertex
        .replace("[", "")
        .replace("]", "")
    }
    .saveAsTextFile("/user/cloudera/Groups")

  val input2 = sc.textFile("/user/cloudera/Groups")
  val raw = input2.map(_.split(","))
  val twoElementArrays = raw.flatMap(_.combinations(2))
  val result = twoElementArrays ++ raw.filter(_.length == 1)
  result.map(_.mkString(","))
    .saveAsTextFile("/user/cloudera/Output/Combinations_Output")
   val data2 = sc
    .textFile("/user/cloudera/Output/Combinations_Output")

  val transactions2: RDD[Array[String]] = data.map(s => s.trim.split(','))

  val fpg = new FPGrowth()
    .setMinSupport(0.2)
    .setNumPartitions(10)
  val model = fpg.run(transactions2)

  model.freqItemsets.collect().foreach { itemset =>
    println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
  }

  val minConfidence = 0.8
  model.generateAssociationRules(minConfidence).collect().foreach { rule =>
    println(
      rule.antecedent.mkString("[", ",", "]")
        + " => " + rule.consequent.mkString("[", ",", "]")
        + ", " + rule.confidence
    )
  }
}
