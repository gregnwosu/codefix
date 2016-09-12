
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_list, concat_ws, col}
import org.apache.spark.mllib.fpm.FPGrowth
//Load data from HDFS

case class Transaction(
  Transaction_ID: String, Dept: String,
  Category: String, Company: String,
  Brand: String, Product_Size: String,
  Product_Measure: String, Purchase_Quantity: String,
  Purchase_Amount: String)

object Transaction {
    def apply(line: String):Transaction = {
      val split = line.split(',')
      Transaction(
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
}


object Main extends App {
  override def main(args: Array[String]) = {
    println (" Running app")
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

  val sc_ = new SparkContext(conf)
  val sc = new org.apache.spark.sql.SQLContext(sc_)
  val data = sc_.textFile(args(0))
  import sc.implicits._
  //Create data schema

  val transCatDataset = data
    .map(Transaction.apply)
    .toDF(
      "Transaction_ID", "Dept", "Category", "Company",
      "Brand", "Product_Size","Product_Measure",
      "Purchase_Quantity", "Purchase_Amount")
    .select("Transaction_ID", "Category").distinct

//  transCatDataset.show

  //Get only categories fields to analyze the relationships between them
  val transGrpAggCategoryConcatString =
    transCatDataset
      .groupBy(collect_list("Transaction_ID"))
      .agg(collect_list(col("Category")) as "Category")
      .withColumn("Category", concat_ws(",", col("Category")))
      .select("Category").distinct

  val flattenedCategories =transGrpAggCategoryConcatString
    .map(_.toString.replace("[", "").replace("]", "")
    )


  val twoElementArrays = flattenedCategories.flatMap(_.combinations(2))
  val result_ = twoElementArrays.union(flattenedCategories.filter (_.length == 1))
  val result = result_.map(_.mkString(","))

  val transactions2: RDD[Array[String]] = data.map(_.trim.split(','))

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
}
