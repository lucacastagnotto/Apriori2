import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

import collection.mutable.ListBuffer
import collection.mutable.Map

object rdd2 {
  def main(args: Array[String]): Unit = {

    // Spark configuration
    val spark_conf = new SparkConf().setMaster("local[4]").setAppName("apriori")
    val sc = new SparkContext(spark_conf)

    // Spark session
    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    // load dataset ratings
    val ratings_path = "dataset/ratings.csv"
    var ds_ratings = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(ratings_path)

    // load dataset movies
    val movies_path = "dataset/movies.csv"
    var ds_movies = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(movies_path)

    // clean data and join
    ds_ratings = ds_ratings.drop("timestamp")
    ds_ratings = ds_ratings.na.drop()
    ds_movies = ds_movies.drop("title", "genres")
    ds_movies = ds_movies.na.drop()
    var ds_full = ds_ratings.join(ds_movies, "movieId")
    ds_full = ds_full.dropDuplicates()

    /*+--------+------+------+
      | movieId|userId|rating|
      +--------+------+------+
      |  110   |  1   |  4.0 |
      +--------+------+------+*/

    // filter only ratings >= 3.0
    ds_full = ds_full.filter(ds_full("rating") >= 3)

    val transactions = ds_full.rdd.map(row => (row(1), row(0))).groupByKey().map(t => t._2.map(_.toString.toInt)).repartition(5)

    val totalTransactions = transactions.count().toInt
    val min_support = 0.3
    val total_support = min_support * totalTransactions

    def getFilteredItemsets(transactions: ListBuffer[Set[Int]], itemsets: Set[Set[Int]], min_support: Double): (Set[(List[Int], Int)], Set[Int]) = {

      val filteredItemsets = itemsets.map{
        itemset => (itemset, transactions.count(transaction => itemset.subsetOf(transaction)))
      }.filter(x => x._2 > 0).map(x => (x._1.toList, x._2))

      var temp_frequent_items : Set[Int] = Set()
      if(!filteredItemsets.isEmpty) {
        temp_frequent_items = filteredItemsets.map(x => x._1.toSet).reduce((x, y) => x ++ y)
      }
      val frequent_items = temp_frequent_items

      (filteredItemsets, frequent_items)
    }

    def apriori(transactionSet: Iterator[Iterable[Int]], min_support: Double, totalTransactions: Int): Iterator[(List[Int], Int)] = {

      var transactions = new ListBuffer[Set[Int]]()
      val singleton_counts = Map[Int, Int]()
      var size = 0
      for(transaction <- transactionSet){
        val items_set = transaction.toSet
        transactions += items_set
        size = size + 1
        for(item <- items_set){
          if(!singleton_counts.contains(item)){
            singleton_counts(item) = 0
          }
          singleton_counts(item) = singleton_counts(item) + 1
        }
      }

      val partition_support = min_support * size

      val frequent_item_counts = singleton_counts.filter( x => x._2 >= partition_support)
      var frequent_items = frequent_item_counts.keys.toList.sorted

      var frequent_itemsets = List[(List[Int], Int)]()
      frequent_itemsets ++= frequent_item_counts.map(x => (List(x._1), x._2))

      var k : Int = 2

      while(frequent_items.nonEmpty){
        val temp_itemsets = frequent_items.toSet.subsets(k)
        val temp_frequent_itemsets = getFilteredItemsets(transactions, temp_itemsets.toSet, partition_support)
        frequent_items = temp_frequent_itemsets._2.toList.sorted
        // temp_frequent_itemsets._1 = { ( [1,2,3], 5 ) , ... , }
        frequent_itemsets ++= temp_frequent_itemsets._1.map(x => x)
        k = k + 1
      }

      frequent_itemsets.iterator
    }

    val partitionResults = transactions.mapPartitions(x => apriori(x, min_support, totalTransactions))

    // Printing

    //println("total frequent itemsets: " + partitionResults.count())
    //partitionResults.foreach(println)

    val grouped = partitionResults.reduceByKey((x,y) => x + y).filter(z => z._2 >= total_support)

    //val myl1 = grouped.filter(x => x._1.size < 2)
    //println("l1 size: " + myl1.count())
    grouped.foreach(println)
    println("# frequent_itemsets: " + grouped.count())

  }
}
