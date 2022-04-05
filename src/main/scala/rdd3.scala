import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

import collection.mutable.ListBuffer
import collection.mutable.Map
//import collection.mutable.Set

object rdd3 {
  def main(args: Array[String]): Unit = {

    // Spark configuration
    val spark_conf = new SparkConf().setMaster("local[*]").setAppName("apriori")
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
    val min_support = 0.12
    val total_support = min_support * totalTransactions

    println("total support: " + total_support)

    val frequent_singleton = transactions.flatMap(transaction => transaction.map(movieId => (movieId, 1))).reduceByKey((x, y) => x + y).filter(x => x._2 >= total_support).map(_._1).collect()

    var frequent_single_items = scala.collection.mutable.Set[Int]()
    var l1 = scala.collection.mutable.Set[List[Int]]()
    for(movieId <- frequent_singleton){
      frequent_single_items += movieId
      val item_as_list = List[Int](movieId)
      l1 += item_as_list
    }

    def candidate_gen(transactions: Iterator[Iterable[Int]], last_frequent_itemset: collection.mutable.Set[List[Int]], all_possible_candidates: Set[Set[Int]], k: Int) : Iterator[(List[Int], Int)] = {
      var transactionSet = new ListBuffer[Set[Int]]()
      for(transaction <- transactions){
        transactionSet += transaction.toSet
      }
      val filteredItemsets = all_possible_candidates.map{
        itemset => (itemset, transactionSet.count(transaction => itemset.subsetOf(transaction)))
      }.filter(x => x._2 > 0).map(x => (x._1.toList, x._2))

      // pruning
      var mutable_filteredItemsets = filteredItemsets.to(collection.mutable.Set)
      for (candidate <- mutable_filteredItemsets) {
        for (my_subset <- candidate._1.toSet.subsets(k - 1)) {
          if (!last_frequent_itemset.contains(my_subset.toList)) {
            mutable_filteredItemsets -= candidate
          }
        }
      }

      mutable_filteredItemsets.iterator
    }

    var last_frequent_itemset = l1
    var k = 2
    while(frequent_single_items.nonEmpty) {
      val all_possible_candidates = frequent_single_items.to(collection.immutable.Set).subsets(k).toSet
      val candidatesPartitions = transactions.mapPartitions(x => candidate_gen(x, last_frequent_itemset, all_possible_candidates, k))

      val frequent_itemsets = candidatesPartitions.reduceByKey((x,y) => x + y).filter(z => z._2 >= total_support)

      if(frequent_itemsets.isEmpty()){
        frequent_single_items.clear()
      }
      else {
        frequent_single_items = frequent_itemsets.map(x => x._1.toSet).reduce((x, y) => x ++ y).to(collection.mutable.Set)
      }
      last_frequent_itemset = frequent_itemsets.map(x => x._1).collect().to(collection.mutable.Set)

      frequent_itemsets.foreach(println)

      k = k + 1

    }


  }
}
