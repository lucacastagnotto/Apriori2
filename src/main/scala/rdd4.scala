import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import collection.mutable.ListBuffer

object rdd4 {
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
    val min_support = 0.27
    val total_support = min_support * totalTransactions

    println("total support: " + total_support)

    val frequent_singleton = transactions.flatMap(transaction => transaction.map(movieId => (movieId, 1))).reduceByKey((x, y) => x + y).filter(x => x._2 >= total_support).map(_._1).collect()
    var l1 = ListBuffer[List[Int]]()
    for(movieId <- frequent_singleton){
      val item_as_list = List[Int](movieId)
      l1 += item_as_list
    }

    def filter_candidates(transactions: Iterator[Iterable[Int]], candidates: Set[Set[Int]], k: Int) : Iterator[(List[Int], Int)] = {
      var transactionSet = new ListBuffer[Set[Int]]()
      for(transaction <- transactions){
        transactionSet += transaction.toSet
      }
      val filteredItemsets = candidates.map{
        itemset => (itemset, transactionSet.count(transaction => itemset.subsetOf(transaction)))
      }.filter(x => x._2 > 0).map(x => (x._1.toList, x._2))

      filteredItemsets.iterator
    }

    def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

    var last_frequent_itemset = sort(l1.toList).toList.map(_.toList)

    var frequent_itemsets = List[List[Int]]()
    frequent_itemsets ++= last_frequent_itemset
    //println("l1:\n")
    //last_frequent_itemset.foreach(println)
    var k = 2
    while(last_frequent_itemset.nonEmpty) {
      val candidates = candidates_generation(last_frequent_itemset, k)
      val candidatesPartitions = transactions.mapPartitions(x => filter_candidates(x, candidates, k))
      val new_frequent_itemsets = candidatesPartitions.reduceByKey((x,y) => x + y).filter(z => z._2 >= total_support)

      last_frequent_itemset = sort(new_frequent_itemsets.map(x => x._1.sorted).collect().toList).toList.map(_.toList)

      k = k + 1

      println("last_freq_itemset:\n")
      last_frequent_itemset.foreach(println)

      /*
      println("nuovi frequent itemsets ordinati per support:\n")
      val sorted_freq_it = new_frequent_itemsets.collect().sortBy(_._2)
      sorted_freq_it.foreach(println)

      frequent_itemsets ++= last_frequent_itemset
      frequent_itemsets.foreach(println)
      */
    }

    def candidates_generation(last_frequent_itemset: List[List[Int]], k : Int) : Set[Set[Int]] = {
      val it1 = last_frequent_itemset.iterator
      var candidates = Set[Set[Int]]()
      while (it1.hasNext) {
        val item1 = it1.next()
        val it2 = last_frequent_itemset.iterator
        while (it2.hasNext) {
          val lNext = it2.next()
          if (item1.take(k - 2) == lNext.take(k - 2) && item1 != lNext && item1.last < lNext.last) {
            val l = List(item1 :+ lNext.last)
            val lF = l.flatten
            candidates += lF.toSet
          }
        }
      }
      // pruning
      val last_freq_itemset_Set = last_frequent_itemset.map(_.toSet).toSet
      if(k > 2) {
        for (candidate <- candidates) {
          for (subset <- candidate.subsets(k - 1)) {
            if (!last_freq_itemset_Set.contains(subset)) {
              candidates -= candidate
              println("last_freq_itemset_Set NON contiene " + subset + " che è un sottoinsieme di " + candidate)
            }
            else {
              println("last_freq_itemset_Set CONTIENE " + subset + " che è un sottoinsieme di " + candidate)
            }
          }
        }
      }
      println("candidates:\n")
      candidates.foreach(println)
      candidates
    }
  }
}