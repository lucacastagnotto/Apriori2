import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable //.{ListBuffer, Map, Set}
import collection.mutable.ListBuffer
import collection.mutable.Map

object IOHandling {

  // load dataset

  // clean dataset

  // apriori

  def main(args: Array[String]): Unit = {

    // Spark configuration
    val spark_conf = new SparkConf().setMaster("local[1]").setAppName("open_csv")
    val sc = new SparkContext(spark_conf)

    // Spark session
    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    val ratings_path="dataset/ratings.csv"

    // load dataset ratings
    var df_ratings = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(ratings_path)

    // load dataset movies
    val movies_path="dataset/movies.csv"

    var df_movies = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(movies_path)

    // clean data and join
    df_ratings = df_ratings.drop("timestamp")
    df_ratings = df_ratings.na.drop()
    df_movies = df_movies.na.drop()

    var df_full = df_ratings.join(df_movies, df_ratings("movieId") === df_movies("movieId"), "inner").select(df_ratings("*"), df_movies("title"))
    df_full =  df_full.dropDuplicates()

    // filter only ratings >= 3.0; Columns = 0: userId, 1: movieId, 2: rating, 3: title
    df_full = df_full.filter(df_full("rating") >= 3)

    // create transactionSet
    val transactionSet =
      df_full.rdd.
      map(x => (x(0), x(1))). // Pair RDD
      groupByKey()

    val transactionSetCollected = transactionSet.collect()

    // costants and variables
    val totalTransactions = transactionSet.count()
    var size = 0
    val singleton_counts = Map[Int, Int]()
    var transactions : ListBuffer[Set[Int]] = ListBuffer()

    // create L1
    for(transaction <- transactionSetCollected){
      // transaction = (273, Seq(1, ... , 30492) : (Any, Iterable[Any])
      var item_set = Set[Int]()
      transaction._2.foreach(movieId => {
        val movieId_int = movieId.toString.toInt
        item_set += movieId_int
      })
      transactions += item_set
      size = size + 1
      for(item <- item_set){
        if(!singleton_counts.contains(item)){
          singleton_counts(item) = 0
        }
        singleton_counts(item) = singleton_counts(item) + 1
      }
    }

    //val min_support = 70.toDouble/totalTransactions * size
    val min_support = 0.2 * totalTransactions

    val frequent_item_counts = singleton_counts.filter( x => x._2 >= min_support)
    // Valid itemset
    var frequent_items = frequent_item_counts.keys.toList.sorted // Why sorted?

    //frequent_items.foreach(println)

    //var frequent_itemsets: List[List[Int]] = List()
    var frequent_itemsets: List[(List[Int], Int)] = List()
    // Add L1-itemsets to frequent_itemsets
    frequent_itemsets ++= frequent_item_counts.map(x => (List(x._1), x._2))

    //println("l1 generated: " + frequent_itemsets.size)

    var k : Int = 2 // starting to generate from pairs

    def getFilteredItemsets(transactions: ListBuffer[Set[Int]], itemsets: Set[Set[Int]], min_support: Double): (Set[(List[Int], Int)], Set[Int]) = {
      // itemsets -= pruned_candidates
      val filteredItemsets = itemsets.map{
        itemset => (itemset, transactions.count(transaction => itemset.subsetOf(transaction)))
      }.filter(x => x._2 >= min_support).map(x => (x._1.toList, x._2))

      var frequent_items : Set[Int] = Set()
      if(!filteredItemsets.isEmpty){
        frequent_items = filteredItemsets.map(x => x._1.toSet).reduce((x, y) => x ++ y) // generate the Set with every element in filteredItemsets // Handle case empty list (Empty.reduce)
      }

      (filteredItemsets, frequent_items.to(collection.immutable.Set))
    }

    //Generate all other size itemsets. Loop runs till no new candidates are generated.
    while(frequent_items.nonEmpty){
      // Candidate generation
      val candidates = frequent_items.toSet.subsets(k)
      val temp_frequent_itemsets = getFilteredItemsets(transactions, candidates.toSet, min_support)
      frequent_items = temp_frequent_itemsets._2.toList.sorted
      frequent_itemsets ++= temp_frequent_itemsets._1.toList.map(x => x)
      k = k + 1
    }

    println("# frequent_itemsets: " + frequent_itemsets.size)
    frequent_itemsets.foreach(println)

  }
}
