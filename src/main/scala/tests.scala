import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import collection.mutable.ListBuffer
import collection.mutable.Map
import scala.util.control.Breaks

object apriori_test {

  def main(args: Array[String]): Unit = {

    // Spark configuration
    val spark_conf = new SparkConf().setMaster("local[1]").setAppName("open_csv_test")
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
    var size = 0 // # distinct movies
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

    val min_support = 70.toDouble/totalTransactions * size

    val frequent_item_counts = singleton_counts.filter( x => x._2 >= min_support)
    // Valid itemset
    var frequent_items = frequent_item_counts.keys.toList.sorted // Why sorted?

    var frequent_itemsets: List[List[Int]] = List()
    // Add L1-itemsets to frequent_itemsets
    frequent_itemsets ++= frequent_item_counts.map(x => List(x._1))

    var k : Int = 2 // starting to generate from pairs

    def getFilteredItemsets(transactions: ListBuffer[Set[Int]], itemsets: Set[Set[Int]], min_support: Double): (Set[List[Int]], Set[Int]) = {
      // itemsets = {{1,2,3}, {1,2,4}, ... , {2,3,4}}
      val outloop = new Breaks;
      val inloop = new Breaks;

      println("starting itemsets size: " + itemsets.size)
      var pruned = 0

      var mutable_itemsets = itemsets.to(collection.mutable.Set)
      //var candidates_after_pruning : Set[Set[Int]] = Set()
      outloop.breakable {
        for (candidate <- itemsets) {
          inloop.breakable {
            for (my_subset <- candidate.subsets(k - 1)) {
              if (!frequent_itemsets.contains(my_subset.toList)) {
                mutable_itemsets -= candidate
                pruned += 1
                inloop.break
              }
            }
          }
        }
      }

      println("candidates eliminated after pruning: " + pruned)
      println("itemsets size after pruning: " + mutable_itemsets.size)

      // Test AREA
      /*var prunedItemsets = itemsets.map{
        itemset =>
          (
            itemset.toList,
            itemset.subsets(k - 1).map(
              y => y.toList
            ).toList
          )
      }*/
      //                                                      Set --------------
      //                                                                         \
      //                                                                        Tuple
      //                                                                   /          \
      //                                                           List[Int]          List
      //                                                              |          /      |      \
      //                                                              |   List[Int] List[Int] List[Int]  (one foreach subset)
      //                                                              |
      // prunedItemsets = {([1,2,3], [[1,2], [1,3], [2,3]]), ... , ([2,3,4], [[2,3], [3,4], [2,4]])} : Set[Tuple(List[Int], List[List[Int]])]

      /*println("prunedItemset tot: " + prunedItemsets.size)
      for(candidate <- prunedItemsets){
        var flag = false
        for(subset <- candidate._2){
          if(!frequent_itemsets.contains(subset)) flag = true
        }
        if(flag) prunedItemsets -= candidate
      }

      println("prunedItemset after pruning: " + prunedItemsets.size)

      val new_itemsets = prunedItemsets.map(x => x._1.toSet)

      new_itemsets.take(20).foreach(println)*/


      // itemsets -= pruned_candidates

      val filteredItemsets = mutable_itemsets.map{
        itemset => (
          itemset,
          transactions.count(transaction => itemset.subsetOf(transaction)) // count how many times the itemset appears in transactions
        )}. // foreach itemset generate the pair (itemset, #_occurences)
        // filteredItemsets = { ({1,2,3}, 120), ({1,2,4}, 80), ... , ({2,3,4}, 70) } : Set[Tuple(Set[Int], Int)]
        filter(x => x._2 > min_support). // foreach pair filter out only those ones with itemset with #_occurences >= min_support
        map(x => x._1.toList) // transform each itemset in a List and each pair in that List (discard #_occurences)
      // filteredItemsets = {[1,2,3], [1,2,4], ... , [2,3,4]} : Set[List[Int]]


      println("itemsets size after counting occurences: " + filteredItemsets.size)

      var frequent_items : Set[Int] = Set()

      if(!filteredItemsets.isEmpty){
        frequent_items = filteredItemsets.map(x => x.toSet).reduce((x, y) => x ++ y) // generate the Set with every element in filteredItemsets // Handle case empty list (Empty.reduce)
      }

      (filteredItemsets.to(collection.immutable.Set), frequent_items.to(collection.immutable.Set))
    }

    //Generate all other size itemsets. Loop runs till no new candidates are generated.
    while(frequent_items.nonEmpty){
      // Candidate generation
      val candidates = frequent_items.toSet.subsets(k)
      val temp_frequent_itemsets = getFilteredItemsets(transactions, candidates.toSet, min_support)
      frequent_items = temp_frequent_itemsets._2.toList.sorted // Why do we convert into List? It can remain Set as we need it as Set for candidates generation, 2 lines above
      frequent_itemsets ++= temp_frequent_itemsets._1.toList.map(x => x.sorted)
      k = k + 1
    }
  }
}



