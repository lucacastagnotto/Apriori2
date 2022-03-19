import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import scala.collection.mutable.Set

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

    //println(transactionSetCollected.getClass)

    // transactionSet.take(20).foreach(println)

    // costants
    val totalTransactions = transactionSet.count()
    var size = 0 // # distinct movies


    // create L1

    //L1: Set[Itemset]

    // Itemset: Tuple(String, Int)
    // Itemset: Tuple(List[String], Int)

    val singleton_counts = Map[String, Int]()

    // transactionSetCollected: Tuple
    // transaction: Tuple(String, Seq)

    for(transaction <- transactionSetCollected){
      val items_set = transaction._2
      //transactions += items_set
      size = size + 1
      for(item <- items_set){
        if(!singleton_counts.contains(item.toString)){
          singleton_counts(item.toString) = 0
        }
        singleton_counts(item.toString) = singleton_counts(item.toString) + 1
      }
    }

    val min_support = 70.toDouble/totalTransactions * size

    val frequent_itemset_1 = Set[String]()
    val frequent_item_counts = singleton_counts.filter( x => x._2 >= min_support)

    // frequent_item_counts.foreach(println)



    // alg apriori

    /*
    var new_df = df_full.select("movieId").distinct
    new_df.show(false)

    val frequent_1 = new_df.rdd.map(x => x)

    var k = 2*/


  }

  def alg_apriori(df_full: DataFrame) = {
    // Generare L1
    var frequent_1 = List()

    var new_df = df_full.select("movieId").distinct()
    new_df.show(false)


  }
}
