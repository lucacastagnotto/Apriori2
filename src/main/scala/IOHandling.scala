import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

object IOHandling {

  // load dataset

  // clean dataset

  // apriori

  def main(args: Array[String]): Unit = {


    val spark_conf = new SparkConf().setMaster("local[1]").setAppName("open_csv")
    val sc = new SparkContext(spark_conf)

    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    val filePath1="dataset/ratings.csv"

    // load dataset ratings
    var df1 = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(filePath1)

    //df1.show(false)

    // load dataset movies

    val filePath2="dataset/movies.csv"

    var df2 = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("inferSchema", "true")
      .load(filePath2)

    //df2.show(false)

    // check movies

    df1 = df1.drop("timestamp")

    //df1.show(false)

    df1.na.drop()
    df2.na.drop()

    var df_full = df1.join(df2, df1("movieId") === df2("movieId"), "inner").select(df1("*"), df2("title"))
    df_full.dropDuplicates()

    // filter only ratings >= 3.0

    //df_full.show(false)

    // alg apriori

    var new_df = df_full.select("movieId").distinct
    //new_df.show(false)

    val frequent_1 = new_df.rdd.map(x => x)

    var k = 2


  }

  def alg_apriori(df_full: DataFrame) = {
    // Generare L1
    var frequent_1 = List()

    var new_df = df_full.select("movieId").distinct()
    new_df.show(false)


  }
}
