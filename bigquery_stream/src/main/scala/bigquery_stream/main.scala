package bigquery_stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.google.cloud.spark.bigquery._
import scala.collection.mutable
import scala.reflect.ClassTag

object main extends App {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("main")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val spark: SparkSession = SparkSession.builder().config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1").getOrCreate
  spark.conf.set("credentialsFile", "src/main/resources/bigquery_stream/credentials2.json")

  import spark.implicits._

  val baseQuery: String = "bigquery-public-data:stackoverflow."
  val resourcePath: String = "src/main/resources/bigquery_stream/"
  val Q: DataFrame = spark.read.bigquery(baseQuery + "posts_questions")
    .select($"id", $"accepted_answer_id", $"owner_user_id")
  val A: DataFrame = spark.read.bigquery(baseQuery + "posts_answers") select($"id", $"owner_user_id")

  val QandA: DataFrame = Q.as("Q")
    .join(A.as("A"), Q("accepted_answer_id") === A("id"))
    .select(
      col("Q.owner_user_id").as("questioner_id"),
      col("A.owner_user_id").as("answerer_id")
      )
    .where(col("Q.owner_user_id").isNotNull && col("A.owner_user_id").isNotNull)

  case class QuestionAnswer(question: String, answer: String)

  val QandAGroup: RDD[(String, (Iterable[String], Double))] = QandA.rdd.map {
    x => QuestionAnswer(x(0).toString, x(1).toString)
  }
    .map { x => (x.question, x.answer) }
    .aggregateByKey(new mutable.HashSet[String])(_ + _, _ ++ _)
    .mapValues(x => (x.toIterable,1.0))
    .cache()
  println("total of == " + QandAGroup.count)
  println(" start page rank ")
  val result = pageRank(QandAGroup).toDF("result_id", "rank").limit(1000)//.orderBy(col("rank").desc).limit(1000)
  //val resultDF = result.toDF("result_id", "rank").orderBy(col("rank").desc).limit(1000)
  result.show(10)
  print(" done with page rank ")
  val users = spark.read.bigquery(baseQuery + "users").select(
    $"id",
    $"display_name",
    $"reputation",
    $"up_votes",
    $"down_votes"
    ).where($"id".isNotNull)
  println("load users ")

  val usersWithRank = result.as("result")
    .join(users.as("users"),
          result("result_id") === users("id")
          )
    .select("result.rank",
      "result.result_id",
            "users.display_name",
            "users.reputation",
            "users.up_votes",
            "users.down_votes"
            ).orderBy(col("result.rank").desc)
  println(" join users ")
  usersWithRank.show(10)


  //Util.writeToFile(usersWithRank, resourcePath + "pageRank")

  def pageRank[T: ClassTag](df: RDD[(T, (Iterable[T], Double))]): RDD[(T, (Iterable[T], Double))] = {

//    var ranks: RDD[(T, Double)] = df.mapValues(_ => 1.0)
    val header: RDD[(T, Iterable[T])] = df.map(x => (x._1, x._2._1))
    //val ranks: RDD[(T, Double)] = df.map(x => (x._1,x._2._2))
    val values: RDD[(Iterable[T], Double)] = df.values
    val contribs: RDD[(T, Double)] = values.flatMap{ case (list, rank) =>
      val size = list.size
      list.map(id => (id, rank / size))
    }
    val ranks2: RDD[(T, Double)] =  contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    val result: RDD[(T, (Iterable[T], Double))] = header.join(ranks2)


//    for (i <- 1 to iter) {
//      println("---iteration = "+ i + " ---")
//      val contribs = df.join(ranks).values.flatMap { case (list, rank) =>
//        val size = list.size
//        list.map(id => (id, rank / size))
//      }
//      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
//    }
//    ranks.sortBy(_._2)
    result.sortBy(_._2._2)
  }
  def convergence[T:ClassTag](df:RDD[(T, (Iterable[T], Double))], iter:Int, threshold:Int):Int = {
    val init: RDD[(T,(Iterable[T], Double))] = pageRank(df)
    val check: RDD[(T,(Iterable[T], Double))] = pageRank(init)

    val score: RDD[Int] = init.zip(check).map{
      item:((T,Double),(T,Double)) => item._1._1 == item._2._1
    }.map{
      if(_) 0 else 1}
    val sumScore: Int = score.sum().toInt

    if (sumScore > threshold){

    }
    else iter
  }

  sc.stop
  spark.close
}