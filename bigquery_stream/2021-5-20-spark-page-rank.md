---
layout: post
title: PageRank analysis from Spark Streaming from BigQuery
---

<br>
![Logos]({{ site.baseurl }}/images/april-fool.png "stackoverflow")
<p align="center">
    <font size="4"> </font>
</p>
<br>
<br>

* TOC
{:toc}
# Preamble
During the COVID19 lockdown, I decided to create learning schedules on a few topics that are accessible online. I wrote about NLP deep learning, and Bayesian programming in previous blogs already. This blog is about one of my learning projects on Scala-Spark. While there are many projects that I studied on different aspects of scala and sparks, this problem one seems to have a balance of skills and use case possibility. The full project can be found on [Github](https://github.com/VincentK1991/spark_tutorials/tree/main/bigquery_stream).

# Problem statement
Stackoverflow is a question/answer forum for programming enthusiasts and professionals. To motivate good behaviors and answer quality, users can earn reputation points from receiving "up" vote from questions or answers. Privileges are awarded for high reputation users. 

There are many other ways to rank user contributions. One possibility imagines user iteractions as directed graphs, where the users are nodes and the question and answers between 2 users are the directed edges. For the purpose of finding the best contributors to answering questions, the directed edges are pointing from questioners to answerers. Moreover, not all answers are created equal. Some answers can be an "accepted answer", meaning the the questioner has endorsed the answer. So we can focus on a subset of network to just the question and accepted answer pairs.

I will explore using Page Rank algorithm to rank user contribution and will see whether the result from Page Rank  is correlated with the reputation score. 

# Stackoverflow data

The stackoverflow questions and answers are id-ed and are publicly available on Google Cloud Big Query at practically zero cost. To get access, you'd need to have google cloud account. Go to BigQuery pannel and find the table `bigquery-public-data:stackoverflow.posts_questions`. This is the table that contains questions posted on Stackoverflow from November 2016 to the present. As of May 2021, we have about 20M rows. There are a few ways we can work with the data, we can either download the table out to csv file, or we can connect our spark application to bigquery and read the data through the network connection. I will choose the second option. But to just visualize the data first, we can query out a few rows. BigQuery support SQL-like query commands. So, in the editor, type 

```
SELECT *
FROM `bigquery-public-data.stackoverflow.posts_questions`
LIMIT 10
```

This will print out 10 rows from 20M rows. We can save the results in csv files. But this is no need because we will connect to the table through spark anyway.

The column that we would need from this table would be the id (which is the id of the question), accepted_answer_id (id of the accepted answer associated with this question), and the owner_user_id (the id of the questioner).

Another table that we would need would be the posts_answers table. The column that we need are the id (this is the id of the answer), and the ownder_user_id (user id of the answerer).

Finally, we would need the users table to get information about the users. the rows that we need are id, display_name, reputation, up_votes, and down_votes. 

we will do the joining and aggregation all in spark.

# Spark

To set up the spark application, see appendix 1. You'd need to have IntelliJ and Java-8. Also, to connect the spark application to BigQuery, you need to have a credential. See appendix 2.

First, these are all the imports that we will need.

<details>
<summary>
<i>import </i>
</summary>
<p>
{% highlight scala %}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.google.cloud.spark.bigquery._
import scala.collection.mutable
import scala.reflect.ClassTag

{% endhighlight %}  
</p>
</details>

We will need boiler plate codes to set up the spark application. In my case, I will set it up locally. we have to configure the sparksession for BigQuery connection. This is done by adding the Jar file and the dependencies to the spark session. Also, connecting to the BiqQuery requires credential file (which is the json file) containing the hash codes. So make sure you have the credential file path for that json file.

<details>
<summary>
<i>boiler plate </i>
</summary>
<p>
{% highlight scala %}

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("main")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val spark = SparkSession.builder().config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1").getOrCreate
  spark.conf.set("credentialsFile", "src/main/resources/bigquery_stream/credentials2.json")

  import spark.implicits._

{% endhighlight %}  
</p>
</details>

after setting this up, we can read from bigquery. The read.bigquery function is a factory method that generates Spark DataFrame of the bigquery table. We can use Select to select out the columns that we will need. Do this for questions and answers. Finally, we will need to join the question DataFrame with the answer DataFrame. The joining condition is that the accepted_answer_id on the question table is the id of the answer in the answer table. 

Then what we need are all the questioner ids and the answerer ids. these are pairs of id of person who ask the questions, and the id of persons who get endorsed for having answer the best answer. 

<details>
<summary>
<i>boiler plate </i>
</summary>
<p>
{% highlight scala %}
  
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

{% endhighlight %}  
</p>
</details>

After getting the DataFrame of the questioner and answerer. we will aggregate the table by the questioner id. The idea is that we want a list of unique questioners and the list of answerers whom each questioners have endorsed. This is parallel to the original page rank use case where we have each webpage points to multiple other webpages through hyperlinks.

# PageRank algorithm

This algorithm is used to find out the most important node in the network by initially distributing equal weights of 1.0 to all nodes. The weights are then divided by number of neighbors and are distributed to the neighboring nodes. This procedure is repeated until convergence point is achieved.

The intuition for distribution being normalized by number of neighbors is that if a particular node has multiple out-going links to many nodes. It will distribute its weights to these many neighboring nodes. Having many neighbors means the denominator for the weight is large. This prevents a contribution from a scenario where one node point to many nodes non-specifically.

The goal is that the node that has many incoming nodes will received greater weights. 
The function below is a simple implementation of the algorithm in Spark-Scala. We can write a generic function of type T that will perform map-reduce tasks iteratively. For our purpose the type T is String. 

<details>
<summary>
<i> iterative page rank algorithm </i>
</summary>
<p>
{% highlight scala %}

  def iterativePageRank[T: ClassTag](df: RDD[(T, Iterable[T])], iter: Int): RDD[(T, Double)] = {
    var ranks: RDD[(T, Double)] = df.mapValues(_ => 1.0)
    for (_ <- 1 to iter) {
      val contribs = df.join(ranks).values.flatMap { case (list, rank) =>
        val size = list.size
        list.map(id => (id, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks
  }

{% endhighlight %}  
</p>
</details>

We can also implement a page rank algorithm that runs recursively until convergence. 

<details>
<summary>
<i> recursive page rank algorithm </i>
</summary>
<p>
{% highlight scala %}

  def pageRank[T: ClassTag](df: RDD[(T, (Iterable[T], Double))]): RDD[(T, (Iterable[T], Double))] = {

    val header: RDD[(T, Iterable[T])] = df.map(x => (x._1, x._2._1))
    val values: RDD[(Iterable[T], Double)] = df.values
    val contribs: RDD[(T, Double)] = values.flatMap{ case (list, rank) =>
      val size = list.size
      list.map(id => (id, rank / size))
    }
    val ranks2: RDD[(T, Double)] =  contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    val result: RDD[(T, (Iterable[T], Double))] = header.join(ranks2)
    result.sortBy(_._2._2)
  }

  @tailrec
  def convergence[T:ClassTag](df:RDD[(T, (Iterable[T], Double))], threshold:Int, iter:Int = 1): (RDD[(T,(Iterable[T], Double))], Int) = {
    val init: RDD[(T,(Iterable[T], Double))] = pageRank(df)
    val subsequent: RDD[(T,(Iterable[T], Double))] = pageRank(init)

    val init_rank: RDD[T] = init.map(x => x._1)
    val subsequent_rank: RDD[T] = subsequent.map(x => x._1)

    val score: RDD[Int] = init_rank.zipShuffle(subsequent_rank).map{
      item:(T,T) => item._1 == item._2
    }.map{
      if(_) 0 else 1}
    val sumScore: Int = score.sum().toInt

    if (iter > 100){
      println("-- running for too long --")
      (subsequent,iter)
    }
    else if (sumScore > threshold){
      println(" -- not converge yet -- ")
      println(" -- run iteration " + iter + 2)
      convergence(subsequent, threshold, iter + 2)
    }
    else (subsequent,iter)
  }

    implicit class RichContext[T](rdd: RDD[T]) {
    def zipShuffle[A](other: RDD[A])(implicit kt: ClassTag[T], vt: ClassTag[A]): RDD[(T, A)] = {
      val otherKeyd: RDD[(Long, A)] = other.zipWithIndex().map { case (n, i) => i -> n }
      val thisKeyed: RDD[(Long, T)] = rdd.zipWithIndex().map { case (n, i) => i -> n }
      val joined = thisKeyed.join(otherKeyd).map(_._2)
      joined
    }
  }

{% endhighlight %}  
</p>
</details>

In this version, we recursively perform page rank until convergence. We define a convergence criterion to be how many rank changes from one iteration to the next. If the rank changes are below certain threshold, we decide that the algorithm has converged.

# Results

Finally, after we run the page rank what we get would be the ranking scores. To compare against other scoring methods, we can use dataframe join to join the user information such as reputation, up votes, and down votes. In this case, I decide to run the page rank algorithm for 5 iterations and take the top 1000 ranks to save as csv file for analysis. 

I found that the page rank result correlates somewhat with the reputation score correlation coefficient of 0.45; and much less correlated on up vote (0.17) or down vote (0.23).

# Discussion

The page rank algorithm is a first step for us to find out the relative importance of users just by looking at the question-answer records. This information can be useful for monitoring the user behaviors as well as content quality on the website. I could also be used when we want to suggest top user answers or prioritize which answers to show on a page. The scenario where the page rank algorithm may be preferred over the "reputation score" is when the reputation scoring becomes unreliable. This can happen  few users fill out the reputation score survey or few people use up vote or down vote features.


# Appendix

1. set up requirement

The set up that I have here is useful for beginner who wants to try spark locally or for debugging purposes. While there are many ways to set up spark locally, I use IntelliJ IDEA to set up the spark project written in Scala. This [blog](https://medium.com/@Sushil_Kumar/setting-up-spark-with-scala-development-environment-using-intellij-idea-b22644f73ef1) provides useful set up instruction. The sbt library dependencies can be found on my [github]().


2. Connect Google-BigQuery to Spark
To connect spark to Bigquery, you need to have a credential. refere [here](https://cloud.google.com/bigquery/docs/authentication/service-account-file) on how to obtain credentials for your project.

