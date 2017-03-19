package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  def initializeWikipediaRanking(): Boolean =
    try {
      StackOverflow
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import StackOverflow._
    sc.stop()
  }

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def assertEquivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Unit = {
    // (1)
    assert(given.toSet == expected.toSet, "The given elements are not the same as the expected elements")
    // (2)
    assert(
      !(given zip given.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 }),
      "The given elements are not in descending order"
    )
  }

  test("'groupedPostings' group questions and answers") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import StackOverflow._
    val articles = List(
        Posting(1,1,None,None,140,Some("CSS")),
        Posting(2,2,None,Some(1),140,Some("CSS")),
        Posting(2,3,None,Some(1),140,Some("CSS")),
        Posting(1,4,None,None,140,Some("CSS")),
        Posting(2,5,None,Some(4),140,Some("CSS")),
        Posting(2,6,None,Some(4),140,Some("CSS")),
        Posting(2,7,None,Some(4),140,Some("CSS"))
    )
    val rdd = sc.parallelize(articles)
    val grouped = groupedPostings(rdd).collect()
    println(grouped.apply(0)._2.toList)
  }

  test("'scoredPostings' score questions with highest answer") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import StackOverflow._
    val articles = List(
      Posting(1,1,None,None,100,Some("CSS")),
      Posting(2,2,None,Some(1),140,Some("CSS")),
      Posting(2,3,None,Some(1),122,Some("CSS")),
      Posting(1,4,None,None,140,Some("CSS")),
      Posting(2,5,None,Some(4),180,Some("CSS")),
      Posting(2,6,None,Some(4),189,Some("CSS")),
      Posting(2,7,None,Some(4),198,Some("CSS"))
    )
    val rdd = sc.parallelize(articles)
    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped).collect()

    println(scored.apply(0))
    println(scored.apply(1))
  }


}