package test
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BigDataProj {
  val conf: SparkConf = new SparkConf().setAppName("Final")
  val sc = new SparkContext(conf)

  val roadData: RDD[String] = sc.textFile("hdfs:/final_data/bigdata-input.txt")
  val spliter = roadData.map(line => line.split('\t'))

  def getRecodes(): Unit = { // 1
    val dataRows = roadData.count()
    println(dataRows)
    // 68,993,773
  }

  def getIdCount(): Unit = { // 2
    val idCounter = spliter.map(id => id(0)).distinct().count()
    println(idCounter)
    //4,308,452
  }

  def getArriveCount(): Unit = { // 3
    val arriveCounter = spliter.map(id => id(1)).distinct().count()
    println(arriveCounter)
    //4,489,240
  }

  def getArriveCount(): Unit = { // 4
    val startCounter = spliter.map(id => id(0)).map(start => (start, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect()
    val mostCommon = startCounter(0)._1 //가장 많은 횟수값
    startCounter.takeWhile(_._1 == mostCommon) //takeWhile()을 통해 가장 많은 횟수값과 같은
    //값들이 있는지 확인함.
    //'10009'라는 출발지가 20,293번 어떠한 목적지로 향함.
  }

  def getArriveCount(): Unit = { // 5
    val endCounter = spliter.map(id => id(1)).map(end => (end, 1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect()
    val mostEndCommon = endCounter(0)._1
    endCounter.takeWhile(_._1 == mostEndCommon)
    // '10029'라는 목적지로 도착하는 출발지의 아이디의 갯수는 13,906번임
  }

  def refactoringCounter(): Unit = { // 6
    /*
      1. reduceByKey()를 통해 출발지별 목적지로 향하는 횟수를 구한 뒤 sortByKey를 사용하기 위해 Key와 Value의 swap()과정을 거
          칩니다. → 성능상 문제는 크게 없지만 코드적으로 비효율적인 문제가 생깁니다.
      2. 추후 collect()를 하는 과정에서 Action이 발생하여 성능적으로 문제가 발생합니다.
    */
    //6번 문제 - 1
    val improveStartCounter = spliter.map(id => id(0)).map(start => (start, 1)).reduceByKey(_+_).takeOrdered(roadData.count().toInt)(Ordering[Int].reverse.on(x => x._2))
    val startMost = improveStartCounter(0)._2
    improveStartCounter.takeWhile(_._2 == startMost)

    //6번 문제 - 2
    val improveEndCounter = spliter.map(id => id(1)).map(end => (end, 1)).reduceByKey(_+_).takeOrdered(roadData.count().toInt)(Ordering[Int].reverse.on(x => x._2))
    val endMost = improveEndCounter(0)._2
    improveEndCounter.takeWhile(_._2 == endMost)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BigDataProj")
    val sc = new SparkContext(conf)

    val iters = 3
    val lines = sc.textFile("s3://leeky-us-east-1-share/bigdata-input.txt")
    val roads = lines.map(s => {
      val splited = s.split("\t")
      (splited(0), splited(1))
    })

    val distinctRoads = roads.distinct()
    val groupedRoads = distinctRoads.groupByKey()
    var results = roads.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val startTime = System.currentTimeMillis()
      val weights = groupedRoads.join(results).values.flatMap{ case(a, b) =>
        a.map(x => (x, b / a.size))
      }
      results = weights.reduceByKey((a, b) => a+b).mapValues(y => 0.1 + 0.9 * y)

      val interimResult = results.take(10)
      interimResult.foreach(record => println(record._1 + " : " + record._2))
      val endTime = System.currentTimeMillis()

      println(i + " th interation took " + (endTime - startTime) / 1000 + " seconds")
    }
  }
}
