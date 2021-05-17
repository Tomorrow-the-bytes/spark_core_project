package com.atguigu.project.app

import com.atguigu.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by shkstart on 2021/5/17.
  */
object CategorySessionTopApp {

/*  first */
  def statCategorySessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
    // 1.过滤出来只包含top10品类的那些点击记录
    // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))

    // 2. 每个品类top10session的计算
    // 2.1 先map需要字段
    val cidSidAndOne: RDD[((Long, String), Int)] =
    filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    // 2.2 做聚合操作  得到 RDD[((cid, sid), count))]
    val cidSidAndCount: RDD[((Long, String), Int)] = cidSidAndOne.reduceByKey(_ + _)
    // 2.2 map出来想要的数据结构  RDD[(cid, (sid, count))]
    val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndCount.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
    // 2.3 分组 排序取top10
    val cidAndSidCountItRDD: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()

    // 2.4 对每个value排序取top10
    val result = cidAndSidCountItRDD.mapValues((it: Iterable[(String, Int)]) => {
      // 只能使用scala的排序, scala排序必须把所有数据全部加装内存才能排
      it.toList.sortBy(-_._2).take(10)
    })

    result.collect.foreach(println)
  }



  /* second */
  def statCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
    // 1.过滤出来只包含top10品类的那些点击记录
    // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))

    // 2. 需要排10次
    val temp = cids.map(f = cid => {
      // 2.1 过滤出来点击id是cid的那些记录
      val cidUserVisitActionRDD: RDD[UserVisitAction] = filteredUserVisitActionRDD.filter(_.click_category_id == cid)
      // 2.2 聚合
      val r = cidUserVisitActionRDD
        .map(action => ((action.click_category_id, action.session_id), 1))
        .reduceByKey(_ + _)
        .map {
          case ((cid, sid), count) => (cid, (sid, count))
        }
        .sortBy(-_._2._2)
        .take(10)
        .groupBy(_._1)
        .map {
          case (cid, arr) => (cid, arr.map(_._2).toList)
        }
      r
    })
    val result = temp.flatMap(map => map)
    result.foreach(println)
  }


/* third */
  def statCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
    // 1.过滤出来只包含top10品类的那些点击记录
    // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))

    // 2. 每个品类top10session的计算
    // 2.1 先map需要字段
    val cidSidAndOne: RDD[((Long, String), Int)] =
    filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    // 2.2 做聚合操作  得到 RDD[((cid, sid), count))]
    val cidSidAndCount: RDD[((Long, String), Int)] = cidSidAndOne.reduceByKey(_ + _)
    // 2.2 map出来想要的数据结构  RDD[(cid, (sid, count))]
    val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndCount.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
    // 2.3 分组 排序取top10
    val cidAndSidCountItRDD: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()

    // 2.4 对每个value排序取top10
    val result = cidAndSidCountItRDD.mapValues((it: Iterable[(String, Int)]) => {
      // 不要把Iterable直接转成list再排序.
      var set: mutable.TreeSet[SessionInfo] = mutable.TreeSet[SessionInfo]()
      it.foreach {
        case (sid, count) =>
          val info = SessionInfo(sid, count)
          set += info
          if (set.size > 10) set = set.take(10)
      }
      set.toList
    })

    result.collect.foreach(println)
  }


/*fourth*/
  def statCategorySessionTop10_4(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
    // 1.过滤出来只包含top10品类的那些点击记录
    // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
    val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))

    // 2. 每个品类top10session的计算
    // 2.1 先map需要字段
    val cidSidAndOne: RDD[((Long, String), Int)] =
    filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    // 2.2 做聚合操作  得到 RDD[((cid, sid), count))]
    val cidSidAndCount: RDD[((Long, String), Int)] =
      cidSidAndOne.reduceByKey(new CategorySessionPartitioner(cids), _ + _)

    // 2.3 cidSidAndCount 执行mapPartitions
    val result = cidSidAndCount.mapPartitions((it: Iterator[((Long, String), Int)]) => {
      // 不要把Iterable直接转成list再排序.
      var set: mutable.TreeSet[SessionInfo] = mutable.TreeSet[SessionInfo]()
      var categoryId = -1L
      it.foreach {
        case ((cid, sid), count) =>
          categoryId = cid
          val info: SessionInfo = SessionInfo(sid, count)
          set += info
          if (set.size > 10) set = set.take(10)

      }
      //            set.map((categoryId, _)).toIterator
      Iterator((categoryId, set.toList))
    })
    result.collect.foreach(println)


  }
}

class CategorySessionPartitioner(cids: Array[Long]) extends Partitioner {
  private val cidIndexMap: Map[Long, Int] = cids.zipWithIndex.toMap

  // 分区和品类id数量保持一致, 可以保证一个分区只有一个cid
  override def numPartitions: Int = cids.length

  //(Long, String)  => (cid, sessionId)
  override def getPartition(key: Any): Int = key match {
    // 使用这个cid在数组中的下标作为分区的索引非常合适
    case (cid: Long, _) => cidIndexMap(cid)
  }

}
