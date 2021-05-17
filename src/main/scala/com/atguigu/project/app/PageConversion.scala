package com.atguigu.project.app

import java.text.DecimalFormat

import com.atguigu.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by shkstart on 2021/5/17.
  */
object PageConversion {

  def statPageConversionRate(sc: SparkContext,
                             userVisitActionRDD: RDD[UserVisitAction],
                             pageString: String) = {
    // 1. 做出来目标跳转流  1,2,3,4,5,6,7
    val pages = pageString.split(",")
    val prePages = pages.take(pages.length - 1)
    val postPages = pages.takeRight(pages.length - 1)
    val targetPageFlows = prePages.zip(postPages).map {
      case (pre, post) => s"$pre->$post"
    }
    // 1.1 把targetPages做广播变量, 优化性能
    val targetPageFlowsBC = sc.broadcast(targetPageFlows)

    // 2. 计算分母, 计算需要页面的点击量
    val pageAndCount = userVisitActionRDD
      .filter(action => prePages.contains(action.page_id.toString))
      .map(action => (action.page_id, 1))
      .countByKey()
    // 3. 计算分子
    // 3.1 按照sessionId分组. 不能先对需要的页面做过滤, 否则会应用调整的逻辑
    val sessionIdGrouped: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
    val pageFlowsRDD = sessionIdGrouped.flatMap {
      case (sid, actionIt) =>
        // 每个session的行为做一个按照时间排序
        val actions: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time)
        val preActions = actions.take(actions.length - 1)
        val postActions = actions.takeRight(actions.length - 1)
        preActions.zip(postActions).map {
          case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
        }.filter(flow => targetPageFlowsBC.value.contains(flow)) // 使用广播变量
    }

    // 3.2 聚合
    val pageFlowsAndCount = pageFlowsRDD.map((_, 1)).countByKey()
    val f = new DecimalFormat(".00%")
    // 4. 计算调整率
    val result = pageFlowsAndCount.map {
      // map:   pageAndCount 分母
      // 1->2   count/1的点击量
      case (flow, count) =>
        val rate = count.toDouble / pageAndCount(flow.split("->")(0).toLong)
        (flow, f.format(rate))

    }
    println(result)
  }
}
