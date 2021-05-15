package com.atguigu.project.app

import com.atguigu.project.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shkstart on 2021/5/15.
  */


object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 把数据从文件读出来
    val sourceRDD = sc.textFile("c:/user_visit_action.txt")
    // 把数据封装好(封装到样例类中)
    val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
      val fields = line.split("_")
      UserVisitAction(
        fields(0),
        fields(1).toLong,
        fields(2),
        fields(3).toLong,
        fields(4),
        fields(5),
        fields(6).toLong,
        fields(7).toLong,
        fields(8),
        fields(9),
        fields(10),
        fields(11),
        fields(12).toLong)
    })

    val categoryTop10 = CategoryTopApp.calcCategoryTop10(sc, userVisitActionRDD)


    categoryTop10.foreach(println)

    // 关闭项目(sc)
    sc.stop()
  }
}

