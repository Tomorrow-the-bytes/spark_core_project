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
    /**
      * @note The first requirement: Top10热门品类
      */
    val categoryTop10 = CategoryTopApp.calcCategoryTop10(sc, userVisitActionRDD)
//    categoryTop10.foreach(println)



    /**
      * @note The second requirement: Top10热门品类中每个品类的 Top10 活跃 Session 统计
      *    @note The first solution   (对每个value排序取top10，需要对Iterable进行tolist，可能内存溢出)
      */

//    CategorySessionTopApp.statCategorySessionTop10(sc,categoryTop10, userVisitActionRDD)



    /**
      * @note The second requirement: Top10热门品类中每个品类的 Top10 活跃 Session 统计
      *    @note The second solution   （每次搞定一个cid,可以使用spark的排序功能,但是需要启用多个job）
      */

//      CategorySessionTopApp.statCategorySessionTop10_2(sc,categoryTop10,userVisitActionRDD)


    /**
      * @note The second requirement: Top10热门品类中每个品类的 Top10 活跃 Session 统计
      *    @note The third solution  (利用treemap容器保持里面只前10个降序的元素，这样就可以保证内存不溢出，但是需要2次shuffle)
      *          @note set去重，注意设置相等的时候的条件
      */
//      CategorySessionTopApp.statCategorySessionTop10_3(sc,categoryTop10,userVisitActionRDD)

    /**
      * @note The second requirement: Top10热门品类中每个品类的 Top10 活跃 Session 统计
      *    @note The fourth solution  (对第三种解法进行优化，减少一次shuffle)
      *
      */

      CategorySessionTopApp.statCategorySessionTop10_4(sc,categoryTop10,userVisitActionRDD)







    /**
      * @note 关闭项目(sc)
      */
    sc.stop()
  }
}

