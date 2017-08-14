/**
  * Created by MWARE on 2017-07-13.
  */
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date

import com.scala.loadmark.Loadmarkabc
import elastic.com.load.scanload
import elastic.com.utils.{CS, LOG}
import elastic.math.Position
import elastic.math.samplex.SampleX_math
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import ScalaCallJava.ScalaCallJava.scala2javaDataframe
import java.io.File

object main {
  def NowDate(): String = {
    val now = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(now)
    return date
  }
  def main(args: Array[String]): Unit = {
    //val a = "-96"
    //println("a.toDouble:"+a.toDouble)
    val celldis = "1"
    val icelldis = 1
    val logger = new PrintWriter(new File("F:\\ScalaPrg\\Fast_DingWei\\log_"+NowDate()+".txt" ))
    val ResultCSV = new PrintWriter(new File("F:\\ScalaPrg\\Fast_DingWei\\ResultCSV_"+NowDate()+".txt" ))
    logger.println(NowDate()+":开始初始化")
//    CS.setRoot("F:\\riqq")
    //println(CS.getRoot())
    //基础数据加载
    scanload.load("F:\\riqq\\", icelldis)
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val  sqlContext =  new SQLContext(sc)
    //20170731, 首先测试调用指纹的 class 。
    logger.println(NowDate()+":开始实例化loadABC")
    val loadABC = new Loadmarkabc.LoadMark()
    // 20170731， 在加载csv的时候，可以是单独一个csv文件，也可以是一个路径，textFile方法可以把路径下的所有csv文件
    // 加载进来。
    loadABC.LoadMark(sc ,sqlContext, 0, "F:\\项目汇总\\解析验证\\zhiwen\\block1\\mrgroup1\\Sample_L_zz_2017081101_8957.csv")
    val  results_lgroup  =  sqlContext.sql("SELECT distinct groupid FROM mark_mr ") // where groupid = 02353780000003
    // 循环simple_l ，并将该mr的相关指纹传入到jar包中。
    logger.println(NowDate()+":查询MR：SELECT distinct groupid FROM mark_mr")
    println("result_lgrouplist.count() 一共有多少个 groupid :"+results_lgroup.count())
    logger.println(NowDate()+":results_lgroup.count():"+results_lgroup.count())
    // 加载Simple_X
    val mark_a_path = CS.getRegionDatabaseSampleX + celldis + ".txt"
    loadABC.LoadMark(sc, sqlContext, 1,mark_a_path)
    logger.println(NowDate() + ":加载SAMPLE_X指纹：" + mark_a_path)

    val result_lgrouplist = results_lgroup.collectAsList()
    import scala.collection.JavaConversions._
    var i = 0
    var icount = 1
    try {
      for (rowgroup <- result_lgrouplist) {
        println("icount:"+icount)
        val results_l = sqlContext.sql("SELECT * FROM mark_mr where groupid = " + rowgroup.getString(0))
        logger.println(NowDate() + ":返回一组MR的SQL语句：" + "SELECT * FROM mark_mr where groupid = " + rowgroup.getString(0))
        println("results_l.count():" + results_l.count()) //
        //println("results_l.take(1): "+results_l.take(1).mkString(","))
        // 001, 三套指纹只需要加载一次
        // 1, 加载完results_l以后，调用cenhong提供的方法，返回一个double 类型的数组。搜索samplex的范围
        //val region = new Array[Double](4)
        // 根据返回的数组(两个点的经纬度)，去查询指纹 ，按照 (lon > min and lon < max and lat >min and lat < max )
//        loadABC.LoadMark(sc, sqlContext, 1, "F:\\项目汇总\\解析验证\\zhiwen\\SAMPLE_X\\" + celldis + ".txt")
//        val mark_a_path = CS.getRegionDatabaseSampleX + celldis + ".txt"
//        loadABC.LoadMark(sc, sqlContext, 1,mark_a_path)
//        logger.println(NowDate() + ":加载SAMPLE_X指纹：" + mark_a_path)
        print(mark_a_path)
        val region = SampleX_math.getSearchSampleXRegion(results_l)
        println(region.toString)
        val sqlstr = "SELECT * FROM mark_a where lon > " + region(0).toString+
          " and lon < "+ region(1).toString + " and lat > "+ region(2).toString + " and lat <"+
          region(3).toString
        println("sqlstr: "+sqlstr)
        val results_a = sqlContext.sql(sqlstr)
        println("results_a.count():" + results_a.count()) //
        logger.println(NowDate() + ":SIMPLE_X指纹的记录数:" + results_a.count())
        var cellid = ""
        var a = 1
        /*var results_bmap: Map[String, Map[String, String]] = Map()
      var results_cmap: Map[String, Map[String, String]] = Map()*/
        val results_bmap = new java.util.HashMap[String, java.util.Map[String, String]]()
        val results_cmap = new java.util.HashMap[String, java.util.Map[String, String]]()
        var markbfilename = ""
        var markcfilename = ""
        //2, 加载 cellgrid 的指纹
        // 根据mr中的小区号判断，加载cellgird中的那个小区的指纹
        logger.println(NowDate() + ":根据MR确定要拉取哪些邻区的GRID指纹。")
        println(results_l.collectAsList().get(0))
        while (a < 14) {
//          println(results_a.collectAsList().get(0).getString(a))
          cellid = results_l.collectAsList().get(0).getString(a)
          println("a1:" + a + ":"+ cellid )
          if (cellid != "0.00") {
            cellid = cellid.substring(0, cellid.indexOf("."))
            println("a2:" + a + ":"+ cellid)
//          markbfilename = "F:\\项目汇总\\解析验证\\zhiwen\\CELLGRID\\" + celldis + "\\" + cellid + ".csv"
            markbfilename = CS.getRegionDatabaseCellcellgrid+ celldis + "\\" + cellid + ".csv"
            logger.println(NowDate() + ":加载:" + markbfilename)
            val fileb = new File(markbfilename)
            if (fileb.exists()) {
              logger.println(NowDate() + ":将GRID的指纹放入MAPB中")
              loadABC.LoadMark(sc, sqlContext, 2, markbfilename)
              val results_b = sqlContext.sql("SELECT * FROM mark_b")
              val results_bList = results_b.collectAsList()
              for (brow <- results_bList) {
                //println("cellid"+cellid+ "-> "+ brow)
                results_bmap += (cellid -> Map(brow.getString(0) -> brow.getString(1)))
              }
//            println("results_b.count():" + results_b.count())
            } else {
              println("文件" + markbfilename + "不存在！")
              logger.println(NowDate() + ":文件" + markbfilename + "不存在！")
            }
//            markcfilename = "F:\\项目汇总\\解析验证\\zhiwen\\CELLGRIDBULID\\" + celldis + "\\" + cellid + ".csv"
            markcfilename = CS.getRegionDatabaseCellcellgridbulid+ celldis + "\\" + cellid + ".csv"
            val filec = new File(markcfilename)
            logger.println(NowDate() + ":加载:" + markcfilename)
            if (filec.exists()) {
              logger.println(NowDate() + ":将CELLGRIDBULID的指纹放入MAPC中")
              loadABC.LoadMark(sc, sqlContext, 3, markcfilename)
              val results_c = sqlContext.sql("SELECT * FROM mark_c")
              val results_cList = results_c.collectAsList()
              for (crow <- results_cList) {
                results_cmap += (cellid -> Map(crow.getString(0) -> crow.getString(1)))
              }
            } else {
              println("文件" + markcfilename + "不存在！")
              logger.println(NowDate() + ":文件" + markbfilename + "不存在！")
            }
          }
          a = a + 2
        }
        // 已经准备好了 bulidgrid_map_50， map_grid_50_10 ， 一组MR， mark_a, mark_b， mark_c
        // 等待传递给 定位。
        try {
          val position = new Position()
         for(line <- position.position(rowgroup.getString(0), results_l, results_a, results_bmap, results_cmap, new LOG("F:\\ScalaPrg\\Fast_DingWei\\log\\Dingweilog.txt")))
           {
             //println(line)
             ResultCSV.println(line)
           }
        } finally {
          ResultCSV.close()
        }
        icount = icount + 1
        // bulidgrid_map_50,map_grid_50_10,results_l(dataframe),results_a(dataframe),results_bmap,results_cmap
      }
    }
    finally {
      logger.println(NowDate() + ":执行结束")
      logger.close()
    }
  }
}
