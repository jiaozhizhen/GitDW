package com.scala.loadmark

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.io.Source

/**
  * Created by MWARE on 2017-07-18.
  * val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val  sqlContext =  new SQLContext(sc)
  */
object Loadmarkabc extends App{
  class LoadMark{

    def LoadMark(sc: SparkContext, sqlContext: SQLContext,MarkType: Int, FilePath: String)={
      if(MarkType == 0)
      {
        LoadSimpleL(sc, sqlContext, FilePath)
      }
      if(MarkType == 1)
        {
          LoadSampleX(sc, sqlContext, FilePath)
        }
      if(MarkType == 2)
        {
          LoadGrid(sc, sqlContext,FilePath)
        }
      if(MarkType == 3)
         {
           LoadGridBulid(sc, sqlContext,FilePath)
         }
    }

    def LoadBlockInfo(sc: SparkContext, sqlContext: SQLContext,InfoType: Int,  FilePath: String)= {
      // 加载 工参
      if(InfoType == 0)
      {
        LoadWorkers(sc, sqlContext, FilePath)
      }
    }

     def LoadWorkers(sc: SparkContext, sqlContext: SQLContext,FilePath: String)= {

     }

     def LoadWorkersList(FilePath: String) :List[String] = {
//       var left = List(1,2,3)var result = List[Int]()
       var FileLst = List[String]()
       val file=Source.fromFile(FilePath)
       for(line <- file.getLines)
       {
         FileLst = FileLst :+ line.toString()
       }
       file.close
       return  FileLst
     }

      def LoadSimpleL(sc: SparkContext, sqlContext: SQLContext,FilePath: String)={
      val mark_l = sc.textFile(FilePath)
      val marklchemaString = "GROUPID,N1_CELL_ID,N1_RSRP,N2_CELL_ID,N2_RSRP,N3_CELL_ID,N3_RSRP,N4_CELL_ID,N4_RSRP,N5_CELL_ID,N5_RSRP,N6_CELL_ID,N6_RSRP,N7_CELL_ID,N7_RSRP,S_CELL_ID,AOA,TA,MROID,S_RSRP,MRO_TS,MRO_MMEUES1APID,MRO_MMEGROUPID,MRO_MMECODE,S_RSRQ,LTESCSINRUL,SDATE,CITY,obj_timeStamp,RESERVED3,RESERVED4,callID,iMSI,mro_error,findncell_v,sum_v,N1_PCI,N1_EARFCN,N2_PCI,N2_EARFCN,N3_PCI,N3_EARFCN,N4_PCI,N4_EARFCN,N5_PCI,N5_EARFCN,N6_PCI,N6_EARFCN,N7_PCI,N7_EARFCN,SCELL_PCI,SCELL_EARFCN,CT_MAXRSRP,CT_RSRQ,CT_PCI,CT_EARFCN,CM_MAXRSRP,CM_RSRQ,CM_PCI,CM_EARFCN"
      //,CU_MAXRSRP,CU_RSRQ,CU_PCI,CU_EARFCN
      val marklchema = StructType(
        marklchemaString.split(",").map(fieldName => StructField(fieldName,StringType,true))
//        Array(StructField("GROUPID",StringType,true),
//          StructField("N1_CELL_ID",DoubleType,true),
//          StructField("N1_RSRP",DoubleType,true),
//          StructField("N2_CELL_ID",DoubleType,true),
//          StructField("N2_RSRP",DoubleType,true),
//          StructField("N3_CELL_ID",DoubleType,true),
//          StructField("N3_RSRP",DoubleType,true),
//          StructField("N4_CELL_ID",DoubleType,true),
//          StructField("N4_RSRP",DoubleType,true),
//          StructField("N5_CELL_ID",DoubleType,true),
//          StructField("N5_RSRP",DoubleType,true),
//          StructField("N6_CELL_ID",DoubleType,true),
//          StructField("N6_RSRP",StringType,true),
//          StructField("N7_CELL_ID",StringType,true),
//          StructField("N7_RSRP",StringType,true),
//          StructField("S_CELL_ID",StringType,true),
//          StructField("AOA",StringType,true),
//          StructField("TA",StringType,true),
//          StructField("MROID",StringType,true),
//          StructField("S_RSRP",StringType,true),
//          StructField("MRO_TS",StringType,true),
//          StructField("MRO_MMEUES1APID",StringType,true),
//          StructField("MRO_MMEGROUPID",StringType,true),
//          StructField("MRO_MMECODE",StringType,true),
//          StructField("S_RSRQ",StringType,true),
//          StructField("LTESCSINRUL",StringType,true),
//          StructField("SDATE",StringType,true),
//          StructField("CITY",StringType,true),
//          StructField("obj_timeStamp",StringType,true),
//          StructField("RESERVED3",StringType,true),
//          StructField("RESERVED4",StringType,true),
//          StructField("callID",StringType,true),
//          StructField("iMSI",StringType,true),
//          StructField("mro_error",StringType,true),
//          StructField("findncell_v",StringType,true),
//          StructField("sum_v",StringType,true),
//          StructField("N1_PCI",StringType,true),
//          StructField("N1_EARFCN",StringType,true),
//          StructField("N2_PCI",StringType,true),
//          StructField("N2_EARFCN",StringType,true),
//          StructField("N3_PCI",StringType,true),
//          StructField("N3_EARFCN",StringType,true),
//          StructField("N4_PCI",StringType,true),
//          StructField("N4_EARFCN",StringType,true),
//          StructField("N5_PCI",StringType,true),
//          StructField("N5_EARFCN",StringType,true),
//          StructField("N6_PCI",StringType,true),
//          StructField("N6_EARFCN",StringType,true),
//          StructField("N7_PCI",StringType,true),
//          StructField("N7_EARFCN",StringType,true),
//          StructField("SCELL_PCI",StringType,true),
//          StructField("SCELL_EARFCN",StringType,true),
//          StructField("CT_MAXRSRP",StringType,true),
//          StructField("CT_RSRQ",StringType,true),
//          StructField("CT_PCI",StringType,true),
//          StructField("CT_EARFCN",StringType,true),
//          StructField("CM_MAXRSRP",StringType,true),
//          StructField("CM_RSRQ",StringType,true),
//          StructField("CM_PCI",StringType,true),
//          StructField("CM_EARFCN",StringType,true)
//        )
      )
      val marklrowRDD = mark_l.map(_.split(",")).map(p => Row(p(0).trim,p(1).trim,p(2).trim,p(3).trim,p(4).trim,p(5).trim,p(6).trim,p(7).trim,p(8).trim,p(9).trim,p(10).trim,p(11).trim,p(12).trim,p(13).trim,p(14).trim,p(15).trim,p(16).trim,p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25),p(26),p(27),p(28),p(29),p(30),p(31),p(32),p(33),p(34),p(35),p(36),p(37),p(38),p(39),p(40),p(41),p(42),p(43),p(44),p(45),p(46),p(47),p(48),p(49),p(50),p(51),p(52),p(53),p(54),p(55),p(56),p(57),p(58),p(59)))
      //,p(60),p(61),p(62),p(63)
      val  marklSchemaRDD = sqlContext.createDataFrame(marklrowRDD, marklchema)
      // 20170731, 注册为临时表
      marklSchemaRDD .createOrReplaceTempView("mark_mr")
    }

    def LoadSampleX(sc: SparkContext, sqlContext: SQLContext,FilePath: String)={
      val mark_x = sc.textFile(FilePath)
      val markxchemaString = "rsrp_1,ncell_2,rsrp_2,ncell_3,rsrp_3,ncell_4,rsrp_4,ncell_5,rsrp_5,ncell_6,rsrp_6,ncell_7,rsrp_7,a,lon,lat,mroid,DL_RATE,DL_SINR,DL_CQI,TYPE,b,c,d,e,f"
      val markxchema = StructType(
        markxchemaString.split(",").map(fieldName => StructField(fieldName,StringType,true))
//        List(
//          StructField("rsrp_1",DoubleType,true),
//          StructField("ncell_2",DoubleType,true),
//          StructField("rsrp_2",DoubleType,true),
//          StructField("ncell_3",DoubleType,true),
//          StructField("rsrp_3",DoubleType,true),
//          StructField("ncell_4",DoubleType,true),
//          StructField("rsrp_4",DoubleType,true),
//          StructField("ncell_5",DoubleType,true),
//          StructField("rsrp_5",DoubleType,true),
//          StructField("ncell_6",DoubleType,true),
//          StructField("rsrp_6",DoubleType,true),
//          StructField("ncell_7",DoubleType,true),
//          StructField("rsrp_7",DoubleType,true),
//          StructField("lon",DoubleType,true),
//          StructField("lat",DoubleType,true),
//          StructField("mroid",DoubleType,true),
//          StructField("DL_RATE",IntegerType,true),
//          StructField("DL_SINR",IntegerType,true),
//          StructField("DL_CQI",IntegerType,true),
//          StructField("TYPE",IntegerType,true)
//        )
      )
      val markxrowRDD = mark_x.map(_.split(",")).map(p => Row(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25)))
      val  markxSchemaRDD = sqlContext.createDataFrame(markxrowRDD, markxchema)
      // 20170731, 注册为临时表
      markxSchemaRDD .createOrReplaceTempView("mark_a")
    }

    def LoadGrid(sc: SparkContext, sqlContext: SQLContext,FilePath: String) ={
      val mark_grid = sc.textFile(FilePath)
//      println("data.count: "+mark_grid.count())
      val markGridschemaString = "mas_nei,pci"
      val markGridschema = StructType(
        markGridschemaString.split(",").map(fieldName => StructField(fieldName,StringType,true))
      )
      val markGridrowRDD = mark_grid.map(_.split(",")).map(p => Row(p(0), p(1)))
      val  markGridSchemaRDD = sqlContext.createDataFrame(markGridrowRDD, markGridschema)
      // 20170731, 注册为临时表
      markGridSchemaRDD .createOrReplaceTempView("mark_b")
    }

    def LoadGridBulid(sc: SparkContext, sqlContext: SQLContext,FilePath: String)=
    {
      val mark_gridbulid = sc.textFile(FilePath)
//      println("data.count: "+mark_grid.count())
      val markGridbulidschemaString = "mas_nei,pci"
      val markGridbulidschema = StructType(
        markGridbulidschemaString.split(",").map(fieldName => StructField(fieldName,StringType,true))
      )
      val markGridbulidrowRDD = mark_gridbulid.map(_.split(",")).map(p => Row(p(0), p(1)))
      val  markGridbulidSchemaRDD = sqlContext.createDataFrame(markGridbulidrowRDD, markGridbulidschema)
      // 20170731, 注册为临时表
      markGridbulidSchemaRDD .createOrReplaceTempView("mark_c")
    }
  }
}
