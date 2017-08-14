package com.scala.ceshi

/**
  * Created by MWARE on 2017-07-17.
  */
class ceshi {
  def returnListBuff: List[Int] = {
    // 通过给定的函数创建 5 个元素
    val squares = List.tabulate(6)(n => n * n)
    println( "一维 : " + squares  )
    return squares
  }

}
