package com.xr.sparkstreaming.utils

import com.typesafe.config.ConfigFactory

/**
  * User:xr
  * Date:2019/8/20 3:25
  * Description:配置特征
  **/

trait Config {
    //加载配置文件
    val config = ConfigFactory.load()
}
