package com.sparkghsom.main.utils

import java.io.File

import org.apache.commons.io.FileUtils

/**
  * Created to replicate KDD CUP dataset columns
  * for high-dimensional experiments
  */
object Replicator {

  def main(args : Array[String]): Unit = {

    // start: generate increments
/*    val increment: Int = 43   // numfeatures
    val multiplier = 128
    val maxRange = 128*increment
    val startRange= 2752
    val rangeX: Range = Predef.intWrapper(startRange).to(maxRange)
    val byRange: Range = rangeX.by(increment)
    byRange.foreach(x => print(x + ", "))
    System.exit(1) */
    // end: generate increments



    // start: generator (used to generate no-name feature configuration for microarray data)
    /*
    generator(2000,"alon")
    generator(22283,"borovecki")
    generator(22283,"burczynski")
    generator(12625,"chiaretti")
    generator(22215,"chin")
    generator(22283,"chowdary")
    generator(1413,"christensen")
    generator(7129,"golub")
    generator(12533,"gordon")
    generator(2905,"gravier")
    generator(2308,"khan")
    generator(7128,"pomeroy")
    generator(6817,"shipp")
    generator(12600,"singh")
    generator(456,"sorlie")
    generator(5565,"su")
    generator(10100,"subramanian")
    generator(12625,"tian")
    generator(7129,"west")
    generator(12625,"yeoh")
    System.exit(1)
    */
    // end generator



    val max = 1
    var i = 0
    var head = ""

    // incremental id where to start from
    //val range = Array(0, 43, 86)     // 2x
    //val range = Array(0, 43, 86, 129)       // 4x
    //val range = Array(0, 43, 86, 129, 172, 215, 258, 301)       // 8x

    //val range = Array(0, 43, 86, 129, 172, 215, 258, 301, 344, 387, 430, 473, 516, 559, 602, 645)       // 16x

    val range = Array(0, 43, 86, 129, 172, 215, 258, 301, 344, 387, 430, 473, 516, 559, 602, 645, 688, 731, 774, 817, 860, 903, 946, 989, 1035, 1075, 1118, 1161, 1204, 1247, 1290, 1333)       // 32x

    // val range = Array(0, 43, 86, 129, 172, 215, 258, 301, 344, 387, 430, 473, 516, 559, 602, 645, 688, 731, 774, 817, 860, 903, 946, 989, 1035, 1075, 1118, 1161, 1204, 1247, 1290, 1333, 1376, 1419, 1462, 1505, 1548, 1591, 1634, 1677, 1720, 1763, 1806, 1849, 1892, 1935, 1978, 2021, 2064, 2107, 2150, 2193, 2236, 2279, 2322, 2365, 2408, 2451, 2494, 2537, 2580, 2623, 2666, 2709, 2752)       // 64x

    // val range = Array(0, 43, 86, 129, 172, 215, 258, 301, 344, 387, 430, 473, 516, 559, 602, 645, 688, 731, 774, 817, 860, 903, 946, 989, 1035, 1075, 1118, 1161, 1204, 1247, 1290, 1333, 1376, 1419, 1462, 1505, 1548, 1591, 1634, 1677, 1720, 1763, 1806, 1849, 1892, 1935, 1978, 2021, 2064, 2107, 2150, 2193, 2236, 2279, 2322, 2365, 2408, 2451, 2494, 2537, 2580, 2623, 2666, 2709, 2752, 2795, 2838, 2881, 2924, 2967, 3010, 3053, 3096, 3139, 3182, 3225, 3268, 3311, 3354, 3397, 3440, 3483, 3526, 3569, 3612, 3655, 3698, 3741, 3784, 3827, 3870, 3913, 3956, 3999, 4042, 4085, 4128, 4171, 4214, 4257, 4300, 4343, 4386, 4429, 4472, 4515, 4558, 4601, 4644, 4687, 4730, 4773, 4816, 4859, 4902, 4945, 4988, 5031, 5074, 5117, 5160, 5203, 5246, 5289, 5332, 5375, 5418, 5461, 5504)       // 128x


    for (i <- range)
      head += "Attribute(name = \"duration\",\n                            index = " + i + ", \n                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"protocol_type\",\n                          index = " + (i+1) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"service\",\n                          index = " + (i+2) + " ,\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"flag\",\n                          index = " + (i+3) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"src_bytes\",\n                          index = " + (i+4) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_bytes\",\n                          index = " + (i+5) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"land\",\n                          index = " + (i+6) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"wrong_fragment\",\n                          index = " + (i+7) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"urgent\",\n                          index = " + (i+8) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"hot\",\n                          index = " + (i+9) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_failed_logins\",\n                          index = " + (i+10) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"logged_in\",\n                          index = " + (i+11) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_compromised\",\n                          index = " + (i+12) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"root_shell\",\n                          index = " + (i+13) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"su_attempted\",\n                          index = " + (i+14) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_root\",\n                          index = " + (i+15) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_file_creations\",\n                          index = " + (i+16) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_shells\",\n                          index = " + (i+17) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_access_files\",\n                          index = " + (i+18) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"num_outbound_cmds\",\n                          index = " + (i+19) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"is_host_login\",\n                          index = " + (i+20) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"is_guest_login\",\n                          index = " + (i+21) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"count\",\n                          index = " + (i+22) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"srv_count\",\n                          index = " + (i+23) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"serror_rate\",\n                          index = " + (i+24) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"srv_serror_rate\",\n                          index = " + (i+25) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"rerror_rate\",\n                          index = " + (i+26) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"srv_rerror_rate\",\n                          index = " + (i+27) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"same_srv_rate\",\n                          index = " + (i+28) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"diff_srv_rate\",\n                          index = " + (i+29) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"srv_diff_host_rate\",\n                          index = " + (i+30) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_count\",\n                          index = " + (i+31) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_srv_count\",\n                          index = " + (i+32) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_same_srv_rate\",\n                          index = " + (i+33) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_diff_srv_rate\",\n                          index = " + (i+34) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_same_src_port_rate\",\n                          index = " + (i+35) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_srv_diff_host_rate\",\n                          index = " + (i+36) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_serror_rate\",\n                          index = " + (i+37) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_srv_serror_rate\",\n                          index = " + (i+38) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_rerror_rate\",\n                          index = " + (i+39) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dst_host_srv_rerror_rate\",\n                          index = " + (i+40) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"label\",\n                          index = " + (i+41) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        Attribute(name = \"dummyID\",\n                          index = " + (i+42) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n                        "

    println(head)
  }

  //********************************************************************************
  def generator(max: Int, dataset: String): Unit = {

    var incr = 1

    var base = ""

    for (i <- 0 to max-1) {
      base += "                        Attribute(name = \"V" + (incr) + "\",\n                          index = " + (incr) + ",\n                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,\n                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),\n\n"

      incr = incr + 1

    }

    //println(base)

    val encoding : String = null
    FileUtils.writeStringToFile(new File(dataset), base, encoding)

  }

}
