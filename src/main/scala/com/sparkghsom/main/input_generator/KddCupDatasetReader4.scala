package com.sparkghsom.main.input_generator

import java.io.FileInputStream

import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum, DistanceHierarchyDimension, DistanceHierarchyElem}
import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.mr_ghsom.{Attribute, GHSom, Instance}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{max, min}

class KddCupDatasetReader4(val dataset : RDD[String]) extends Serializable{

  val attributes = Array(
    Attribute(name = "duration",
      index = 0,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 1,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 2 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 3,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 4,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 5,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 6,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 7,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 8,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 9,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 10,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 11,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 12,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 13,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 14,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 15,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 16,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 17,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 18,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 19,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 20,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 21,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 22,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 23,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 24,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 25,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 26,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 27,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 28,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 29,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 30,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 31,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 32,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 33,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 34,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 35,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 36,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 37,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 38,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 39,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 40,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 41,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 42,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 43,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 44,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 45 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 46,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 47,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 48,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 49,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 50,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 51,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 52,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 53,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 54,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 55,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 56,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 57,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 58,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 59,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 60,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 61,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 62,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 63,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 64,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 65,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 66,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 67,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 68,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 69,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 70,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 71,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 72,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 73,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 74,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 75,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 76,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 77,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 78,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 79,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 80,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 81,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 82,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 83,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 84,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 85,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 86,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 87,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 88 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 89,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 90,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 91,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 92,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 93,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 94,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 95,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 96,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 97,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 98,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 99,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 100,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 101,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 102,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 103,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 104,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 105,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 106,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 107,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 108,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 109,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 110,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 111,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 112,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 113,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 114,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 115,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 116,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 117,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 118,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 119,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 120,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 121,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 122,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 123,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 124,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 125,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 126,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 127,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 128,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 129,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 130,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 131 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 132,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 133,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 134,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 135,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 136,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 137,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 138,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 139,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 140,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 141,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 142,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 143,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 144,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 145,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 146,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 147,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 148,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 149,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 150,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 151,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 152,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 153,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 154,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 155,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 156,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 157,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 158,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 159,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 160,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 161,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 162,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 163,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 164,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 165,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 166,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 167,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 168,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 169,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 170,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue)
  )

  private val datasetOfInstanceObjs = instanizeDataset(dataset)

  case class Data ( id: Int, className : String, attributeVector : Array[DimensionType] ) {
    def getInstanceObj = {
      Instance(id, className, attributeVector)
    }
  }

  def printDataset() {
    // println(datasetRDD.count)
  }

  def getDataset : RDD[Instance] = datasetOfInstanceObjs

  private def instanizeDataset(dataset : RDD[String]) : RDD[Instance] = {

    // RDD[PureData] i.e. RDD[class-name, <attribute-vector>]
    val pureDataRDD: RDD[PureData] = getPureDataRDD(dataset)

    // RDD[PureData] => RDD[(index, AttributeTypeValue[AttributeType, Value])]
    val attributeMap = getIndexAttributeTypeValueRDD( pureDataRDD )

    val (minAttributeMap, maxAttributeMap) = getAttributeMinMaxValuesMap(attributeMap)

    val domainAttributeMap = getAttributeDomainValuesMap(attributeMap)

    for (i <- 0 until attributes.size) {
      attributes(i).dimensionType match {
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL =>
          attributes(i).domainValues = domainAttributeMap(i).filter { value => !value.equals("UNKNOWN") }
            .toArray
            .sorted
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC =>
          attributes(i).maxValue = maxAttributeMap(i)
          attributes(i).minValue = minAttributeMap(i)
        case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
      }
    }

    getInstanceRDD(pureDataRDD)
  }

  // Pure record from the file (just split the class-name and attribute vector)
  private case class PureData(id: Int, name : String, attributeVector : Array[String])
  // Class for holding just a attribute type and the original string value
  private case class AttributeTypeValue (attributeType : DimensionTypeEnum.Value, value : String)

  private def getPureDataRDD(dataset : RDD[String]) = {
    dataset.map{
      record => {
        val array = record.split(',')
        val arrayWithoutId = array.drop(1)
        PureData(
          array.head.toInt,
          array.last, // class label
          Array.tabulate(attributes.size)(i => arrayWithoutId(i))
        )
      }
    }
  }

  private def getIndexAttributeTypeValueRDD ( pureDataRDD : RDD[PureData]) = {

    def convertToIndexAttributeTypeValueTuple(index : Int, value : String) : (Int, AttributeTypeValue)= {
      attributes(index).dimensionType match {
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC =>
          (index, AttributeTypeValue(DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC, value))
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL =>
          (index, AttributeTypeValue(DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL, value))
        case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
      }
    }

    pureDataRDD.flatMap {
      pureData =>
        pureData.attributeVector.zipWithIndex
          .map(valueIndex => convertToIndexAttributeTypeValueTuple(valueIndex._2, valueIndex._1))
    }
  }

  private def getAttributeMinMaxValuesMap( inputRDD : RDD[(Int, AttributeTypeValue)]) = {
    val numericAttributesRDD = inputRDD.filter(tuple => tuple._2.attributeType == DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC)
      .map(tuple => (tuple._1, tuple._2.value.toDouble))
    numericAttributesRDD.persist()

    val maxAttributeMap = numericAttributesRDD.reduceByKey( max(_,_) ).collectAsMap()

    val minAttributeMap = numericAttributesRDD.reduceByKey( min(_,_) ).collectAsMap()

    numericAttributesRDD.unpersist(false)

    (minAttributeMap, maxAttributeMap)
  }

  private def getAttributeDomainValuesMap( inputRDD : RDD[(Int, AttributeTypeValue)]) = {
    val nominalAttributesRDD = inputRDD.filter(tuple => tuple._2.attributeType == DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
      .map(tuple => (tuple._1, Set(tuple._2.value)))
    nominalAttributesRDD.reduceByKey( _.union(_) ).collectAsMap()
  }

  private def getInstanceRDD( inputRDD : RDD[PureData] ) : RDD[Instance] = {

    def getDistanceHierarchyDimension(index : Int, value : String) = {

      attributes(index).dimensionType match {

        case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL => {
          val distanceHierarchyElemObj = DistanceHierarchyElem(value, 0.5)
          DistanceHierarchyDimension(attributes(index).name, distanceHierarchyElemObj, DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
        }

        case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC => {
          var normalizedValue = 0.0
          val den = (attributes(index).maxValue - attributes(index).minValue)

          if (den>0.0)
            normalizedValue = (value.toDouble - attributes(index).minValue) / den

          val distanceHierarchyElemObj = DistanceHierarchyElem("+", normalizedValue)

          DistanceHierarchyDimension(attributes(index).name, distanceHierarchyElemObj, DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC)
        }

        case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
      }
    }

    inputRDD.map{
      record => {
        val attributeVectorIndex = record.attributeVector.zipWithIndex
        Data(
          record.id,
          record.name,
          attributeVectorIndex.map{
            valueIndex => getDistanceHierarchyDimension(valueIndex._2, valueIndex._1)
          }
        )
          .getInstanceObj
      }
    }
  }
}

object KddCupDatasetReader4 {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val (appname, master, datasetPath, testPath) =
    try {
      val prop = new java.util.Properties()
      prop.load(new FileInputStream("config.properties"))
      (
        prop.getProperty("appname"),
        prop.getProperty("master"),
        prop.getProperty("datasetPath"),
        prop.getProperty("testPath")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }


  def main(args : Array[String]) {

    val logger = LogManager.getLogger(appname)

    val sc = SparkConfig.getSparkContext

    var epochs = GHSomConfig.epochs

    println("EPOCHS: " + epochs)
    println("TAU1: " + GHSomConfig.tau1)
    println("TAU2: " + GHSomConfig.tau2)
    /*
    val maxVector = Array.fill(10)(DoubleDimension.MinValue)
    val attribVector = Array.fill(10)(DoubleDimension.getRandomDimensionValue)
    println(maxVector.mkString)
    println(attribVector.mkString)

    for ( i <- 0 until attribVector.size ) {
        maxVector(i) = if (attribVector(i) > maxVector(i)) attribVector(i) else maxVector(i)
    }
    println(maxVector.mkString)
    */
    val dataset = sc.textFile(datasetPath)    // id + label
    var datasetReader = new KddCupDatasetReader(dataset)
    val processedDataset = datasetReader.getDataset

    val testDataset: RDD[String] = sc.textFile(testPath)
    val testDatasetDummy: RDD[String] = testDataset.map(x => x.split(",").toList.drop(1).dropRight(1).mkString(",") + ",dummy")
    val testDatasetDummyWithID: RDD[String] = testDataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",dummy")

    val groundTruth: RDD[(Int, String)] = testDataset.map(x => {
      val row = x.split(",").toList
      (row.take(1).apply(0).toInt , row.takeRight(1).apply(0))
    })

    datasetReader = new KddCupDatasetReader(testDatasetDummyWithID)
    val processedTestDataset: RDD[Instance] = datasetReader.getDataset

    val ghsom = GHSom()
    ghsom.trainAndTest(processedDataset, processedTestDataset, groundTruth, datasetReader.attributes, epochs)
  }
}
