package com.sparkghsom.main.input_generator

import java.io.FileInputStream

import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum, DistanceHierarchyDimension, DistanceHierarchyElem}
import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.mr_ghsom.{Attribute, GHSom, Instance}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{max, min}

class KddCupDatasetReader16(val dataset : RDD[String]) extends Serializable{

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
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 171,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 172,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 173,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 174 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 175,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 176,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 177,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 178,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 179,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 180,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 181,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 182,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 183,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 184,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 185,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 186,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 187,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 188,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 189,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 190,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 191,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 192,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 193,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 194,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 195,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 196,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 197,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 198,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 199,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 200,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 201,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 202,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 203,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 204,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 205,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 206,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 207,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 208,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 209,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 210,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 211,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 212,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 213,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 214,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 215,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 216,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 217 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 218,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 219,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 220,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 221,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 222,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 223,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 224,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 225,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 226,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 227,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 228,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 229,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 230,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 231,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 232,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 233,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 234,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 235,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 236,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 237,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 238,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 239,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 240,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 241,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 242,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 243,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 244,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 245,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 246,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 247,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 248,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 249,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 250,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 251,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 252,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 253,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 254,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 255,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 256,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 257,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 258,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 259,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 260 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 261,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 262,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 263,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 264,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 265,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 266,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 267,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 268,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 269,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 270,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 271,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 272,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 273,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 274,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 275,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 276,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 277,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 278,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 279,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 280,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 281,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 282,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 283,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 284,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 285,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 286,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 287,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 288,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 289,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 290,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 291,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 292,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 293,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 294,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 295,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 296,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 297,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 298,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 299,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 300,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 301,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 302,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 303 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 304,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 305,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 306,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 307,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 308,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 309,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 310,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 311,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 312,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 313,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 314,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 315,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 316,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 317,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 318,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 319,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 320,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 321,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 322,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 323,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 324,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 325,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 326,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 327,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 328,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 329,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 330,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 331,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 332,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 333,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 334,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 335,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 336,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 337,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 338,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 339,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 340,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 341,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 342,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 343,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 344,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 345,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 346 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 347,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 348,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 349,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 350,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 351,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 352,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 353,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 354,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 355,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 356,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 357,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 358,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 359,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 360,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 361,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 362,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 363,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 364,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 365,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 366,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 367,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 368,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 369,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 370,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 371,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 372,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 373,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 374,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 375,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 376,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 377,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 378,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 379,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 380,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 381,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 382,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 383,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 384,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 385,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 386,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 387,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 388,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 389 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 390,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 391,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 392,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 393,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 394,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 395,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 396,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 397,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 398,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 399,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 400,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 401,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 402,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 403,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 404,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 405,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 406,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 407,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 408,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 409,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 410,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 411,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 412,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 413,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 414,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 415,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 416,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 417,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 418,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 419,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 420,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 421,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 422,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 423,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 424,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 425,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 426,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 427,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 428,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 429,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 430,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 431,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 432 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 433,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 434,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 435,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 436,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 437,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 438,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 439,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 440,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 441,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 442,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 443,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 444,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 445,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 446,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 447,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 448,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 449,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 450,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 451,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 452,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 453,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 454,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 455,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 456,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 457,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 458,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 459,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 460,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 461,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 462,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 463,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 464,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 465,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 466,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 467,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 468,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 469,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 470,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 471,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 472,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 473,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 474,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 475 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 476,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 477,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 478,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 479,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 480,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 481,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 482,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 483,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 484,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 485,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 486,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 487,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 488,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 489,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 490,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 491,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 492,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 493,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 494,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 495,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 496,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 497,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 498,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 499,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 500,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 501,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 502,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 503,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 504,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 505,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 506,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 507,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 508,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 509,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 510,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 511,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 512,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 513,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 514,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 515,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 516,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 517,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 518 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 519,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 520,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 521,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 522,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 523,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 524,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 525,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 526,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 527,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 528,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 529,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 530,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 531,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 532,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 533,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 534,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 535,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 536,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 537,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 538,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 539,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 540,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 541,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 542,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 543,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 544,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 545,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 546,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 547,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 548,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 549,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 550,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 551,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 552,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 553,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 554,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 555,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 556,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 557,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 558,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 559,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 560,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 561 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 562,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 563,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 564,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 565,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 566,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 567,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 568,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 569,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 570,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 571,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 572,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 573,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 574,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 575,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 576,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 577,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 578,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 579,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 580,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 581,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 582,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 583,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 584,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 585,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 586,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 587,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 588,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 589,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 590,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 591,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 592,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 593,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 594,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 595,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 596,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 597,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 598,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 599,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 600,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 601,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 602,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 603,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 604 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 605,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 606,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 607,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 608,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 609,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 610,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 611,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 612,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 613,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 614,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 615,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 616,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 617,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 618,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 619,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 620,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 621,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 622,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 623,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 624,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 625,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 626,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 627,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 628,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 629,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 630,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 631,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 632,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 633,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 634,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 635,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 636,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 637,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 638,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 639,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 640,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 641,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 642,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 643,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dummyID",
      index = 644,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "duration",
      index = 645,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "protocol_type",
      index = 646,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "service",
      index = 647 ,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "flag",
      index = 648,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "src_bytes",
      index = 649,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_bytes",
      index = 650,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "land",
      index = 651,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "wrong_fragment",
      index = 652,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "urgent",
      index = 653,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "hot",
      index = 654,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_failed_logins",
      index = 655,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "logged_in",
      index = 656,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_compromised",
      index = 657,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "root_shell",
      index = 658,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "su_attempted",
      index = 659,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_root",
      index = 660,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_file_creations",
      index = 661,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_shells",
      index = 662,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_access_files",
      index = 663,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "num_outbound_cmds",
      index = 664,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_host_login",
      index = 665,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "is_guest_login",
      index = 666,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "count",
      index = 667,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_count",
      index = 668,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "serror_rate",
      index = 669,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_serror_rate",
      index = 670,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "rerror_rate",
      index = 671,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_rerror_rate",
      index = 672,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "same_srv_rate",
      index = 673,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "diff_srv_rate",
      index = 674,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "srv_diff_host_rate",
      index = 675,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_count",
      index = 676,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_count",
      index = 677,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_srv_rate",
      index = 678,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_diff_srv_rate",
      index = 679,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_same_src_port_rate",
      index = 680,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_diff_host_rate",
      index = 681,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_serror_rate",
      index = 682,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_serror_rate",
      index = 683,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_rerror_rate",
      index = 684,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "dst_host_srv_rerror_rate",
      index = 685,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),

    Attribute(name = "label",
      index = 686,
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

object KddCupDatasetReader16 {

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
