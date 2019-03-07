package com.sparkghsom.main.globals

import java.io.FileInputStream

object GHSomConfig {

  val variance_method = VarianceType.COEFF_UNALIKELIHOOD

  val (epochs, init_layer_size, tau1, tau2, grid_size_factor, hierarchical_count_factor, label_som, num_labels, class_labels,
  compute_topographical_error, debug, ignore_unknown, mqe_criterion, growth_multiple, link_weight, path) =
    try {
      val prop = new java.util.Properties()
      prop.load(new FileInputStream("config.properties"))
      (
        prop.getProperty("EPOCHS").toInt,
        prop.getProperty("INIT_LAYER_SIZE").toInt,
        prop.getProperty("TAU1").toDouble,
        prop.getProperty("TAU2").toDouble,
        prop.getProperty("GRID_SIZE_FACTOR").toDouble,
        prop.getProperty("HIERARCHICAL_COUNT_FACTOR").toDouble,
        prop.getProperty("LABEL_SOM").toBoolean,
        prop.getProperty("NUM_LABELS").toInt,
        prop.getProperty("CLASS_LABELS").toBoolean,
        prop.getProperty("COMPUTE_TOPOGRAPHICAL_ERROR").toBoolean,
        prop.getProperty("DEBUG").toBoolean,
        prop.getProperty("IGNORE_UNKNOWN").toBoolean,
        prop.getProperty("MQE_CRITERION").toBoolean,
        prop.getProperty("GROWTH_MULTIPLE").toBoolean,
        prop.getProperty("LINK_WEIGHT").toDouble,
        prop.getProperty("PATH")
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

  object VarianceType extends Enumeration {
    type DimensionTypeEnum = Value
    val MQE_CRITERION, QE_CRITERION, SIMPLE_MATCHING, COEFF_UNALIKELIHOOD = Value
    //val NUMERIC, NOMINAL, ORDINAL, DISTANCE_HIERARCHY_NUMERIC, DISTANCE_HIERARCHY_NOMINAL = Value
  }

}
