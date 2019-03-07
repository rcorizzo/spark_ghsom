package com.sparkghsom.main.utils
import weka.classifiers.{AbstractClassifier, Evaluation}
import weka.classifiers.evaluation.Prediction
import weka.classifiers.functions.LibSVM
import weka.core.converters.ConverterUtils.DataSource
import weka.core.{Instances, WekaPackageManager}
import java.io.{File, FileInputStream, FileWriter}
import java.util

import scala.collection.mutable.ArrayBuffer


object SVM_SAP {

  val inputQ = "extid_ADFG,extid_ADMIN_WORKBENCH,extid_AGGDD,extid_AGSDFGV,extid_ALGER,extid_AMSTERDAM,extid_ASF,extid_ASGFGF,extid_ASJGF,extid_AURON,extid_BATTERY,extid_BELGIUMCHAR,extid_BIGDATA,extid_BLACKCARD,extid_BOB,extid_BOBSINFOC,extid_BROWNSA,extid_BST,extid_BZDFBG,extid_CAEN,extid_CALEN,extid_CANYONNE,extid_CATALOGUEUNKUT,extid_CHARONE,extid_COLLEGE,extid_CORPS,extid_CUTINHO,extid_DAMSOZ,extid_DDP,extid_DENVER,extid_DHJHDF,extid_DHSFD,extid_DJDTY,extid_DOLLARS,extid_DORDEPLATINE,extid_DRAKE,extid_DUBLIN,extid_DUSTY,extid_DXA,extid_EARTH,extid_EMI,extid_EMPLEENEW,extid_EMPLOYEE,extid_EN_Y,extid_ERRO_0API2BMD8N2ET85BJSA0FFDOV,extid_ERRO_0PMVW2Q0N6Y3MP5FL42G3NITR,extid_ERRO_12EXZN432LP4OGIMBCHTDMQRZ,extid_ERRO_24SBG1S9KCQ72TIWOX30BEYOF,extid_ERRO_2W56AGMAAAWPAYMI40DDA7U33,extid_ERRO_3BRQG37ZFCKEG88G7B5NWXTZJ,extid_ERRO_45LWIQOAMOXSGMSB6J2ZPGU8F,extid_ERRO_4XBCG260GNKIBV5LY3ASNO7BJ,extid_ERRO_4Z2VRKJ9LSLE5SJOCTACYZ7DB,extid_ERRO_5P0SDDNQAM7873IWPNILLVKEN,extid_ERRO_5SX1U7ZKT4YS673JC17ORI7M7,extid_ERRO_6MR7WVFW0HC66LNEB950K17V3,extid_ERRO_7EGNU6XLUFYW1U0P2TCTI8KY7,extid_ERRO_8663RIFBOELLX2DZUDKMGFY1B,extid_ERRO_8CDZPKISJ0Z2GS9JYHKKDN7DB,extid_ERRO_8LSYO1SHW8GHP9RQ9EKJFLZIN,extid_ERRO_943FMW0ICZLSC0MUQ1SDBUKGF,extid_ERRO_96E5HQDX70I0JPRCWUSQZZ8R3,extid_ERRO_APV4VCZTH3N6NOWZLJGDEJUA7,extid_ERRO_AUS9MSR4I4HD3JN8CY0MSAYOF,extid_ERRO_BYKL8RXBGU0J421XGIY4D54B3,extid_ERRO_C58NTWAG8JGF2T2BY32WK5P1R,extid_ERRO_CE75HFQK61QSU0DTW2G8OPOUN,extid_ERRO_CYUFPK4VSQ3OAY454UKHMVNDB,extid_ERRO_CZ5TSTH97OMQ1Y7SBXW7FP24V,extid_ERRO_DQV9Q4YZ1N9FX6L33I40DWF7Z,extid_ERRO_EG33GQA92V4P59PIE6Q3QQ51R,extid_ERRO_EIKPNGGOVLW5SEYDV2BTC3SB3,extid_ERWE,extid_EUROP,extid_EUROS,extid_EUROSFD,extid_EYRSG,extid_EZE,extid_FACEBOOK,extid_FAMAS,extid_FASD,extid_FATAL,extid_FBRIP,extid_FERR,extid_FGDF,extid_FOOTBALL,extid_FRA,extid_GAF,extid_GEQRGT,extid_GOALCATALOG,extid_GOODLUCK,extid_GQREG,extid_GRADFS,extid_GREASY,extid_GROOVE,extid_HALFTIME,extid_HAUNTED,extid_HELSINKI,extid_HIGH,extid_HIST,extid_HJSFGH,extid_HSDFGH,extid_HSG,extid_HSGF,extid_HTRSGTR,extid_IA_IPH_X,extid_INDIA,extid_INFOCAT,extid_INFOCATA,extid_INFOCATALOGUE,extid_INFOCATCHAR,extid_INFOCATFORPEPPER,extid_INFOCATKEY,extid_INFOCATSTEVE,extid_INFOJOUEU,extid_INFOMACHI,extid_INFOSCROR,extid_INLEAING,extid_INSTA,extid_INTEL,extid_IOC_CHAR_CAPITALS,extid_IOC_CHAR_EMP,extid_IOC_CHAR_FOOD,extid_IOC_CHAR_FR,extid_IOC_CHAR_KD,extid_IOC_CHAR_SD,extid_IOC_CHAR_TOREADOR,extid_IOC_CHAR_W,extid_IOC_CHAR_WC,extid_IOC_FT_CHAR,extid_IOC_SN_CHAR,extid_IOC_TESTING,extid_ISOLA,extid_ISOLA2,extid_JAYZ,extid_JDETYYHJ,extid_JENSETLER,extid_JJRY,extid_JUL,extid_JUNCTION,extid_JUNK,extid_KDA,extid_KDB,extid_KDC,extid_KDD,extid_KDE,extid_KDP,extid_KDQ,extid_KDR,extid_KDS,extid_KEY,extid_KFFDD,extid_KTR,extid_LAKE,extid_LANDING,extid_LANDINGS,extid_LFY,extid_LIGAONE,extid_LODGE,extid_LOKE,extid_LONDON,extid_LONELY,extid_LOOSE,extid_LOOT,extid_LUCKY,extid_LUTVANIA,extid_M1911,extid_MARCELO,extid_MARS,extid_MARSEILLE,extid_MENTON,extid_MEX,extid_MEXICO,extid_MIN,extid_MOISTY,extid_MONACO,extid_MOONA,extid_MOSCOU,extid_MP5,extid_MUMBAY,extid_NEWONE,extid_NEWYORK,extid_NEYMAR,extid_NICE,extid_NINETTA,extid_NPLCLNT001,extid_NYP,extid_OAPAPAYA,extid_ORDER,extid_OYE,extid_PAGE123,extid_PAGE3,extid_PAGE4,extid_PAGE425,extid_PAGE45,extid_PAGE456,extid_PAGE777,extid_PARIS,extid_PAU,extid_PAYNE,extid_PERISCOPE,extid_PLANN,extid_PLEASANT,extid_PNL,extid_POLICE,extid_POPUL,extid_PORSCHE,extid_PRICE,extid_RASA,extid_REELS,extid_REG,extid_RETAIL,extid_RHQEG,extid_RICCARDO,extid_RISKY,extid_ROUB,extid_RPG,extid_RUSSIA,extid_SALTY,extid_SAPBIW,extid_SAPI_RFC_DEBUG,extid_SAPNETW,extid_SAYEN,extid_SCHEDULER_FROM_TREE0PAK_C12JY4Q49CBX991TCWCZ9WM9X,extid_SCHEDULER_FROM_TREE0PAK_CQN9Q498PX7YZB8JWSV9DSC79,extid_SCHEDULER_FROM_TREEZPAK_A07XZT52U2CLG5NFEGT5AXAMN,extid_SCREEN,extid_SDAGF,extid_SDFDFG,extid_SDGDF,extid_SDHGGF,extid_SDL_REQU_08XYQT943I1IZAR952AG44DN3,extid_SDL_REQU_1Q84G49YE1KAKLBAGJ0J67GNJ,extid_SDL_REQU_1W942AKE9Y2IOV2HAKF565R0V,extid_SDL_REQU_3DJ9RLL8KHLAA5MIM15888U1B,extid_SEINEZOO,extid_SELECAO,extid_SELLS,extid_SHE,extid_SHOORE,extid_SKILLS,extid_SMG,extid_SNAP,extid_SNEAZZY,extid_SNOBY,extid_STICKY,extid_THOMSON,extid_TILTED,extid_TINDER,extid_TOKYO,extid_TOMATO,extid_TORONTO,extid_TOTO,extid_TOULON,extid_TOWER,extid_TOWN,extid_TRAINS,extid_TURCOIN,extid_TWITTER,extid_VEND,extid_VENEZUELA,extid_WAILINGS,extid_WINS,extid_WOODS,extid_WORLDHAMP,extid_WRH,extid_WTYH,extid_ZDSOCOMD,extid_ZPAK_A07XZT52U2CLG5NFEGT5AXAMN,extid_ZUCKEFR,object_BW_PROCESS,object_RSAP,object_RSAR,object_RSD,object_RSSM,subobject_IOBC_DEL,subobject_IOBC_SAVE,subobject_IOBJ_DEL,subobject_IOBJ_SAVE,subobject_LOADING,subobject_METADATA,subobject_ODSO_DEL,subobject_RSAP_INFO,subobject_SDL,subobject_TCB"
  val numAttributes = inputQ.split(",").length
  val folds = 5
  val seqLength = Array(31,7,24,2,11,12,4,8,1,22,21,34,20,28,2,9,2,6,20,4,7,4,3,22,5,6,15,4,8,11,7,5,5,8,11,9,3,1,10,9,7,3,16,23,2,8,5,10,4)
  var seqCount = 1
  var strContainer = ArrayBuffer[String]()

  while(seqCount <= seqLength.length) {
    val currSeqLen = seqLength.apply(seqCount-1)

    for(i<- 1 to currSeqLen-1) {
      val str = String.valueOf(seqCount).concat(",").concat(String.valueOf(i))
      strContainer += str
      println(str)
    }
    seqCount +=1
  }

  // ************************************************************************************
  def main(args: Array[String]): Unit = {

    val path = "/Users/hydergine/git/dm-toolkit/"
    val datasetName = "sap_split_"

    //val modality = "polynomial"
    //val modality = "rbf"
    //val modality = "sigmoid"
    val modality = "linear"

    var ignoredAttributes = Array(0,1,2,3,4)    // Ignoring id, actionid, user, date, time

    var split = 1

    while (split <= folds) {

      System.out.println("Fold:" + split)
      val trainFile = split + "_training"
      val testFile = split + "_test"
      trainAndTestSVM(path, datasetName, trainFile, testFile, modality, split, ignoredAttributes)

      split = split + 1
    }
    System.out.println()
  }
  // ************************************************************************************
  def trainAndTestSVM(path: String,
                      datasetName: String,
                      trainFile: String,
                      testFile: String,
                      modality: String,
                      splitNumber: Int,
                      ignoredAttributes: Array[Int])  = {

    WekaPackageManager.loadPackages( false, true, false )

    val classifier : AbstractClassifier = new LibSVM()

    val options: String = {
      if(modality.equals("linear"))
        "-S 0 -K 0 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("polynomial"))
        "-S 0 -K 1 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("rbf"))
        "-S 0 -K 2 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("sigmoid"))
        "-S 0 -K 3 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else
        ""
    }
    /*
     -S <int>
        Set type of SVM (default: 0)
          0 = C-SVC
          1 = nu-SVC
          2 = one-class SVM
          3 = epsilon-SVR
          4 = nu-SVR
     */
    val optionsArray = options.split(" ")
    classifier.setOptions(optionsArray)

    val trainDataset: Instances = new DataSource(path + datasetName + trainFile + ".csv").getDataSet
    println("Train: " + path + datasetName + trainFile + ".csv")

    val testDataset: Instances = new DataSource(path + datasetName + testFile + ".csv").getDataSet
    println("Test: " + path + datasetName + testFile + ".csv")

    ignoredAttributes.foreach(i =>{
      trainDataset.deleteAttributeAt(i)
      testDataset.deleteAttributeAt(i)
    })


    trainDataset.setClassIndex(testDataset.numAttributes()-1)
    testDataset.setClassIndex(testDataset.numAttributes()-1)

    classifier.buildClassifier(trainDataset)

    val eval : Evaluation  = new Evaluation(trainDataset)
    eval.evaluateModel(classifier, testDataset)

    val predictions: util.ArrayList[Prediction] = eval.predictions()
    val predArray: Array[String] = predictions.toArray.map(p => p.toString)   // to return

    writeFile("output/SVM_" + modality + "_" + datasetName + "_" + splitNumber + ".predictions", "sequence_id,subseq_length,actual_class,predicted_class")

    var predCount=0

    predArray.foreach(e => {
      val split = e.split(" ")
      val measured = split(1)
      val predicted =  split(2)
      val seqDescription = strContainer.head
      writeFile("output/SVM_" + modality + "_" + datasetName + "_" + splitNumber + ".predictions", seqDescription + "," + measured + "," + predicted)
      predCount += 1
      strContainer.remove(0)
    })

  }
  // ************************************************************************************
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }
    fw.close()
  }
  // ************************************************************************************


}
