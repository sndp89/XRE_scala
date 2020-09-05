// Databricks notebook source
import org.apache.spark.sql.SparkSession
import spark.implicits._
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val sparkSession_tune_20171128 = SparkSession.builder().getOrCreate()
spark.catalog.dropTempView("tuneEvent_20171128")
val tableName_tune_20171128 = s"tuneEvent_20171128"
val input_tune_20171128 = "s3n://cloudbridge-staging/data/deap/sdp.watermark.xre.comcast.TuneEvent/2017/11/28/*/*"
val table = sparkSession_tune_20171128.sql(s"CREATE TEMPORARY VIEW $tableName_tune_20171128\n" + "USING com.databricks.spark.avro\n" + s"OPTIONS (path '$input_tune_20171128')").sparkSession.table(tableName_tune_20171128)
// Data frame
val df_20171128 = sparkSession_tune_20171128.sql("SELECT * " + s"FROM $tableName_tune_20171128 where deliveryMedium = 'IP' and assetClass = 'LINEAR_TV'")
df_20171128.createOrReplaceTempView("dataframe_20171128")
df_20171128.printSchema()
println("done")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dataframe_20171128 limit 1000

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct device.receiverId, device.macAddress, device.deviceType, device.partner, device.ipAddress, device.firmwareVersion,header.hostname,asset.rawUrl, asset.companyName, deliveryMedium, assetClass, tuneStatus, statusMessage, latency from dataframe_20171128

// COMMAND ----------

//Reading devices to account mapping file from oracle which is now in s3
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp
import spark.implicits._
val Account = sc.textFile("s3n://atlantic-production-advanalysis/thin-slice/W_ACCOUNT_VAULT_D.txt")

case class AccountOct(ACCOUNT_NUMBER : String
    , FIRST_NAME : String
    , LAST_NAME : String
    , PARTNER : String
    )
    
val AccountOct10 = Account.map(s=>s.split("\\|")).map(
    s=>AccountOct(s(0).replaceAll("\"", ""),
            s(1).replaceAll("\"", ""),
            s(2).replaceAll("\"", ""),
            s(3).replaceAll("\"", "")
        )
).cache()

//convert to DataFrame & also make it temp table
AccountOct10.toDF().createOrReplaceTempView("Accountdetails") 
val Account_details = spark.sql("select * from Accountdetails")
println("DONE")

// COMMAND ----------

Account_details.repartition(1).write.format("csv").option("header", "true").save("/mnt/scratch/sandeep/W_ACCOUNT_VAULT_D.csv")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from Accountdetails limit 100

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from Accountdetails

// COMMAND ----------

//Reading the table that has recceiver ID and ACCOUNT NUMBER from s3(from Oracle)
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp
import spark.implicits._
val DevicesAccount = sc.textFile("s3n://atlantic-production-advanalysis/thin-slice/DevicesAccount_10Oct2017.txt")
val DevicesAccount_header = DevicesAccount.first()//extract header

case class DevicesAccountOct(ACCOUNT_NUMBER : String, 
                             SERVICE_ACCOUNT_ID : String, 
                             PHYSICAL_DEVICE_ID : String, 
                             RECEIVER_ID : String
                            )
    
val DevicesAccountOct10 = DevicesAccount.filter(row => row!= DevicesAccount_header).filter(s=>s.split("\\|").length > 3).map(s=>s.split("\\|")).map(
    s=>DevicesAccountOct(s(0).replaceAll("\"", ""),
            s(1).replaceAll("\"", ""),
            s(2).replaceAll("\"", ""),
            s(3).replaceAll("\"", "")
        )
).cache()

//convert to DataFrame & also make it temp table
DevicesAccountOct10.toDF().createOrReplaceTempView("Account_and_Devices") 
println("DONE")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from Account_and_Devices limit 100

// COMMAND ----------

//Reading the table that has rosetta features(this was obatined my merging ROSETTA data with account_number(obtained by joining with oracle) & RECEIVER_ID of 11/28 xre raw data) from s3. Test_flag was obtained by joining the  that result with exclusion list receiver id from s3.
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp
import spark.implicits._
val rosetta_accounts = sc.textFile("s3n://atlantic-production-advanalysis/thin-slice/test_accounts_rosetta.csv")
val rosetta_accounts_header = rosetta_accounts.first()//extract header

case class rosetta_accounts_Nov(ACCOUNT_NUMBER : String, RECEIVER_ID: String, ZIP : Integer, ZIP4 : Integer, 
                             STATE : String, City : String, Courtesy_ind: Integer, ACCOUNT_STATUS : String, ethnic_roll_up : String, 
                             Total_Payment : Double, Customer_connect_date: String, latitude: Double, longitude: Double, x1_platform: String,
                             Dwell_type: String, Number_of_adults_unit: Integer, demo_fiber: Integer, test_flag: String
                            )
    
val rosetta_accounts_Nov_MELD = rosetta_accounts.filter(row => row!= rosetta_accounts_header).filter(s =>s.split(",").length > 15).map(s=>s.split(",")).map(
    s=>rosetta_accounts_Nov(s(0).replaceAll("\"", ""),
            s(1).replaceAll("\"", ""),                
            s(2).replaceAll("\"", "").toInt,
            s(3).replaceAll("\"", "").toInt,
            s(4).replaceAll("\"", ""),
            s(5).replaceAll("\"", ""),
            s(6).replaceAll("\"", "").toInt,
            s(7).replaceAll("\"", ""),
            s(8).replaceAll("\"", ""),
            s(9).replaceAll("\"", "").toDouble,
            s(10).replaceAll("\"", ""),
            s(11).replaceAll("\"", "").toDouble,
            s(12).replaceAll("\"", "").toDouble,
            s(13).replaceAll("\"", ""),
            s(14).replaceAll("\"", ""),
            s(15).replaceAll("\"", "").toInt,
            s(16).replaceAll("\"", "").toInt,
            s(17).replaceAll("\"", "")
        )
).cache()

//convert to DataFrame & also make it temp table
rosetta_accounts_Nov_MELD.toDF().createOrReplaceTempView("Rosetta_Accounts_Devices") 
println("DONE")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from Rosetta_Accounts_Devices limit 1000

// COMMAND ----------

//Reading the table that has test receiver ID's(exclusion list) from s3
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp
val test_receivers = sc.textFile("s3n://deap-stg/applications/receiverId-exclusion-list/output/receiverIds/weekly-aggregate/2017-11-28")
val test_receivers_header = test_receivers.first()//extract header

case class test_receivers_Nov(RECEIVER_ID: String)
    
val test_receivers_Nov_MELD = test_receivers.filter(row => row!= test_receivers_header).map(s=>s.split(",")).map(
    s=>test_receivers_Nov(s(0).replaceAll("\"", "")            
        )
).cache()

//convert to DataFrame & also make it temp table
test_receivers_Nov_MELD.toDF().createOrReplaceTempView("test_receivers_Nov_28") 
println("DONE")

// COMMAND ----------

val XRE_rosetta = spark.sql("select DISTINCT a.device.receiverId, a.device.macAddress, a.device.deviceType, a.device.partner, a.device.ipAddress, a.device.firmwareVersion,a.header.hostname,a.asset.rawUrl, a.asset.companyName, a.deliveryMedium, a.assetClass, a.tuneStatus, a.statusMessage, a.latency, b.ACCOUNT_NUMBER,b.ZIP,b.ZIP4,b.STATE,b.City,b.Courtesy_ind,b.ACCOUNT_STATUS,b.ethnic_roll_up,b.Total_Payment,b.Customer_connect_date,b.latitude,b.longitude,b.x1_platform,b.Dwell_type,b.Number_of_adults_unit,b.demo_fiber,b.test_flag from dataframe_20171128 a INNER JOIN Rosetta_Accounts_Devices b ON b.RECEIVER_ID=a.device.receiverId")

// COMMAND ----------

XRE_rosetta.repartition(1).write.format("csv").option("header", "true").save("/mnt/scratch/sandeep/XRE_rosetta3.csv")

// COMMAND ----------

val XRE_ROSETTA = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/scratch/sandeep/XRE_rosetta3.csv")
XRE_ROSETTA.toDF().createOrReplaceTempView("XRE_ROSETTA")

// COMMAND ----------

//Reading RDk data for 11/28
val RDK = sc.textFile("s3n://deap-stg/RDK-data/csv-sandbox/Daily/2017/11/28/*.csv")
val RDK_header = RDK.first()// #extract header

import org.apache.spark.sql.types.DateType
import java.sql.Timestamp

case class RDK_November(
      isTestDevice: String
    , Time:String
    , Version :String
    , StbIp : String
    , mac : String
    , PartnerId : String
    , Make :String
    , StbType :String
    , receiverId: String
    , Content_Type_ID : String
    , TotalStartupTime :Double
    , TotalNetworkTime : Double
    , LoadBucketTime :Double
    , PreparetoPlayBucketTime :Double
    , PlayBucketTime : Double
    , DrmReadyBucketTime : Double
    , DecoderStreamingBucketTime :Double
    , SendonStreamPlayingbacktoXREBucketTime :Double
    , MainManifestRequestTime : Double
    , FirstprofileManifestRequestTime :Double
    , Firstfragmentrequesttime :Double
    , Licenserequesttime :Double
    , NumberOfMainManifest : Integer
    , TotalCombinedTimeofMainManifestRequests: Double
    , NumberOfProfileManifestRequests : Integer
    , TotalCombinedTimeofProfileManifestrequests : Double
    , NumberofFragmentRequests: Integer
    , TotalCombinedTimeofFragmentrequests : Double
    , IS_live_flag : Integer
    , Needed_license_flag: Integer
    , Did_ABR_switch_flag : Integer
    , is_FOG_enabled : Integer
    , isDDPlus : Integer
    , isDemuxed : Integer
    , assetDuration: Integer
    , tuneSuccess: Integer
    )
    

val DailyNov = RDK.filter(row => row != RDK_header).filter(row => row.split(",").length > 12).map(s=>s.split(",")).map(
    s=>RDK_November(s(0).replaceAll("\"", ""),
            s(1).replaceAll("\"", ""),
            s(2).replaceAll("\"", ""),                  
            s(3).replaceAll("\"", ""),                      
            s(4).replaceAll("\"", ""),                      
            s(5).replaceAll("\"", ""),                      
            s(6).replaceAll("\"", ""),                 
            s(7).replaceAll("\"", ""),
            s(8).replaceAll("\"", ""),
            s(9).replaceAll("\"", ""),
            s(10).replaceAll("\"", "").toDouble,
            s(11).replaceAll("\"", "").toDouble,                 
            s(12).replaceAll("\"", "").toDouble,   
            s(13).replaceAll("\"", "").toDouble,       
            s(14).replaceAll("\"", "").toDouble,                 
            s(15).replaceAll("\"", "").toDouble,                   
            s(16).replaceAll("\"", "").toDouble,                    
            s(17).replaceAll("\"", "").toDouble,                    
            s(18).replaceAll("\"", "").toDouble,                
            s(19).replaceAll("\"", "").toDouble,                   
            s(20).replaceAll("\"", "").toDouble,                
            s(21).replaceAll("\"", "").toDouble,
            s(22).replaceAll("\"", "").toInt,
            s(23).replaceAll("\"", "").toDouble,
            s(24).replaceAll("\"", "").toInt,
            s(25).replaceAll("\"", "").toDouble,
            s(26).replaceAll("\"", "").toInt,
            s(27).replaceAll("\"", "").toDouble,
            s(28).replaceAll("\"", "").toInt,
            s(29).replaceAll("\"", "").toInt,
            s(30).replaceAll("\"", "").toInt,
            s(31).replaceAll("\"", "").toInt,
            s(32).replaceAll("\"", "").toInt,
            s(33).replaceAll("\"", "").toInt,
            s(34).replaceAll("\"", "").toInt,
            s(35).replaceAll("\"", "").toInt
        )
).cache()

// convert to DataFrame & also make it temp table
DailyNov.toDF().createOrReplaceTempView("Nov_RDK_28")

println("DONE")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from Nov_RDK_28 limit 1000

// COMMAND ----------

//FEATURES OF RDK, XRE & ROSETTA combined
val RDK_XRE_ROSETTA = spark.sql("select * from XRE_ROSETTA a INNER JOIN Nov_RDK_28 b where a.receiverId=b.receiverId")
RDK_XRE_ROSETTA.printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from XRE_ROSETTA a INNER JOIN Nov_RDK_28 b where a.receiverId=b.receiverId
// MAGIC ---RDK_XRE_ROSETTA.repartition(1).write.format("csv").option("header", "true").save("/mnt/scratch/sandeep/RDK_XRE_ROSETTA.csv")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from XRE_ROSETTA limit 1000

// COMMAND ----------

XRE_ROSETTA.printSchema()

// COMMAND ----------

XRE_ROSETTA.count()

// COMMAND ----------

///Feature Selection and Feature Extraction
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.stat.Correlation
//Set Error reporting and not see a whole bunch of warnings
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors // we may get some errors if this is not imported
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("/mnt/scratch/sandeep/XRE_rosetta3.csv")

val logregdata = (data.select(data("test_flag").as("label"), $"deviceType", $"companyName", $"tuneStatus", $"statusMessage", $"latency",
                   $"STATE", $"City", $"Courtesy_ind", $"ethnic_roll_up", $"Total_Payment",$"Dwell_type", $"Number_of_adults_unit", $"demo_fiber"))

//Convert strings to numerical values
val deviceIndexer = new StringIndexer().setInputCol("deviceType").setOutputCol("deviceTypeIndex").setHandleInvalid("keep")
val tuneStatusIndexer = new StringIndexer().setInputCol("tuneStatus").setOutputCol("tuneStatusIndex").setHandleInvalid("keep")
val statusMessageIndexer = new StringIndexer().setInputCol("statusMessage").setOutputCol("statusMessageIndex").setHandleInvalid("keep")
val STATEIndexer = new StringIndexer().setInputCol("STATE").setOutputCol("STATEIndex").setHandleInvalid("keep")
val CityIndexer = new StringIndexer().setInputCol("City").setOutputCol("CityIndex").setHandleInvalid("keep")
val ethnicIndexer = new StringIndexer().setInputCol("ethnic_roll_up").setOutputCol("ethnicIndex").setHandleInvalid("keep")
val DwellIndexer = new StringIndexer().setInputCol("Dwell_type").setOutputCol("DwellIndex").setHandleInvalid("keep")

//Convert these above numerical values into One Hot Encoding 0 or 1
val deviceEncoder = new OneHotEncoder().setInputCol("deviceTypeIndex").setOutputCol("devicetypeVec")
val tuneStatusEncoder = new OneHotEncoder().setInputCol("tuneStatusIndex").setOutputCol("tuneStatusVec")
val statusMessageEncoder = new OneHotEncoder().setInputCol("statusMessageIndex").setOutputCol("statusMessageVec")
val STATEEncoder = new OneHotEncoder().setInputCol("STATEIndex").setOutputCol("STATEVec")
val CityEncoder = new OneHotEncoder().setInputCol("CityIndex").setOutputCol("CityVec")
val ethnicEncoder = new OneHotEncoder().setInputCol("ethnicIndex").setOutputCol("ethnicVec")
val DwellEncoder = new OneHotEncoder().setInputCol("DwellIndex").setOutputCol("DwellVec")


// COMMAND ----------

////Build a model using test_flag as target variable
//(label,features)
val assembler = (new VectorAssembler().setInputCols(Array("devicetypeVec","tuneStatusVec", "statusMessageVec", "latency", "STATEVec", "CityVec", "Courtesy_ind", "ethnicVec", "Total_Payment", "DwellVec", "Number_of_adults_unit", "demo_fiber")).setOutputCol("features"))
val Array(training, test) = logregdata.randomSplit(Array(0.7,0.3), seed=35262)

//Set up ML pipeline
import org.apache.spark.ml.Pipeline
val lr = new LogisticRegression()
val pipeline = new Pipeline().setStages(Array(deviceIndexer, tuneStatusIndexer, statusMessageIndexer, STATEIndexer, CityIndexer, ethnicIndexer, DwellIndexer, deviceEncoder, tuneStatusEncoder, statusMessageEncoder, STATEEncoder, CityEncoder, ethnicEncoder, DwellEncoder, assembler, lr))
val model = pipeline.fit(training)
val results = model.transform(test)

//Model evaluation
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val predictionAndLabels = results.select($"prediction",$"label").as[(Double,Double)].rdd
val metrics = new MulticlassMetrics(predictionAndLabels)
println("Confusion matrix:")
println(metrics.confusionMatrix)
println(metrics.accuracy)

// COMMAND ----------

import FeatureExtraction._

// COMMAND ----------

dbutils.fs.help()
