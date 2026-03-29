# 5.9 案例分析（DStream 历史案例）

本节保留一个基于 DStream + HBase 的历史工程案例，用来说明早期 Spark Streaming 如何围绕微批、目录监控和 `foreachRDD` 组织处理链路。它对理解存量系统仍然有价值，但不应被视为 Spark 4.x 新项目的默认模板；如果今天重新实现同类需求，应优先考虑 Structured Streaming、DataFrame/Dataset、checkpoint 与事件时间语义。

案例场景仍然采用油井传感器日志：持续接收设备上报数据，筛选告警事件，并把明细与汇总结果写入外部存储。这里保留 HBase 写入流程，目的是帮助读者读懂“Spark Streaming + HBase”这一类历史架构。典型实时用例包括：

  - 网站监控、网络监控

  - 欺诈识别

  - 网页点击

  - 广告

  - 物联网传感器

Spark Streaming将数据流划分为每X秒的批次，称为DStream，它在内部是一系列RDD。Spark应用程序使用Spark
API处理RDD，并且批处理返回RDD操作的处理结果。

![https://www.mapr.com/sites/default/files/blogimages/sparkstream2-blog.png](../media/05_stream_processing/media/image14.jpeg)

图例 5‑14 将数据流划分为X秒的批次

Spark
Streaming支持HDFS目录、TCP套接字、Kafka、Flume等数据源。数据流可以使用Spark的核心API，DataFrames
SQL或机器学习API进行处理，并且可以保存到文件系统、HDFS、数据库或提供Hadoop OutputFormat的任何数据源。

下面这个历史案例继续沿用 DStream + HBase 的组合，目的是说明一个典型微批流式应用怎样从输入流一路走到外部存储。示例背景是油井监控：钻井平台传感器持续产生日志数据，Spark Streaming 负责实时处理，再把结果写入 HBase，供后续分析和报表使用。

![](../media/05_stream_processing/media/image15.jpeg)

图例 5‑15 流数据处理阶段

要在HBase中存储数据流中的每一个事件，还需要筛选和存储报警信息，以及每天的汇总统计信息。Spark
Streaming示例流程首先读取传感器产生的日志信息，然后处理流数据，并将处理后的数据写入到
HBase表。Spark Streaming示例代码执行以下操作：

  - 读取日志信息。

  - 处理流数据。

  - 将处理后的数据写入HBase表。

汇总统计的代码执行以下操作：

  - 读取写入HBase的数据

  - 计算每日摘要统计信息

  - 写汇总统计到 HBase表

### 5.9.1 探索数据

传感器日志信息的数据列包括日期、时间和一些与来自传感器读数的相关度量，例如psi、流量等，另外还包括传感器的维护和生产厂家信息。

#### 5.9.1.1 传感器日志

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
val schema =
StructType(
Array(
StructField("resid", StringType, nullable=false),
StructField("date", StringType, nullable=false),
StructField("time", StringType, nullable=false),
StructField("hz", DoubleType, nullable=false),
StructField("disp", DoubleType, nullable=false),
StructField("flo", LongType, nullable=false),
StructField("sedPPM", DoubleType, nullable=false),
StructField("psi", LongType, nullable=false),
StructField("chlPPM", DoubleType, nullable=false)
)
)
// Exiting paste mode, now interpreting.
schema: org.apache.spark.sql.types.StructType =
StructType(StructField(resid,StringType,false),
StructField(date,StringType,false), StructField(time,StringType,false),
StructField(hz,DoubleType,false), StructField(disp,DoubleType,false),
StructField(flo,LongType,false), StructField(sedPPM,DoubleType,false),
StructField(psi,LongType,false), StructField(chlPPM,DoubleType,false))
scala> case class Sensor(resid: String, date: String, time: String, hz:
Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM:
Double)
defined class Sensor
scala> val df =
spark.read.schema(schema).csv("/data/sensordata.csv").as[Sensor]
df: org.apache.spark.sql.Dataset[Sensor] = [sensorname: string, date:
string ... 7 more fields]
scala> df.show(5)
+----------+-------+----+-----+-----+---+------+---+------+
| resid | date|time| hz| disp|flo|sedPPM|psi|chlPPM|
+----------+-------+----+-----+-----+---+------+---+------+
| COHUTTA|3/10/14|1:01|10.27| 1.73|881| 1.56| 85| 1.94|
| COHUTTA|3/10/14|1:02| 9.67|1.731|882| 0.52| 87| 1.79|
| COHUTTA|3/10/14|1:03|10.47|1.732|882| 1.7| 92| 0.66|
| COHUTTA|3/10/14|1:05| 9.56|1.734|883| 1.35| 99| 0.68|
| COHUTTA|3/10/14|1:06| 9.74|1.736|884| 1.27| 92| 0.73|
+----------+-------+----+-----+-----+---+------+---+------+
only showing top 5 rows
```

#### 5.9.1.2 维护信息

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
val maintSchema =
StructType(
Array(
StructField("resid", StringType, nullable=false),
StructField("eventDate", StringType, nullable=false),
StructField("technician", StringType, nullable=false),
StructField("description", StringType, nullable=false)
)
)
// Exiting paste mode, now interpreting.
maintSchema: org.apache.spark.sql.types.StructType =
StructType(StructField(resid,StringType,false),
StructField(eventDate,StringType,false),
StructField(technician,StringType,false),
StructField(description,StringType,false))
scala> case class Maint(resid: String, eventDate: String, technician:
String, description: String)
scala> df.show(5)
+----------+---------+----------+--------------------+
| resid|eventDate|technician| description|
+----------+---------+----------+--------------------+
| COHUTTA| 3/15/11| J.Thomas| Install|
| COHUTTA| 2/20/12| J.Thomas| Inspection|
| COHUTTA| 1/13/13| J.Thomas| Inspection|
| COHUTTA| 6/15/13| J.Thomas| Tighten Mounts|
| COHUTTA| 2/27/14| J.Thomas| Inspection|
+----------+---------+----------+--------------------+
only showing top 5 rows
```

#### 5.9.1.3 生产厂家

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
val pumpInfoSchema =
StructType(
Array(
StructField("resid", StringType, nullable=false),
StructField("pumpType", StringType, nullable=false),
StructField("purchaseDate", StringType, nullable=false),
StructField("serviceDate", StringType, nullable=false),
StructField("vendor", StringType, nullable=false),
StructField("longitude", FloatType, nullable=false),
StructField("latitude", FloatType, nullable=false)
)
)
// Exiting paste mode, now interpreting.
pumpInfoSchema: org.apache.spark.sql.types.StructType =
StructType(StructField(resid,StringType,false),
StructField(pumpType,StringType,false),
StructField(purchaseDate,StringType,false),
StructField(serviceDate,StringType,false),
StructField(vendor,StringType,false),
StructField(longitude,FloatType,false),
StructField(latitude,FloatType,false))
scala> case class PumpInfo(resid: String, pumpType: String,
purchaseDate: String, serviceDate: String, vendor: String, longitude:
Float, latitude: Float)
defined class PumpInfo
scala> val df =
spark.read.schema(pumpInfoSchema).csv("/data/sensorvendor.csv").as[PumpInfo]
df: org.apache.spark.sql.Dataset[PumpInfo] = [resid: string,
pumpType: string ... 5 more fields]
scala> df.show(5)
+----------+---------+------------+-----------+--------+---------+---------+
| resid| pumpType|purchaseDate|serviceDate| vendor|longitude| latitude|
+----------+---------+------------+-----------+--------+---------+---------+
| COHUTTA|HYDROPUMP| 11/27/10| 3/15/11|HYDROCAM|29.687277|-91.16249|
|NANTAHALLA|HYDROPUMP| 11/27/10| 3/15/11|HYDROCAM|29.687128| -91.1625|
|THERMALITO|HYDROPUMP| 5/25/08| 9/26/09| GENPUMP|29.687277|-91.16249|
| BUTTE|HYDROPUMP| 5/25/08| 9/26/09| GENPUMP| 29.68693| -91.1625|
| CARGO|HYDROPUMP| 5/25/08| 9/26/09| GENPUMP|29.683147|-91.14545|
+----------+---------+------------+-----------+--------+---------+---------+
only showing top 5 rows
```

#### 5.9.1.4 HBase表格

传感器日志流数据的HBase表格模式如下：

（1）Row key：（resid + date + time）的复合行键

（2）data列族：包括与输入数据字段相对应的列，

（3）alert列族：具有对应报警值的列。请注意，data和alert列族可能会设置为在一段时间后过期。

每日统计汇总的HBase表格模式如下：

（1）Row key：（resid + date）的复合行键

（2）stats列族：最小值、最大值和平均值的列。

![https://www.mapr.com/sites/default/files/blogimages/sparkstream5-blog.png](../media/05_stream_processing/media/image16.jpeg)

图例 5‑16数据格式

创建HBase表：

hbase(main):001:0\> create 'sensor', {NAME=\>'data'}, {NAME=\>'alert'},
{NAME=\>'stats'}

Created table sensor

Took 1.2132 seconds

\=\> Hbase::Table - sensor

hbase(main):002:0\> list

TABLE

sensor

1 row(s)

Took 0.0462 seconds

\=\> \["sensor"\]

hbase(main):004:0\> scan 'sensor'

ROW COLUMN+CELL

0 row(s)

Took 0.1649 seconds

### 5.9.2 创建数据流

传感器数据来自逗号分隔的CSV文件，将其保存到一个目录中，Spark
Streaming将监视目录并处理添加到该目录中的任何文件。如前所述，Spark
数据流支持不同的流式数据源，为简单起见此示例将使用CSV文件。

下面的函数将Sensor对象转换为HBase Put对象，该对象用于将行插入到HBase中。可以使用Spark
的TableOutputFormat类写入HBase表，这与从MapReduce写入HBase表的方式类似。下面使用TableOutputFormat类设置写入HBase的配置，将通过示例应用程序代码完成这些步骤。首先使用Scala案例类定义与传感器数据CSV文件对应的Sensor模式：

case class Sensor(resid: String, date: String, time: String, hz: Double,
disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)
extends Serializable

代码 5‑35

parseSensor方法解析CSV文件，根据逗号分隔符提取出数值，用其定义Sensor类。

def parseSensor(str: String): Sensor = {

val p = str.split(",")

Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble,
p(6).toDouble, p(7).toDouble, p(8).toDouble)

}

代码 5‑36

本小节继续沿用上面的历史案例，因此输入源仍采用目录文件 + `textFileStream`，写出仍以 HBase 为目标。这种写法便于演示 DStream 的微批处理链路，但在 Spark 4.x 的新系统中，更常见的做法是使用 Structured Streaming 从 Kafka、对象存储或湖仓增量源读取数据，再通过 DataFrame API 完成转换与输出。

这一部分对应历史案例里的“创建输入流”步骤，代码链路本身并不复杂，核心可以概括为三步：

  - 首先初始化一个Spark的StreamingContext 对象。

  - 使用StreamingContext对象创建一个离散流，用来表示为输入数据流，在DStream对象上应用转换和输出操作。

  - 然后可以开始接收数据，并使用StreamingContext.start()处理它。

  - 最后等待使用streamingContext.awaitTermination()停止处理

通过下面的代码显示这些步骤，Spark数据流应用的最佳运行方案是通过使用Maven或SBT构建独立的应用程序。第一步是建立一个StreamingContext，这是用于流功能的主入口点，在这个例子中将使用2秒批次时间间隔。

val sparkConf = new SparkConf().setAppName("HBaseStream")

val ssc = new StreamingContext(sparkConf, Seconds(2))

val linesDStream = ssc.textFileStream("/root/data/stream")

val sensorDStream = linesDStream.map(Sensor.parseSensor)

代码 5‑37

可以创建表示源数据的离散流linesDStream。在这个例子中，使用的StreamingContext.textFileStream()方法来创建输入流，来监视与Hadoop兼容文件系统的新文件，并处理在该目录中创建的任何文件。

![](../media/05_stream_processing/media/image17.jpeg)

图例 5‑17创建输入流

这种摄取类型支持将新文件写入目录的工作流程，并使用Spark
Streaming检测它们，提取并处理数据，这种摄取类型只能将文件移动或复制到目录中使用。linesDStream表示传入的数据流，其中每个记录是一行文本流。一个离散流的内部是RDD的序列，每个RDD之间时间间隔2秒。接下来，将解析数据行为Sensor对象，使用linesDStream.map()操作，map()操作在RDD上应用parseSensor()方法，产生包含Sensor对象的RDD。

施加在离散流上的任何操作，被转移为对底层RDD的操作，对linesDStream上每个RDD的map()操作，将产生sensorDStream中的每个RDD，接下来使用Dstream.foreachRDD()方法来应用处理在离散流上的RDD。对低PSI的传感器对象进行过滤，以创建一个警报传感器对象的RDD，然后使用convertToPut()和convertToPutAlert()将Sensor数据转换给HBase
Put对象。

sensorDStream.foreachRDD { rdd =\>

// 过滤传感器低psi的数据

val alertRDD = rdd.filter(sensor =\> sensor.psi \< 5.0)

alertRDD.take(1).foreach(println)

// 将传感器数据转换成put对象，写入HBase表的列族中

rdd.map(Sensor.convertToPut).

saveAsHadoopDataset(jobConfig)

alertRDD.map(Sensor.convertToPutAlert).

saveAsHadoopDataset(jobConfig)

}

代码 5‑38

要开始接收数据时，必须明确地调用StreamingContext.start()方法，然后调用awaitTermination()方法，等待流计算完成。

println("start streaming")

ssc.start()

ssc.awaitTermination()

代码 5‑39

接下来就是把处理后的流数据写入 HBase。这里会先把数据组织成便于查询和检查的结构，再通过 `convertToPut` 把 `Sensor` 对象转换成 HBase 所需的 `Put` 对象，作为最终写入动作的输入。

def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {

val dateTime = sensor.date + " " + sensor.time

// 创建一个组合行键: sensorid\_date time

val rowkey = sensor.resid + "\_" + dateTime

val put = new Put(Bytes.toBytes(rowkey))

// 增加列族数据

put.addColumn(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))

put.addColumn(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))

put.addColumn(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))

put.addColumn(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))

put.addColumn(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))

put.addColumn(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))

return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)

}

代码 5‑40

接下来使用PairRDDFunctions.saveAsHadoopDataset()方法写入传感器和警报数据。

![处输入图片的描述](../media/05_stream_processing/media/image18.jpeg)

图例 5‑18使用 saveAsHadoopDataset 方法写入到HBase中

这将使用该存储系统的Hadoop Configuration对象将RDD输出到任何Hadoop支持的存储系统上，将sensorRDD
对象转换为Put对象，然后使用
saveAsHadoopDataset()方法写入到HBase中。现在要读取HBase传感器表数据，然后计算每日摘要统计信息并将这些统计信息写入统计信息列族，以下代码读取HBase表传感器表psi列数据，使用StatCounter计算此数据的统计数据，然后将统计数据写入传感器统计数据列系列。

val conf = HBaseConfiguration.create()

conf.set(TableInputFormat.INPUT\_TABLE, HBaseSensorStream.tableName)

//读取列族psi列中的数据

conf.set(TableInputFormat.SCAN\_COLUMNS, "data:psi")

//加载(row key,row Result)RDD元组

val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf\[TableInputFormat\],

classOf\[org.apache.hadoop.hbase.io.ImmutableBytesWritable\],

classOf\[org.apache.hadoop.hbase.client.Result\])

//转换(row key,row Result)元组为resultRDD

val resultRDD = hBaseRDD.map(tuple =\> tuple.\_2)

val keyValueRDD = resultRDD.

map(result =\> (Bytes.toString(result.getRow()).

split(" ")(0), Bytes.toDouble(result.value)))

// 通过rowkey分组,得到列值的统计

val keyStatsRDD = keyValueRDD.

groupByKey().

mapValues(list =\> StatCounter(list))

keyStatsRDD.map { case (k, v) =\> convertToPut(k, v)
}.saveAsHadoopDataset(jobConfig)

代码 5‑41

newAPIHadoopRDD()的输出是键值对RDD，PairRDDFunctions.saveAsHadoopDataset()方法将Put对象保存到HBase。现在，让我们看一看代码运行步骤和输出结果。

步骤1：启动流媒体应用

spark-submit --class HBaseSensorStream
/data/application/sensor-streaming/target/scala-2.13/sensor-streaming-assembly-0.1.jar

代码 5‑42

步骤2：将流数据文件复制到流目录

cp /data/sensordata.csv /root/data/stream/

代码 5‑43

步骤3：我们可以扫描写入表的数据，但是无法从shell界面读取二进制double值。启动hbase
shell命令，扫描data列族和alert列族

hbase(main):007:0\> scan 'sensor', {COLUMNS=\>\['data'\], LIMIT =\> 1}

ROW COLUMN+CELL

ANDOUILLE\_3/10/14 10:01 column=data:chlPPM, timestamp=1586161685698,
value=?\\xF7\\x0A=p\\xA3\\xD7\\x0A

ANDOUILLE\_3/10/14 10:01 column=data:disp, timestamp=1586161685698,
value=?\\xFB|\\xED\\x91hr\\xB0

ANDOUILLE\_3/10/14 10:01 column=data:flo, timestamp=1586161685698,
value=@\\x93\\xEC\\x00\\x00\\x00\\x00\\x00

ANDOUILLE\_3/10/14 10:01 column=data:hz, timestamp=1586161685698,
value=@\#\\xA3\\xD7\\x0A=p\\xA4

ANDOUILLE\_3/10/14 10:01 column=data:psi, timestamp=1586161685698,
value=@S\\x00\\x00\\x00\\x00\\x00\\x00

ANDOUILLE\_3/10/14 10:01 column=data:sedPPM, timestamp=1586161685698,
value=?\\xD3333333

1 row(s)

Took 0.0186 seconds

hbase(main):006:0\> scan 'sensor', {COLUMNS=\>\['alert'\], LIMIT =\> 2}

ROW COLUMN+CELL

LAGNAPPE\_3/14/14 19:39 column=alert:psi, timestamp=1586161686313,
value=\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00

LAGNAPPE\_3/14/14 19:41 column=alert:psi, timestamp=1586161686313,
value=\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00

2 row(s)

代码 5‑44

步骤4：启动以下程序之一以读取数据并计算每日统计数据

（1）计算一列的统计信息

root@48feaa001420:\~\# spark-submit --class HBaseReadWrite
/data/application/sensor-streaming/target/scala-2.13/sensor-streaming-assembly-0.1.jar

20/04/06 13:35:19 WARN NativeCodeLoader: Unable to load native-hadoop
library for your platform... using builtin-java classes where applicable

(COHUTTA\_3/10/14,95.0)

(COHUTTA\_3/10/14,88.0)

(COHUTTA\_3/10/14,(count: 958, mean: 87.586639, stdev: 7.309181, max:
100.000000, min: 75.000000))

代码 5‑45

（2）计算整列的统计信息

root@48feaa001420:\~\# spark-submit --class HBaseReadRowWriteStats
/data/application/sensor-streaming/target/scala-2.13/sensor-streaming-assembly-0.1.jar

20/04/06 13:37:56 WARN NativeCodeLoader: Unable to load native-hadoop
library for your platform... using builtin-java classes where applicable

root

|-- rowkey: string (nullable = true)

|-- hz: double (nullable = false)

|-- disp: double (nullable = false)

|-- flo: double (nullable = false)

|-- sedPPM: double (nullable = false)

|-- psi: double (nullable = false)

|-- chlPPM: double (nullable = false)

\+-----------------+----+-----+------+------+----+------+

| rowkey| hz| disp| flo|sedPPM| psi|chlPPM|

\+-----------------+----+-----+------+------+----+------+

|ANDOUILLE\_3/10/14|9.82|1.718|1275.0| 0.3|76.0| 1.44|

|ANDOUILLE\_3/10/14|9.88|1.716|1273.0| 0.1|80.0| 0.89|

\+-----------------+----+-----+------+------+----+------+

only showing top 2 rows

`[MOJO\_3/10/14,87.20876826722338]`

`[CARGO\_3/11/14,87.2901878914405]`

root

|-- rowkey: string (nullable = true)

|-- maxhz: double (nullable = true)

|-- minhz: double (nullable = true)

|-- avghz: double (nullable = true)

|-- maxdisp: double (nullable = true)

|-- mindisp: double (nullable = true)

|-- avgdisp: double (nullable = true)

|-- maxflo: double (nullable = true)

|-- minflo: double (nullable = true)

|-- avgflo: double (nullable = true)

|-- maxsedPPM: double (nullable = true)

|-- minsedPPM: double (nullable = true)

|-- avgsedPPM: double (nullable = true)

|-- maxpsi: double (nullable = true)

|-- minpsi: double (nullable = true)

|-- avgpsi: double (nullable = true)

|-- maxchlPPM: double (nullable = true)

|-- minchlPPM: double (nullable = true)

|-- avgchlPPM: double (nullable = true)

`[MOJO\_3/10/14,10.5,9.5,9.999457202505226,3.345,1.828,2.6188089770354934,1770.0,967.0,1385.8131524008352,2.0,0.0,0.9798121085594999,100.0,75.0,87.20876826722338,2.0,0.5,1.2699686847599168]`

`[CARGO\_3/11/14,10.5,9.5,10.010824634655517,3.864,1.983,2.948458246346556,1579.0,810.0,1204.7265135699374,2.0,0.0,0.9811482254697279,100.0,75.0,87.2901878914405,2.0,0.5,1.2506784968684743]`

SensorStatsRow(MOJO\_3/10/14,10.5,9.5,9.999457202505226,3.345,1.828,2.6188089770354934,1770.0,967.0,1385.8131524008352,2.0,0.0,0.9798121085594999,100.0,75.0,87.20876826722338,2.0,0.5,1.2699686847599168)

SensorStatsRow(CARGO\_3/11/14,10.5,9.5,10.010824634655517,3.864,1.983,2.948458246346556,1579.0,810.0,1204.7265135699374,2.0,0.0,0.9811482254697279,100.0,75.0,87.2901878914405,2.0,0.5,1.2506784968684743)

代码 5‑46

（3）启动HBase shell并扫描统计信息

hbase(main):002:0\> scan 'sensor' , {COLUMNS=\>\['stats'\], LIMIT =\> 1}

ROW COLUMN+CELL

ANDOUILLE\_3/10/14 column=stats:chlPPMmax, timestamp=1586180290366,
value=@\\x00\\x00\\x00\\x00\\x00\\x00\\x00

ANDOUILLE\_3/10/14 column=stats:chlPPMmin, timestamp=1586180290366,
value=?\\xE0\\x00\\x00\\x00\\x00\\x00\\x00

ANDOUILLE\_3/10/14 column=stats:dispavg, timestamp=1586180290366,
value=?\\xF9\\x83KY3\\x88\\x8D

ANDOUILLE\_3/10/14 column=stats:dispmax, timestamp=1586180290366,
value=@\\x00\\xE7l\\x8BC\\x95\\x81

ANDOUILLE\_3/10/14 column=stats:dispmin,

代码 5‑47

### 5.9.3 转换操作

本节将学习如何在离散流上应用操作，现在有了输入数据流想回答一些问题，例如：

  - 产生低压警报传感器的生产厂家和维护信息是什么？

为了回答这些问题，将在刚刚创建的离散流滤警报数据，并与供应商和维护信息进行连接操作，这些信息在产生数据流之前被读入并缓存。每个RDD被转换成DataFrame，并注册为一个临时表，然后使用SQL查询，下面的查询回答的第一个问题。

val pumpRDD =
sc.textFile("/root/data/sensorvendor.csv").map(parsePumpInfo)

val maintRDD = sc.textFile("/root/data/sensormaint.csv").map(parseMaint)

val maintDF = maintRDD.toDF()

val pumpDF = pumpRDD.toDF()

maintDF.createOrReplaceTempView ("maint")

pumpDF.createOrReplaceTempView ("pump")

sensorDStream.foreachRDD(rdd =\> {

rdd.filter { sensor =\> sensor.psi \< 5.0
}.toDF.registerTempTable("alert")

val alertPumpMaint = sqlContext.sql("select
a.resid,a.date,a.psi,p.pumpType,p.vendor,m.date,m.technician from alert
a join pump p on a.resid = p.resid join maint m on p.resid = m.resid")

alertPumpMaint.show()

})

代码 5‑48

Spark
Streaming提供了一组关于离散流的转换，这些转换类似于RDD上的转换，包括map()、flatMap()、filter()、join()和reduceByKey()等等。Spark
Streaming还提供诸如reduce()和count()等运算符，这些运算符返回由单一元素组成的离散流，但是在不同于RDD的reduce()和count()运算符，这些不触发离散流上的实际计算，他们不是动作，而是定义另一个离散流。有状态的转换可以跨越批次保持状态，使用数据或来自先前批的中间结果以计算当前批次的结果，包括基于滑动窗口的转换和跨越时间的跟踪状态。流式转换应用于在离散流中的每个RDD，依次施加转换到RDD的元素。动作是输出运算符，调用时在
离散流上触发计算，他们包括：

（1）print()将每个批次的前10个元素打印到控制台，通常用于调试和测试。

（2）saveAsObjectFile()、saveAsTextFiles()和saveAsHadoopFiles()函数将数据流输出为Hadoop兼容的文件格式。

（2）foreachRDD()运算符应用到离散流的每个批次内的RDD上。

现在，让我们看一看代码运行步骤和输出结果。

root@48feaa001420:\~\# spark-submit --class SensorStreamSQL
/data/application/sensor-streaming/target/scala-2.13/sensor-streaming-assembly-0.1.jar

20/04/06 14:35:26 WARN NativeCodeLoader: Unable to load native-hadoop
library for your platform... using builtin-java classes where applicable

Starting streaming process

Low pressure alert

Sensor(NANTAHALLA,3/13/14,2:05,0.0,0.0,0.0,1.73,0.0,1.51)

Alert pump maintenance data

\+----------+-------+---+---------+------------+-----------+--------+---------+----------+-----------+

| resid| date|psi| pumpType|purchaseDate|serviceDate|
vendor|eventDate|technician|description|

\+----------+-------+---+---------+------------+-----------+--------+---------+----------+-----------+

|NANTAHALLA|3/13/14|0.0|HYDROPUMP| 11/27/10| 3/15/11|HYDROCAM| 3/15/11|
J.Thomas| Install|

\+----------+-------+---+---------+------------+-----------+--------+---------+----------+-----------+

only showing top 1 row

代码 5‑49

### 5.9.4 窗口操作

通过窗口操作，可以在数据的滑动窗口上应用转换，可以多批次合并结果，在StreamingContext中指定的时间间隔进行计算。

![](../media/05_stream_processing/media/image19.jpeg)

图例 5‑19数据的滑动窗口

在图例 5‑19中，Original DStream以一秒的间隔进入。滑动窗口的长度由window
length指定，在这种情况下为3个单位，窗口在离散流上按照指定的滑动间隔进行滑动，在这种情况下是2个单元。窗口长度和滑动间隔必须是离散流批次间隔的倍数，当前为1秒。当窗口在离散流上滑动时，所有落在该窗口中RDD被组合，该操作被应用于组合的RDD上，产生了窗口流中的RDD，所有窗口操作都需要两个参数：

  - 窗口长度是指窗口的持续时间，在此示例中窗口长度为3个单位。

  - 滑动间隔是指操作窗口执行的间隔，在此例子中滑动间隔是2个单位。

再次，这两个参数必须是离散流的批次间隔的倍数。例如要每4秒生成单词计数，并且持续6秒的数据，应用reduceByKey操作在键值对离散流上，使用reduceByKeyAndWindow窗口操作设置窗口长度为6，滑动间隔为4：

val windowsWordCounts = pairs.reduceByKeyAndWindow(a:Int,b:Int)=\>(a+b),
Seconds(6),Seconds(4))

代码 5‑50

使用窗口操作回答一下两个问题：

  - 传感器事件计数是多少？

  - 什么是最大，最小和平均的psi？

为了回答这些问题，将在窗口流上使用操作。每2秒的时间间隔使用持续6秒的窗口数据流回答上述的两个问题：

sensorDStream.window(Seconds(6), Seconds(2))

.foreachRDD { rdd =\>

if (\!rdd.partitions.isEmpty) {

val sensorDF = rdd.toDF()

println("sensor data")

sensorDF.show()

sensorDF.createOrReplaceTempView("sensor")

val res = spark.sql("SELECT resid, date, count(resid) as total FROM
sensor GROUP BY resid, date")

println("sensor count ")

res.show

val res2 = spark.sql("SELECT resid, date, MAX(psi) as maxpsi, min(psi)
as minpsi, avg(psi) as avgpsi FROM sensor GROUP BY resid,date")

println("sensor max, min, averages ")

res2.show

}

代码 5‑51

在代码 5‑47中，通过res可以回答第一个问题，通过res2的结果回答了什么是最大、最小和平均的psi，使用相同的窗口操作在每个传感器RDD上收集psi数据。现在，让我们看一看代码运行步骤和输出结果。

root@48feaa001420:\~\# spark-submit --class SensorStreamWindow
/data/application/sensor-streaming/target/scala-2.13/sensor-streaming-assembly-0.1.jar

20/04/06 14:35:29 WARN NativeCodeLoader: Unable to load native-hadoop
library for your platform... using builtin-java classes where applicable

Starting streaming process

Sensor count

\+-----+-------+-----+

|resid| date|total|

\+-----+-------+-----+

| CHER|3/10/14| 958|

\+-----+-------+-----+

only showing top 1 row

Sensor max, min, averages

\+-----+-------+------+------+-----------------+

|resid| date|maxpsi|minpsi| avgpsi|

\+-----+-------+------+------+-----------------+

| CHER|3/10/14| 100.0| 75.0|87.44885177453027|

\+-----+-------+------+------+-----------------+

only showing top 1 row

Sensor count

\+-----+-------+-----+

|resid| date|total|

\+-----+-------+-----+

| CHER|3/10/14| 958|

\+-----+-------+-----+

only showing top 1 row

Sensor max, min, averages

\+-----+-------+------+------+-----------------+

|resid| date|maxpsi|minpsi| avgpsi|

\+-----+-------+------+------+-----------------+

| CHER|3/10/14| 100.0| 75.0|87.44885177453027|

\+-----+-------+------+------+-----------------+

only showing top 1 row

Sensor count

\+-----+-------+-----+

|resid| date|total|

\+-----+-------+-----+

| CHER|3/10/14| 958|

\+-----+-------+-----+

only showing top 1 row

Sensor max, min, averages

\+-----+-------+------+------+-----------------+

|resid| date|maxpsi|minpsi| avgpsi|

\+-----+-------+------+------+-----------------+

| CHER|3/10/14| 100.0| 75.0|87.44885177453027|

\+-----+-------+------+------+-----------------+

only showing top 1 row

代码 5‑52

  - 当什么情况下窗口操作是非常有用的？
