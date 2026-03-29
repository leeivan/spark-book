# 10.3 SparkContext与SparkSession

在Spark 4.x中，应用程序应优先以SparkSession作为统一入口。SparkSession封装了创建与访问SQL、DataFrame/Dataset、Structured Streaming所需的核心上下文，能够显著简化工程代码结构。

历史上（Spark 2.0之前）常见SQLContext/HiveContext写法仅用于理解演进，不再建议作为新项目模板。

  - 注意

SparkSession已将SQLContext和HiveContext在Spark 2.0之后统一为单一入口对象。

可以使用SparkSession.builder方法创建SparkSession的实例。

import org.apache.spark.sql.SparkSession

val spark = SparkSession

.builder()

.appName("Spark SQL basic example")

.config("spark.some.config.option", "some-value")

.getOrCreate()

代码 10.1
完成后可以调用 `stop()` 方法关闭当前的 SparkSession。

spark.stop

代码 10.2
正如在以前的Spark版本，spark-shell创建了一个SparkContext，变量名为sc。在Spark
2.0之后，spark-shell会创建一个SparkSession，变量名为spark。在这个spark-shell中，可以看到spark已经存在，并且可以查看它的所有属性。

root@3997e0349ac9:\~\# spark-shell

Spark context Web UI available at http://172.17.0.2:4040

Spark context available as 'sc' (master = local\[\*\], app id =
local-1525959700559).

Spark session available as 'spark'.

Welcome to

\_\_\_\_ \_\_

/ \_\_/\_\_ \_\_\_ \_\_\_\_\_/ /\_\_

\_\\ \\/ \_ \\/ \_ \`/ \_\_/ '\_/

/\_\_\_/ .\_\_/\\\_,\_/\_/ /\_/\\\_\\ version 4.1.1

/\_/

Using Scala version 2.13.16 (OpenJDK 64-Bit Server VM, Java 17)

Type in expressions to have them evaluated.

Type :help for more information.

```scala
scala> spark
res0: org.apache.spark.sql.SparkSession =
<org.apache.spark.sql.SparkSession@47fbf95e>
scala> sc
res2: org.apache.spark.SparkContext =
<org.apache.spark.SparkContext@5e9af5d4>
```

代码 10.3
SparkSession封装了SparkContext。首先简单理解一下SparkContext的功能。

![Fig 7. SparkContext as it relates to Driver and Cluster
Manager](../media/10_running_applications/media/image1.png)

图例 10‑1SparkContext 与Driver和Cluster Manager的关系

如图所示（图例 10‑1），SparkContext是底层执行上下文；每个JVM通常只有一个SparkContext。Spark驱动程序（Driver
Program）通过它连接集群管理器（YARN，Kubernetes或Standalone）并提交作业。业务层代码建议通过SparkSession访问能力，在需要底层控制时再使用`spark.sparkContext`。

在Spark 4.x中，SparkSession已经是统一入口点：既可处理DataFrame/Dataset与SQL，也可衔接流处理与底层执行上下文。这样可以减少上下文对象切换带来的复杂度，降低出错概率。下面继续介绍SparkSession的类和实例方法。

  - builder(): Builder

> `builder()` 用来创建一个 Builder，再通过它获取或创建 SparkSession 实例。下面的示例适用于 Scala 应用程序；在 `spark-shell` 中，SparkSession 通常已经由系统自动创建。

import org.apache.spark.sql.SparkSession

val builder = SparkSession.builder

代码 10.4
  - version: String

> 返回当前Spark的版本。在内部，version使用spark.SPARK\_VERSION值，即CLASSPATH中spark-version-info.properties属性文件中的version属性。

```scala
scala> spark.version
res4: String = 4.1.1
```

代码 10.5
  - implicits

implicits对象是一个具有Scala隐式方法的帮助类，用于将Scala对象转换为Dataset、DataFrame和Column。它还定义了Scala原始类型的Encoder，例如Int、Double、String及其Product和Collection。

val spark = SparkSession.builder.getOrCreate()

import spark.implicits.\_

代码 10.6
implicits对象提供对从任何类型的RDD（Encoder所包括的）、case类或元组以及Seq创建Dataset，还提供从Scala的Symbol或$到Column的转换，还提供从Product类型（例如案例类或元组）的RDD或Seq到DataFrame的转换，具有从Int、Long和String的RDD到具有单个列名“\_1”的DataFrame直接转换。

  - 注意

> 只能在Int、Long和String原始类型的RDD对象上调用toDF方法。

  - def emptyDataset\[T\](implicit arg0: Encoder\[T\]): Dataset\[T\]

> 创建一个空的 Dataset\[T\]。当你已经确定记录类型，但暂时还没有任何数据时，这个方法很方便。

```scala
scala> val strings = spark.emptyDataset[String]
strings: org.apache.spark.sql.Dataset[String] = [value: string]
scala> strings.printSchema
root
|-- value: string (nullable = true)
```

代码 10.7
  - def range(end: Long): Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long): Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long, step: Long):
    Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long, step: Long, numPartitions: Int):
    Dataset\[java.lang.Long\]

> 创建一个Dataset\[Long\]。range的方法系列创建一个Long数据的数据集。

```scala
scala> spark.range(start = 0, end = 4, step = 2, numPartitions =
5).show
+---+
| id|
+---+
| 0|
| 2|
+---+
```

代码 10.8
  - 注意

第一个变体（不明确指定numPartitions）使用SparkContext.defaultParallelism来分配numPartitions。

  - def sql(sqlText: String): DataFrame

执行SQL查询（并返回DataFrame）。sql执行参数sqlText传递的SQL语句并创建一个DataFrame。

```scala
scala> sql("SHOW TABLES")
res0: org.apache.spark.sql.DataFrame = [tableName: string, isTemporary:
boolean]
scala> sql("DROP TABLE IF EXISTS testData")
res1: org.apache.spark.sql.DataFrame = []
// Let's create a table to SHOW it
spark.range(10).write.option("path",
"/tmp/test").saveAsTable("testData")
scala> sql("SHOW TABLES").show
+---------+-----------+
|tableName|isTemporary|
+---------+-----------+
| testdata| false|
+---------+-----------+
```

代码 10.9
  - def udf: UDFRegistration

访问用户定义的函数（UDF）。udf属性允许访问UDFRegistration，允许注册基于SQL查询的用户定义函数。

```scala
scala> spark.udf.register("myUpper", (s: String) => s.toUpperCase)
res6: org.apache.spark.sql.expressions.UserDefinedFunction =
UserDefinedFunction(<function1>,StringType,Some(List(StringType)))
scala> val strs = ('a' to 'c').map(_.toString).toDS
strs: org.apache.spark.sql.Dataset[String] = [value: string]
scala> strs.createOrReplaceTempView("strs")
scala> sql("SELECT *, myUpper(value) UPPER FROM strs").show
+-----+-----+
|value|UPPER|
+-----+-----+
| a| A|
| b| B|
| c| C|
+-----+-----+
```

代码 10.10
  - def table(tableName: String): DataFrame

从表创建DataFrame。将表加载为DataFrame，如果存在。

```scala
scala> spark.catalog.tableExists("strs")
res12: Boolean = true
scala> val t1 = spark.table("strs")
t1: org.apache.spark.sql.DataFrame = [value: string]
scala> t1.show
+-----+
|value|
+-----+
| a|
| b|
| c|
+-----+
```

代码 10.11
  - lazy val catalog: Catalog

访问结构化查询实体的元数据目录，catalog属性是当前元数据目录的查询接口，元数据目录包括关系实体，如数据库、表、函数、表列和临时视图。

```scala
scala> spark.catalog.listTables.show
+------------------+--------+-----------+---------+-----------+
| name|database|description|tableType|isTemporary|
+------------------+--------+-----------+---------+-----------+
|my_permanent_table| default| null| MANAGED| false|
| strs| null| null|TEMPORARY| true|
+------------------+--------+-----------+---------+-----------+
```

代码 10.12
  - def read: DataFrameReader

read方法返回一个DataFrameReader，用于从外部存储系统读取数据并将其加载到DataFrame。

val dfReader: DataFrameReader = spark.read

代码 10.13
  - lazy val conf: RuntimeConfig

访问当前的运行时配置。

  - def readStream: DataStreamReader

访问DataStreamReader以读取流数据集。

  - def streams: StreamingQueryManager

访问StreamingQueryManager以管理结构化流查询。

  - def newSession(): SparkSession

创建一个新的SparkSession 。

  - def stop(): Unit

停止SparkSession 。
