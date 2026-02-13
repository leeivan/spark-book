# Spark应用程序


## 本章先看懂什么
- 一个 Spark 应用从开发到上线的完整链路。
- 打包、提交、依赖管理和配置的关键点。
- 本地模式与集群模式的差异。

## 一个最小例子
需求：每天凌晨跑一次离线任务。
1. 用 `sbt/maven` 打包 fat jar。
2. 用 `spark-submit` 指定主类与参数。
3. 在 YARN/K8s 上定时触发。
4. 失败自动重试并记录日志。

工程化的核心是“可重复运行 + 可观测 + 可回滚”。

> **版本基线（更新于 2026-02-13）**
> 本书默认适配 Apache Spark 4.1.1（稳定版），并兼容 4.0.2 维护分支。
> 推荐环境：JDK 17+（建议 JDK 21）、Scala 2.13、Python 3.10+。
Apache Spark被广泛认为是MapReduce在Apache
Hadoop集群上进行通用数据处理的后继者。与MapReduce应用程序一样，每个Spark应用程序都是一个自包含的计算，它运行用户提供的代码来计算结果。与MapReduce作业一样，Spark应用程序可以使用多个主机的资源。但是，Spark比MapReduce有很多优点。

在MapReduce中，最高级别的计算单位是Job，用来加载数据、应用Map函数、Shuffle、应用Reduce函数，并将数据写回持久存储。在Spark中，最高级别的计算单位是应用程序。Spark应用程序有多种方式，其中包括可用于单个批处理作业，具有多个作业的交互式会话或持续满足请求的长期服务。Spark作业可以包含多个Map和Reduce。本章将学习怎样构建和部署单个批处理作业的独立应用程序。

## SparkContext与SparkSession

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

代码 4.2

并使用stop方法停止当前的SparkSession 。

spark.stop

代码 4.3

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

scala\> spark

res0: org.apache.spark.sql.SparkSession =
<org.apache.spark.sql.SparkSession@47fbf95e>

scala\> sc

res2: org.apache.spark.SparkContext =
<org.apache.spark.SparkContext@5e9af5d4>

代码 4.4

SparkSession封装了SparkContext。首先简单理解一下SparkContext的功能。

![Fig 7. SparkContext as it relates to Driver and Cluster
Manager](media/10_running_applications/media/image1.png)

图例 4‑1SparkContext 与Driver和Cluster Manager的关系

如图所示（图例
4‑1），SparkContext是底层执行上下文；每个JVM通常只有一个SparkContext。Spark驱动程序（Driver
Program）通过它连接集群管理器（YARN，Kubernetes或Standalone）并提交作业。业务层代码建议通过SparkSession访问能力，在需要底层控制时再使用`spark.sparkContext`。

在Spark 4.x中，SparkSession已经是统一入口点：既可处理DataFrame/Dataset与SQL，也可衔接流处理与底层执行上下文。这样可以减少上下文对象切换带来的复杂度，降低出错概率。下面继续介绍SparkSession的类和实例方法。

  - builder(): Builder

> 创建一个Builder来获取或创建一个SparkSession实例，builder()创建一个新的Builder，可以使用SparkSession
> API构建完整配置的SparkSession。下面代码是在Scala应用程序中的方法，在Scala交互界面不需要创建SparkSession。

import org.apache.spark.sql.SparkSession

val builder = SparkSession.builder

代码 4.5

  - version: String

> 返回当前Spark的版本。在内部，version使用spark.SPARK\_VERSION值，即CLASSPATH中spark-version-info.properties属性文件中的version属性。

scala\> spark.version

res4: String = 4.1.1

代码 4.6

  - implicits

implicits对象是一个具有Scala隐式方法的帮助类，用于将Scala对象转换为Dataset、DataFrame和Column。它还定义了Scala原始类型的Encoder，例如Int、Double、String及其Product和Collection。

val spark = SparkSession.builder.getOrCreate()

import spark.implicits.\_

代码 4.7

implicits对象提供对从任何类型的RDD（Encoder所包括的）、case类或元组以及Seq创建Dataset，还提供从Scala的Symbol或$到Column的转换，还提供从Product类型（例如案例类或元组）的RDD或Seq到DataFrame的转换，具有从Int、Long和String的RDD到具有单个列名“\_1”的DataFrame直接转换。

  - 注意

> 只能在Int、Long和String原始类型的RDD对象上调用toDF方法。

  - def emptyDataset\[T\](implicit arg0: Encoder\[T\]): Dataset\[T\]

> 创建一个空Dataset\[T\]。emptyDataset创建一个空数据集，假设将来的记录是类型T。

scala\> val strings = spark.emptyDataset\[String\]

strings: org.apache.spark.sql.Dataset\[String\] = \[value: string\]

scala\> strings.printSchema

root

|-- value: string (nullable = true)

代码 4.8

  - def range(end: Long): Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long): Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long, step: Long):
    Dataset\[java.lang.Long\]

  - def range(start: Long, end: Long, step: Long, numPartitions: Int):
    Dataset\[java.lang.Long\]

> 创建一个Dataset\[Long\]。range的方法系列创建一个Long数据的数据集。

scala\> spark.range(start = 0, end = 4, step = 2, numPartitions =
5).show

\+---+

| id|

\+---+

| 0|

| 2|

\+---+

代码 4.9

  - 注意

第一个变体（不明确指定numPartitions）使用SparkContext.defaultParallelism来分配numPartitions。

  - def sql(sqlText: String): DataFrame

执行SQL查询（并返回DataFrame）。sql执行参数sqlText传递的SQL语句并创建一个DataFrame。

scala\> sql("SHOW TABLES")

res0: org.apache.spark.sql.DataFrame = \[tableName: string, isTemporary:
boolean\]

scala\> sql("DROP TABLE IF EXISTS testData")

res1: org.apache.spark.sql.DataFrame = \[\]

// Let's create a table to SHOW it

spark.range(10).write.option("path",
"/tmp/test").saveAsTable("testData")

scala\> sql("SHOW TABLES").show

\+---------+-----------+

|tableName|isTemporary|

\+---------+-----------+

| testdata| false|

\+---------+-----------+

代码 4.10

  - def udf: UDFRegistration

访问用户定义的函数（UDF）。udf属性允许访问UDFRegistration，允许注册基于SQL查询的用户定义函数。

scala\> spark.udf.register("myUpper", (s: String) =\> s.toUpperCase)

res6: org.apache.spark.sql.expressions.UserDefinedFunction =
UserDefinedFunction(\<function1\>,StringType,Some(List(StringType)))

scala\> val strs = ('a' to 'c').map(\_.toString).toDS

strs: org.apache.spark.sql.Dataset\[String\] = \[value: string\]

scala\> strs.createOrReplaceTempView("strs")

scala\> sql("SELECT \*, myUpper(value) UPPER FROM strs").show

\+-----+-----+

|value|UPPER|

\+-----+-----+

| a| A|

| b| B|

| c| C|

\+-----+-----+

代码 4.11

  - def table(tableName: String): DataFrame

从表创建DataFrame。将表加载为DataFrame，如果存在。

scala\> spark.catalog.tableExists("strs")

res12: Boolean = true

scala\> val t1 = spark.table("strs")

t1: org.apache.spark.sql.DataFrame = \[value: string\]

scala\> t1.show

\+-----+

|value|

\+-----+

| a|

| b|

| c|

\+-----+

代码 4.12

  - lazy val catalog: Catalog

访问结构化查询实体的元数据目录，catalog属性是当前元数据目录的查询接口，元数据目录包括关系实体，如数据库、表、函数、表列和临时视图。

scala\> spark.catalog.listTables.show

\+------------------+--------+-----------+---------+-----------+

| name|database|description|tableType|isTemporary|

\+------------------+--------+-----------+---------+-----------+

|my\_permanent\_table| default| null| MANAGED| false|

| strs| null| null|TEMPORARY| true|

\+------------------+--------+-----------+---------+-----------+

代码 4.13

  - def read: DataFrameReader

read方法返回一个DataFrameReader，用于从外部存储系统读取数据并将其加载到DataFrame。

val dfReader: DataFrameReader = spark.read

代码 4.14

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

## 构建应用

假设希望使用Spark API编写一个独立的应用程序，将在Scala中运行一个简单的应用程序，代码如下：

/\* SimpleApp.scala \*/

import org.apache.spark.sql.SparkSession

object SimpleApp {

def main(args: Array\[String\]) {

val logFile = "YOUR\_SPARK\_HOME/README.md" // Should be some file on
your system

val spark = SparkSession.builder.appName("Simple
Application").getOrCreate()

val logData = spark.read.textFile(logFile).cache()

val numAs = logData.filter(line =\> line.contains("a")).count()

val numBs = logData.filter(line =\> line.contains("b")).count()

println(s"Lines with a: $numAs, Lines with b: $numBs")

spark.stop()

}

}

代码 4.1

  - 注意

Scala应用程序应该定义一个main()方法，而不是扩展scala.App，scala.App子类可能无法正常工作。

该程序仅计算README文件中包含“a”的行数和包含“b”的行数。注意，需要将YOUR\_SPARK\_HOME替换为安装Spark的位置。与前几章的具有Spark交互界面的示例不同，Spark交互界面会初始化自己的SparkSession，而Spark应用程序需要初始化一个SparkSession作为程序的一部分。调用SparkSession.builder构造一个SparkSession，然后设置Spark应用程序名称（可以在后面的介绍的Spark监控界面上找到），最后调用getOrCreate获取SparkSession实例spark。

应用程序依赖于Spark
API，所以还将包括一个sbt配置文件build.sbt，它解释Spark编译过程需要的依赖关系，在该文件中添加了Spark依赖的类库：

name := "Simple Project"

version := "1.0"

scalaVersion := "2.13.16"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1"

代码 4.2

要使sbt正常工作，需要根据典型的目录结构布局build.sbt和SimpleApp.scala。一旦构建完成，就可以创建一个包含应用程序代码的jar包，然后使用spark-submit脚本运行Spark程序。

SBT是Simple Build
Tool的简称，如果读者使用过Maven，那么可以简单将SBT看做是Scala世界的Maven，虽然二者各有优劣，但完成的工作基本是类似的。虽然Maven同样可以管理Scala项目的依赖并进行构建，但SBT具有某些特性，比如：

（1）使用Scala作为DSL来定义build文件

（2）通过触发执行特性支持持续的编译与测试

（3）增量编译

（4）可以混合构建Java和Scala项目

（5）并行的任务执行

（6）可以重用Maven或者ivy的repository进行依赖管理

SBT的发展可以分为两个阶段，即SBT\_0.7.x时代以及SBT\_0.10.x以后的时代。目前来讲，SBT\_0.7.x已经很少使用，大部分公司和项目都已经迁移到0.10.x以后的版本上来，最新的是1.1.4版本，0.10.x之后的版本build定义采用了新的设置系统。SBT已成为事实上的构建Scala应用程序的默认工具，SBT使用Apache
Ivy内部管理库依赖性，如果正在编写一个纯Scala
Spark应用程序，或者具有Java和Scala代码的混合代码库，则最有可能从采用SBT中获益。

### sbt

#### 项目结构

如果使用sbt 0.13.13或更高版本，则可以使用sbt new命令快速设置简单的Hello World构建，键入以下命令到终端。

$ sbt new sbt/scala-seed.g8

....

Minimum Scala build.

name \[My Something Project\]: hello

Template applied in ./hello

代码 4.3

当提示输入项目名称时，输入hello。这将在名为hello目录下创建一个新项目。现在从hello目录中，启动sbt并在sbt
shell中输入run。在Linux或OS X上，这些命令可能如下所示：

$ cd hello

$ sbt

...

\> run

...

\[info\] Compiling 1 Scala source to
/xxx/hello/target/scala-2.12/classes...

\[info\] Running example.Hello

Hello

命令 4.1

要离开sbt shell，请键入exit或使用Ctrl + D（Unix）或Ctrl + Z（Windows）。

\> exit

命令 4.2

在sbt的术语中，基础目录是包含项目的目录，因此如果创建了一个包含hello/build.sbt的项目hello，如上面示例中所示，hello就是基本目录。像Maven一样，SBT使用标准的项目目录结构。首先要构建一下相对简单的项目，SBT期望一个看起来像这样的结构：

build.sbt

lib/

project/

src/

main/

resources/

\<files to include in main jar here\>

scala/

\<main Scala sources\>

java/

\<main Java sources\>

test/

resources

\<files to include in test jar here\>

scala/

\<test Scala sources\>

java/

\<test Java sources\>

其中包含以下重要路径和文件：

  - build.sbt

此文件包含有关该项目的重要属性。构建定义在项目基本目录的build.sbt文件中（实际上是任何名为\*.sbt的文件）描述。build.sbt文件是一个基于Scala的文件，其中包含有关该项目的属性。早期版本的SBT需要文件两行间隔，但是这个限制已经在较新的版本中被删除了。libraryDependencies设置的语法将在下一节中介绍。

name := "BuildingSBT"

version := "1.0"

scalaVersion := "2.13.16"

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

代码 4.7

  - lib/

此目录包含在本地下载的任何非托管库依赖项。

  - project

已经在项目的基础目录中看到了build.sbt。其他的sbt
文件在project子目录中。project目录可以包含.scala文件，这些文件最后会和.sbt文件合并共同构成完整的构建定义。除build.sbt之外，project目录还可以包含.scala文件用来定义助手对象和一次性插件。我们可能会在项目中看到.sbt文件，但不等同于项目基本目录中的.sbt文件。build.properties文件控制SBT的版本。不同的项目可以在同一开发环境中使用不同版本的工具。build.properties文件始终包含一个标识SBT版本的属性，例子中该版本是0.13.15。

\# SBT Properties File: controls version of sbt tool

sbt.version=0.13.15

代码 4.8

什么是插件？插件继承了构建定义，大多数通常是添加设置，新的设置可以是新的任务。例如，一个插件可以添加一个codeCoverage任务来生成一个测试覆盖率报告。如果在hello目录下，而且正在往构建定义中添加一个sbt-site插件，创建hello/project/site.sbt并且通过传递插件的Ivy模块ID声明插件依赖给addSbtPlugin：

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.7.0")

代码 4.9

如果添加sbt-assembly，像下面这样创建 hello/project/assembly.sbt：

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

代码 4.10

不是所有的插件都在同一个默认的仓库中，而且一个插件的文档会指导添加能够找到它的仓库：

resolvers += Resolver.sonatypeRepo("public")

代码 4.11

插件通常提供设置将它添加到项目并且开启插件功能。

  - src

sbt默认使用与Maven相同的目录结构放置源文件，所有路径都相对于基本目录。在src/中的其他目录将被忽略。另外，所有隐藏的目录都将被忽略。源代码可以放在项目的基本目录中hello/app.scala，这可能适用于小型项目，但对于普通项目，人们倾向于将项目放在src/main/目录中以保持整洁。src/main/java这个目录是SBT希望放Java源代码的地方；src/main/scala这个目录是SBT放Scala源代码的地方。

  - target

此目录是SBT放置编译的类和jar文件的位置。

最后，有两个非常简单的Spark应用程序（在Java和Scala中）用于演示SBT。每个应用程序都依赖于Apache Commons
CSV库，因此可以演示SBT如何处理依赖关系。

/\*\*

\* A simple Scala application with an external dependency to

\* demonstrate building and submitting as well as creating an

\* assembly jar.

\*/

object SBuilding{

def main(args: Array\[String\]) {

val spark = SparkSession.builder.appName("SBuilding").getOrCreate()

// Create a simple RDD containing 4 numbers.

val numbers = Array(1, 2, 3, 4)

val numbersListRdd = spark.sparkContext.parallelize(numbers)

// Convert this RDD into CSV (using Java CSV Commons library).

val printer = new CSVPrinter(Console.out, CSVFormat.DEFAULT)

val javaArray: Array\[java.lang.Integer\] = numbersListRdd.collect() map
java.lang.Integer.valueOf

printer.printRecords(javaArray)

spark.stop()

}

}

代码 4.13 Scala

#### 编译集成

对于底层的实现，SBT使用Apache
Ivy从Maven2存储库下载依赖关系。可以在build.sbt文件中定义的依赖关系，格式为groupID
% artifactID % revision ，对于使用Maven的开发人员来说，这可能是熟悉的。在示例中，有3个依赖关系：Commons
CSV、Spark Core和Spark SQL：

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

代码 4.14

如果使用groupID %% artifactID % revision，而不是groupID % artifactID %
revision（区别是groupID后的双百分号），SBT将项目的Scala版本添加到artifactID，即spark-core\_2.13，这只是一种明确Scala版本的捷径方式。只能在Java中使用的依赖库应始终用单个百分比操作符（%）编写。如果不知道依赖库的groupID或artifactID，则可能会在该依赖项的网站或Maven
Central Repository中找到它们，用SBT构建的示例源代码。

cd /root/spark-app/building-sbt

sbt clean

sbt package

\# (This command may take a long time on its first run)

命令 4.3 SBT构建命令

package命令用于编译/src/main/中的源代码，并且创建一个没有任何依赖关系的项目代码的jar文件。在例子中的目录有Java和Scala应用程序，所以最终得到一个包含这两个应用程序的jar文件。SBT中还有更多的配置选项可能需要了解，例如可以使用dependencyClasspath分隔编译、测试和运行时依赖关系，或添加resolvers来标识用于下载依赖关系的备用存储库。有关详细信息，参阅SBT参考手册。

现在可以使用熟悉的spark-submit脚本运行这些应用程序。使用--packages参数将Commons
CSV作为运行时依赖关系。记住，spark-submit使用Maven语法，而不是SBT语法（冒号作为分隔符而不是百分号），并且不需要将Spark本身作为依赖关系，因为默认情况下它是隐含的。可以使用逗号分隔列表添加其他Maven
ID。

\# Run the Scala version.

cd /root/spark-app/building-sbt

$SPARK\_HOME/bin/spark-submit \\

\--class com.pinecone.SBuilding \\

\--packages org.apache.commons:commons-csv:1.2 \\

target/scala-2.13/buildingsbt\_2.13-1.0.jar

命令 4.5

让SBT处理的依赖项的另一种方法是自行下载它们。以下是如何更改的示例项目来使用此方法：

（1）更新build.sbt文件以删除Commons CSV依赖关系。

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1"

//libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

代码 4.18

（2）将Commons CSV下载到本地的lib /目录。SBT在编译时隐含地使用此目录中的任何东西。

cd /root/spark-app/building-sbt/lib

wget
<http://central.maven.org/maven2/org/apache/commons/commons-csv/1.2/commons-csv-1.2.jar>

命令 4.6

（3）像以前一样构建代码（代码 4.15）。这导致与以前的方法相同的jar文件。

（4）现在可以使用spark-submit使用--jars参数运行应用程序，以将Commons
CSV作为运行时依赖关系。可以使用逗号分隔列表添加其他jar。

\# Run the Scala version.

cd /root/spark-app/building-sbt

$SPARK\_HOME/bin/spark-submit \\

\--class com.pinecone.SBuildingSBT \\

\--jars lib/commons-csv-1.2.jar \\

target/scala-2.13/buildingsbt\_2.13-1.0.jar

命令 4.8提交scala程序

作为最佳做法，应确保依赖库不是同时即在lib目录中保存，也在build.sbt中定义。如果指定受管理的依赖项，并且还在lib目录中有本地副本，则如果依赖库的版本不同步，则可能会浪费时间排除这个小故障。还应该查看Spark自己的集成jar，当运行spark-submit时，它将隐含在类路径中。如果需要的应用依赖库已经是Spark的核心依赖库，在应用jar中包括应用依赖库的副本可能会导致版本冲突。

#### 创建jar

随着库依赖性的增加，将所有这些文件发送到Spark集群中的每个节点的网络开销也会增加。官方的Spark文档建议创建一个特殊的jar文件，其中包含应用程序及其所有依赖项，称为装配jar
（或“uber”jar）以减少网络负载。装配jar包含被组合和扁平化的一组类和资源文件。使用sbt-assembly插件生成装配jar。这个插件已经在的示例项目中，如project/assembly.sbt文件所示：

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

代码 4.41

更新build.sbt文件将所提供的Spark依赖关系标记为provided。这防止依赖关系被包含在装配jar中。如果需要，还可以恢复Commons
CSV依赖关系，尽管lib/目录中的本地副本仍将在编译时自动获取。

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.1" %
provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1" %
provided

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

代码 4.42

接下来，运行assembly命令。此命令创建一个包含应用程序和GSON类的装配jar。

cd /root/spark-app/building-sbt

sbt assembly

代码 4.43

还有更多配置选项可用，例如使用MergeStrategy来解决潜在的重复和依赖冲突。可以使用less命令确认装配jar的内容：

cd /root/spark-app/building-sbt

less target/scala-2.13/BuildingSBT-assembly-1.0.jar | grep commons

\-rw---- 1.0 fat 0 b- stor 16-Mar-20 13:31 org/apache/commons/

\-rw---- 1.0 fat 0 b- stor 16-Mar-20 13:31 org/apache/commons/csv/

\-rw---- 2.0 fat 11560 bl defN 15-Aug-21 17:48
META-INF/LICENSE\_commons-csv-1.2.txt

\-rw---- 2.0 fat 1094 bl defN 15-Aug-21 17:48
META-INF/NOTICE\_commons-csv-1.2.txt

\-rw---- 2.0 fat 824 bl defN 15-Aug-21 17:48
org/apache/commons/csv/Assertions.class

\-rw---- 2.0 fat 1710 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVFormat$Predefined.class

\-rw---- 2.0 fat 13983 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVFormat.class

\-rw---- 2.0 fat 1811 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVParser$1.class

\-rw---- 2.0 fat 1005 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVParser$2.class

\-rw---- 2.0 fat 7784 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVParser.class

\-rw---- 2.0 fat 899 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVPrinter$1.class

\-rw---- 2.0 fat 7457 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVPrinter.class

\-rw---- 2.0 fat 5546 bl defN 15-Aug-21 17:48
org/apache/commons/csv/CSVRecord.class

\-rw---- 2.0 fat 1100 bl defN 15-Aug-21 17:48
org/apache/commons/csv/Constants.class

\-rw---- 2.0 fat 2160 bl defN 15-Aug-21 17:48
org/apache/commons/csv/ExtendedBufferedReader.class

\-rw---- 2.0 fat 6407 bl defN 15-Aug-21 17:48
org/apache/commons/csv/Lexer.class

\-rw---- 2.0 fat 1124 bl defN 15-Aug-21 17:48
org/apache/commons/csv/QuoteMode.class

\-rw---- 2.0 fat 1250 bl defN 15-Aug-21 17:48
org/apache/commons/csv/Token$Type.class

\-rw---- 2.0 fat 1036 bl defN 15-Aug-21 17:48
org/apache/commons/csv/Token.class

命令 4.9使用less命令确认装配jar的内容

现在可以使用装配jar提交执行应用程序。因为依赖关系是捆绑在一起的，所以不需要使用--jars或—packages：

\# Run the Scala version.

cd /root/spark-app/building-sbt

$SPARK\_HOME/bin/spark-submit \\

\--class com.pinecone.SBuildingSBT \\

target/scala-2.13/BuildingSBT-assembly-1.0.jar

命令 4.11提交scala程序

## 部署应用

Spark程序bin目录中的spark-submit脚本用于在集群上启动应用程序。它可以通过统一的界面，使用所有Spark支持的集群管理器。因此不必为每个应用程序专门配置应用程序。如果开发的代码依赖于其他项目，则需要将它们与应用程序一起打包，才能将代码分发到Spark群集。为此，需要创建一个包含代码及其依赖关系的装配jar或uber-jar。一个uber-jar也是一个jar文件，不仅包含一个Java程序，还嵌入了它的依赖关系。这意味着jar作为软件的一体化分发，不需要任何其他Java代码。优点在于可以分发的uber-jar，并不关心任何依赖关系是否安装在目标位置，因为的uber-jar实际上没有依赖关系。

sbt和Maven都提供了装配插件。创建装配jar时，列出Spark和Hadoop作为provided依赖项，指出这些不需要捆绑，因为它们在运行时由集群管理器提供。一旦有一个装配jar，可以调用bin/spark-submit脚本传递jar运行应用程序。用户应用程序打包完成后，可以使用bin/spark-submit脚本启动，此脚本负责使用Spark及其依赖关系设置类路径，并可支持Spark支持的不同群集管理器和部署模式：

./bin/spark-submit \\

\--class \<main-class\> \\

\--master \<master-url\> \\

\--deploy-mode \<deploy-mode\> \\

\--conf \<key\>=\<value\> \\

... \# other options

\<application-jar\> \\

\[application-arguments\]

代码 4.48

一些常用的选项是：

（1）--class：应用程序的入口点（例如org.apache.spark.examples.SparkPi ）

（2）--master：群集的主URL （例如： spark://23.195.26.187:7077 ）

（3）--deploy-mode：是否将驱动程序部署在工作节点（cluster）上，或作为外部客户机（client）本地部署，默认值为client。

（4）--conf：Key = value格式的任意Spark配置属性。对于包含空格的值，用引号括起“key = value”。

（5）application-jar：包含应用程序和所有依赖关系的捆绑jar的路径，该URL必须在集群中全局可见，例如hdfs://路径或所有节点上存在的file://路径。

（6）application-arguments：参数传递给主类的main方法

如果集群中运行驱动程序的主节点和工作节点在同一个物理网络中，常见的部署策略是从主节点上提交应用程序。在此设置中，client模式是适当的。在client模式下，驱动程序直接在spark-submit过程中启动，作为集群的客户端。应用程序的输入和输出连接到控制台。因此，此模式特别适用于涉及REPL的应用程序，例如Spark交互界面。

如果的应用程序从远离工作节点的机器提交，例如本地在笔记本电脑上，通常使用cluster模式来最大限度地减少驱动程序和执行器之间的网络延迟，目前Standalone模式不支持Python应用程序的cluster模式。

有几个可用的选项是特定于正在使用的集群管理器，例如使用具有cluster部署模式的Spark独立集群，还可以指定--supervise，以确保如果出现非零退出代码失败，则自动重新启动驱动程序，要枚举所有可用于spark-submit可用选项，使用--help运行它，以下是常见选项的几个示例：

\# Run application locally on 8 cores

./bin/spark-submit \\

\--class org.apache.spark.examples.SparkPi \\

\--master local\[8\] \\

/path/to/examples.jar \\

100

\# Run on a Spark standalone cluster in client deploy mode

./bin/spark-submit \\

\--class org.apache.spark.examples.SparkPi \\

\--master spark://207.184.161.138:7077 \\

\--executor-memory 20G \\

\--conf spark.executor.instances=10 \\

/path/to/examples.jar \\

1000

\# Run on a Spark standalone cluster in cluster deploy mode with
supervise

./bin/spark-submit \\

\--class org.apache.spark.examples.SparkPi \\

\--master spark://207.184.161.138:7077 \\

\--deploy-mode cluster \\

\

\--executor-memory 20G \\

\--conf spark.executor.instances=10 \\

/path/to/examples.jar \\

1000

\# Run on a YARN cluster

export HADOOP\_CONF\_DIR=XXX

./bin/spark-submit \\

\--class org.apache.spark.examples.SparkPi \\

\--master yarn \\

\--deploy-mode cluster \\ \# can be client for client mode

\--executor-memory 20G \\

\--num-executors 50 \\

/path/to/examples.jar \\

1000

\# Run a Python application on a Spark standalone cluster

./bin/spark-submit \\

\--master spark://207.184.161.138:7077 \\

examples/src/main/python/pi.py \\

1000

\# Run on a Kubernetes cluster in cluster deploy mode

./bin/spark-submit \\

\--class org.apache.spark.examples.SparkPi \\

\--master k8s://https://207.184.161.138:6443 \\

\--deploy-mode cluster \\

\--executor-memory 20G \\

\--conf spark.executor.instances=10 \\

local:///opt/spark/examples/jars/spark-examples_2.13-4.1.1.jar \\

1000

命令 4.19

### Spark 4.1.1 Structured Streaming 提交模板（Kafka + Kubernetes）

下面给出一个可直接改值运行的最小模板，适用于Spark 4.1.1在Kubernetes上的结构化流任务提交：

```bash
./bin/spark-submit \
  --name structured-kafka-job \
  --class com.example.StructuredKafkaJob \
  --master k8s://https://<k8s-apiserver>:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=<namespace> \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=<sa-name> \
  --conf spark.kubernetes.container.image=<registry>/spark:4.1.1 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=4g \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.streaming.checkpointLocation=s3a://<bucket>/checkpoints/structured-kafka-job \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  local:///opt/spark/app/structured-kafka-job_2.13-1.0.0.jar
```

常用参数说明：

（1）`--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`：Kafka源所需依赖。

（2）`spark.sql.streaming.checkpointLocation`：结构化流恢复与容错关键目录，建议放在对象存储或HDFS上。

（3）`spark.kubernetes.container.image`：需包含应用运行所需JDK/Python及系统依赖。

（4）如果任务为PySpark，可将主程序jar替换为`.py`文件，并按需增加`--py-files`。

表格 4.1给出了传递给Spark的master URL是以下格式之一：

| master URL                      | 含义                                                                                                                                             |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| local                           | 用一个工作线程在本地运行Spark，即根本没有并行性。                                                                                                                    |
| local\[K\]                      | 使用K个工作线程在本地运行Spark，理想情况下将其设置为机器上的核心数。                                                                                                          |
| local\[K,F\]                    | 使用K个工作线程和F个maxFailures本地运行Spark，有关此变量的说明，参阅spark.task.maxFailures。                                                                             |
| local\[\*\]                     | 在本地运行Spark，其工作线程与机器上的逻辑内核一样多。                                                                                                                  |
| local\[\*,F\]                   | 在本地运行Spark，其工作线程与机器上的逻辑内核一样多和F个maxFailures。                                                                                                    |
| spark://HOST:PORT               | 连接到给定的Spark独立集群主控，端口必须是主服务器配置使用的端口，默认情况下为7077。                                                                                                 |
| spark://HOST1:PORT1,HOST2:PORT2 | 使用Zookeeper连接到带有备用主机的给定Spark独立集群。该列表必须具有使用Zookeeper设置的高可用性群集中的所有主机，端口必须是主服务器配置使用的端口，默认情况下为7077。                                                |
| k8s://https://HOST:PORT          | 连接到给定的 Kubernetes API Server，常见端口为 6443。通常配合 --deploy-mode cluster 与 spark.kubernetes.* 配置提交。 |
| yarn                            | 根据--deploy-mode的值，以client或cluster模式连接到YARN群集，将基于HADOOP\_CONF\_DIR或YARN\_CONF\_DIR变量找到集群位置。                                                     |

表格 4.2Spark的master URL格式

另外，spark-submit脚本可以从属性文件加载默认的Spark配置值，并将它们传递到应用程序。默认情况下，它将从Spark目录中的conf/spark-defaults.conf中读取选项，以这种方式加载默认Spark配置可以避免需要某些配置信息通过spark-submit设置，例如如果设置了spark.master属性，则可以从spark-submit安全地省略--master标志。实际上可以通过三个地方设置运行Spark应用程序的配置信息，分别为：

（1）应用程序中的SparkConf

（2）spark-submit命令行参数

（3）Spark目录中的配置文件

通常，在应用程序的SparkConf上显式设置的配置值具有最高优先级，其次是通过spark-submit命令行参数传递配置值，然后将该值设置为默认值。如果不清楚配置选项的来源，可以使用--verbose选项运行spark-submit来打印出细粒度的调试信息。

当使用spark-submit，应用程序jar以及--jars选项中包含的任何jar将被自动传输到群集。--jars之后提供的URL必须用逗号分隔。这些jar文件必须包含在驱动程序和执行器类路径上。目录扩展不适用于--jars。Spark使用以下URL方案来允许不同的策略来传播jar：

（1）file:绝对路径和file:/ URI由驱动程序的HTTP文件服务器提供，每个执行器从驱动程序HTTP服务器提取文件。

（2）hdfs:、http:、https：、ftp：这些按照预期从URI中下拉文件和jar

（3）local:以local:开头的URI，预计作为每个工作节点上的本地文件存在。这意味着不会出现网络传递文件，并且适用于推送大型文件和jar到每个工作节点上，或通过NFS、GlusterFS等共享。NFS（Network
File
System）即网络文件系统，是FreeBSD支持的文件系统中的一种，它允许网络中的计算机之间通过TCP/IP网络共享资源。在NFS的应用中，本地NFS的客户端应用可以透明地读写位于远端NFS服务器上的文件，就像访问本地文件一样。GlusterFS是Scale-Out存储解决方案Gluster的核心，它是一个开源的分布式文件系统，具有强大的横向扩展能力，通过扩展能够支持数PB存储容量和处理数千客户端。GlusterFS借助TCP/IP或InfiniBandRDMA网络将物理分布的存储资源聚集在一起，使用单一全局命名空间来管理数据。

  - 注意

jar和文件将复制到执行器节点上每个SparkContext的工作目录，这可能会随着时间的推移占用大量空间，并需要清理。使用YARN时，清理将自动进行处理；如果通过Spark
Standalone，可以使用spark.worker.cleanup.appDataTtl属性配置自动清理。

用户可能还包括任何其他依赖关系通过使用—packages参数，其中提供逗号分隔的Maven坐标列表。使用此命令时将处理所有传递依赖关系，搜索当地的maven资源库，然后搜索maven中心和由--repositories提供的任何其他远程存储库。坐标的格式应为groupId:artifactId:version。这些参数可以与pyspark、spark-shell和spark-submit一起使用。对于Python，等效的--py-files选项可用于将.egg、.zip和.py库分发到执行器上。

### 集群架构

Spark应用程序作为独立的集群进程运行，由称为驱动程序（Driver
Program）中的SparkContext对象协调。具体来说，要在集群上运行，SparkContext可以连接到几种类型的集群管理器，它们跨应用程序分配资源。一旦连接，Spark将在集群中的节点上获取执行器（Executor），这些进程可以为应用程序运行计算和存储数据。接下来，Spark将由jar或Python文件定义应用程序代码（被传递给SparkContext）发送给执行器。最后，SparkContext将任务发送给执行器运行。

![park cluster
components](media/10_running_applications/media/image2.png)

图例 4‑1集群框架

Apache Spark遵循主/从架构，包含两个主要守护程序和一个集群管理器：

（1）主（Master）进程：驱动程序（Driver Program）

（2）从（Slaver）进程：工作节点（Worker Node）

Spark集群有一个主进程和任意数量的从进程。驱动程序和执行器运行他们各自的Java进程，用户可以在同一个水平Spark集群上或在单独的机器上运行它们，即在垂直Spark集群或混合机器配置中运行它们，有关这种架构有几件有用的事情要注意。

每个应用程序都获得自己的执行器进程，这些进程在整个应用程序的持续时间内保持不变，并在多线程中运行任务。这有利于在调度方（每个驱动程序安排自己的任务）和执行方（在不同JVM中运行的不同应用程序的任务）之间彼此隔离应用程序，但是这也意味着数据不能在不写入外部存储系统的情况下在不同的Spark应用程序（SparkContext的实例）之间共享。

Spark与底层群集管理器无关，只要可以获取执行器进程，并且这些进程彼此通信，即使在也支持其他应用程序的集群管理器上运行，它也是相对容易的。驱动程序必须在其生命周期中监听并接收其执行器的传入连接，因此驱动程序必须能够从工作节点进行网络寻址。

因为驱动程序调度集群上的任务，所以它应该靠近工作节点运行，最好在相同的局域网上运行。如果要远程发送求到集群，最好是向驱动程序打开一个RPC，并从附近提交操作，而不是从远离工作节点运行驱动程序。

#### 驱动程序

Spark驱动程序（也称为应用程序的驱动程序进程）是为Spark应用程序承载SparkContext的JVM进程。它是Spark应用程序中的主节点。它是作业和任务执行的驾驶舱（使用DAGScheduler和任务计划程序）。它承载环境的Web
UI。

驱动程序是Spark应用程序的主节点。将Spark应用程序分解为任务并安排它们在执行器上运行。一个驱动程序是任务调度程序生活和产生任务的工作节点司机协调工作节点和整体执行任务。

驱动程序是Spark交互界面的核心点和入口点，包括Scala，Python和R三种语言。驱动程序运行应用程序的main函数，并且是创建Spark上下文的地方。Spark驱动程序包含各种组件—DAGScheduler、TaskScheduler、BackendScheduler和BlockManager，负责将Spark用户代码转换为在集群上执行的实际Spark作业，其主要任务包括：

（1）在Spark集群的主节点上运行的驱动程序会调度作业执行，并与集群管理器协商；

（2）将RDD转换为执行图并将图分解成多个阶段；

（3）驱动程序存储有关所有弹性分布式数据库及其分区的元数据；

（4）作为作业和任务执行的控制部分，将用户应用程序转换为称为任务的较小执行单元，然后执行器执行任务，即运行单个任务的工作进程；

（5）驱动程序通过端口4040（默认端口）的Web UI公开有关正在运行的spark应用程序的信息。

  - 注意

Spark shell是一个Spark应用程序和驱动程序，创建一个可用作为sc的SparkContext。

#### 执行器

工作节点也称从节点，是正在运行Spark实例，其中执行器（Executor）执行任务。它们是Spark中的计算节点。工作节点接收在线程池中运行的序列化任务，托管一个BlockManager可以向Spark群集中的其他工作节点提供块。工作节点之间使用其块管理器实例进行通信。BlockManager是Spark中数据块（简单的块）的键值存储。BlockManager充当在Spark应用程序中的每个节点上运行的本地缓存，即驱动程序和执行程序（并在创建SparkEnv时创建）

工作节点中执行器是负责执行任务的分布式代理，执行器为在Spark应用程序中缓存的RDD提供内存中的存储。执行器启动时，首先向驱动程序注册并直接通信执行任务。执行器可以在其生命周期内并行运行多个任务，并跟踪运行的任务。执行器是负责执行任务的分布式代理。每个Spark应用程序都有自己的执行器。执行者通常在Spark应用程序的整个生命周期中运行，这种现象称为执行器的静态分配。然而，用户还可以选择动态分配执行器，其中可以动态添加或删除Spark执行器以匹配总体负载。执行器的主要功能：

（1）执行器执行所有的数据处理。

（2）从外部来源读取和写入数据。

（3）执行器将计算结果数据存储在内存，缓存或硬盘驱动器中。

（4）与存储系统交互。

### 集群管理

集群管理器（Cluster
Manager）是一个外部服务负责获取Spark集群上的资源并将其分配给Spark的作业（Job）。有3种不同类型的集群管理器，Spark应用程序可以利用其进行各种物理资源的分配和释放，例如Spark作业的内存、CPU内存等。Hadoop
YARN、Kubernetes或Standalone集群管理器可以在内部或云端启动一个Spark应用程序来运行。

为任何Spark应用选择集群管理器取决于应用程序的目标，因为所有集群管理器都提供不同的调度功能集。要开始使用Spark时，Standalone集群管理器是开发新的Spark应用程序时最容易使用的集群管理器。目前支持的三个集群管理器包括：

（1）Standalone：Spark包含的简单集群管理器，可以轻松设置集群。

（2）Kubernetes：容器化集群管理平台，适合云原生部署。

（3）Hadoop YARN：Hadoop 2中的资源管理器。

（4）Kubernetes：Spark 4.x 中 Kubernetes 已是主流部署选项之一。Kubernetes 是提供以容器为中心基础设施的开源平台，相关文档可参考官方
Github组织中积极开发。有关文档，参阅该项目的README。

#### Standalone

要安装Spark
Standalone模式，只需将Spark的编译版本放在群集上的每个节点上即可。可以使用每个版本获取Spark的预构建版本，也可以自行构建。

  - 手动启动集群

可以通过执行以下命令启动独立的主服务器：

./sbin/start-master.sh

命令 4.20

一旦开始，主节点的显示终端将打印出一个类似spark://HOST:PORT的URL，可以使用其连接工作节点，或作为“master”参数传递给SparkContext。还可以在主节点的Web
UI上找到此URL，默认情况下Web
UI的地址为http://localhost:8080。同样，可以启动一个或多个从节点，并通过以下方式将其连接到主服务器：

./sbin/start-slave.sh \<master-spark-URL\>

命令 4.21

一旦开始工作，查看主节点的Web
UI，应该看到列出的新节点，以及其CPU和内存的数量，其中内存为操作系统留下了一千兆字节，最后以下配置选项可以传递给主节点和从节点：

| 参数                                      | 含义                                                             |
| --------------------------------------- | -------------------------------------------------------------- |
| \-h HOST ，--host HOST                   | 要监听的主机名                                                        |
| \-i HOST ，--ip HOST                     | 要监听的主机名（已弃用，使用-h或--host）                                       |
| \-p PORT ，--port PORT                   | 用于侦听的服务端口，默认值主服务器为7077，工作节点为随机                                 |
| \--webui-port PORT                      | Web UI端口，默认值为8080，主服务器为8081                                    |
| \-c CORES --cores CORES ， --cores CORES | 允许Spark应用程序在机器上使用的总CPU内核，默认值为全部可用; 仅在工作节点上                     |
| \-m MEM ，--memory MEM                   | 允许Spark应用程序在计算机上使用的内存总量，格式为1000M或2G，默认值为计算机的总RAM减去1 GB；仅在工作节点上 |
| \-d DIR ， --work-dir DIR                | 用于临时空间和作业输出日志的目录，默认值为SPARK\_HOME/work；仅在工作节点上。                 |
| \--properties-file FILE                 | 要加载的自定义Spark属性文件的路径，默认值为conf/ spark-defaults.conf              |

表格 4.3传递给主节点和从节点的配置选项

  - 集群启动脚本

要启动具有启动脚本的Spark独立集群，应该在Spark目录conf中创建一个名为slaves的文件，该文件必须包含打算启动Spark工作的所有计算机的主机名，每行一个。如果slaves文件不存在，则启动脚本默认为单个机器（localhost），这对于测试非常有用。主机通过ssh访问每个工作机器。默认情况下，ssh并行运行，需要无密码（使用私钥）访问进行设置。如果没有无密码设置，可以设置环境变量SPARK\_SSH\_FOREGROUND，并为每个工作节点提供一个密码。设置此文件后，可以使用以下Shell脚本启动或停止集群，这些脚本基于Hadoop的部署脚本，并且可以在SPARK\_HOME/sbin中，其中包括：

sbin/start-master.sh：在脚本执行的机器上启动主实例。

sbin/start-slaves.sh：在conf/slaves文件中指定的每台机器上启动一个从实例。

sbin/start-slave.sh：在脚本执行的机器上启动一个从实例。

sbin/start-all.sh：启动主和多个从实例。

sbin/stop-master.sh：停止通过sbin/start-master.sh脚本启动的主服务器。

sbin/stop-slaves.sh：停止在conf/slaves文件中指定的计算机上的所有从实例。

sbin/stop-all.sh：停止主和从实例。

这些脚本必须在要作为主节点的计算机上执行，而不是在本地计算机上执行，还可以通过在目录conf中的spark-env.sh设置环境变量来进一步配置集群。通过从spark-env.sh.template开始创建此文件，并将其复制到所有的工作机器，以使设置生效。以下设置可用：

| 环境变量                       | 含义                                                                                            |
| -------------------------- | --------------------------------------------------------------------------------------------- |
| SPARK\_MASTER\_HOST        | 将主机绑定到特定的主机名或IP地址，例如公共地址。                                                                     |
| SPARK\_MASTER\_PORT        | 在不同的端口上启动主实例，默认值为7077。                                                                        |
| SPARK\_MASTER\_WEBUI\_PORT | 主网页界面的端口，默认值为8080。                                                                            |
| SPARK\_MASTER\_OPTS        | 仅适用于主机的配置属性，格式为“-Dx = y”，默认值为无。有关可能的选项列表，参见下文。                                                |
| SPARK\_LOCAL\_DIRS         | 用于Spark中“scratch”空间的目录，包括存储在磁盘上的地图输出文件和RDD。这应该在系统中的快速本地磁盘上。也可以是不同磁盘上的多个目录的逗号分隔列表。             |
| SPARK\_WORKER\_CORES       | 允许Spark应用程序在机器上使用的核心总数，默认值为所有可用内核。                                                            |
| SPARK\_WORKER\_MEMORY      | 允许Spark应用程序在机器上使用的总内存量，例如1000m，2g，默认值为总内存减去1 GB；注意每个应用程序的单个内存都使用其spark.executor.memory属性进行配置。 |
| SPARK\_WORKER\_PORT        | 在特定端口启动Spark worker，默认值为random。                                                               |
| SPARK\_WORKER\_WEBUI\_PORT | 用于工作者Web UI的端口，默认值为8081。                                                                      |
| SPARK\_WORKER\_DIR         | 用于运行应用程序的目录，其中将包括日志和临时空间，默认值为SPARK\_HOME/ work。                                               |
| SPARK\_WORKER\_OPTS        | 仅适用于“-Dx = y”形式的工作的配置属性，有关可能的选项列表，参见下文。                                                       |
| SPARK\_DAEMON\_MEMORY      | 内存分配给Spark主人和工程师守护进程自身（默认值：1g）。                                                               |
| SPARK\_DAEMON\_JAVA\_OPTS  | Spark主和工作程序守护进程的JVM选项本身以“-Dx = y”的形式（默认值：无）。                                                  |
| SPARK\_PUBLIC\_DNS         | Spark主人和工作节点的公共DNS名称（默认值：无）。                                                                  |

表格 4.4spark-env.sh设置环境变量

  - 将应用程序连接到群集

要在Spark群集上运行应用程序，只需将主节点的URL地址，类似spark://IP:PORT，传递给SparkContext构造函数即可。要针对群集运行交互式，运行以下命令：

./bin/spark-shell --master spark://IP:PORT

命令 4.22

还可以传递一个选项--total-executor-cores \<numCores\>来控制spark-shell在群集上使用的核心数。

  - 启动Spark应用程序

spark-submit脚本提供了将编译的Spark应用程序提交到集群的最直接方法。对于Standalone集群，Spark目前支持两种部署模式。在client模式下，驱动程序以与提交应用程序的客户端相同的进程启动，但是在cluster模式下，驱动程序是从群集中的一个工作节点中启动进程，客户端进程在履行其提交应用程序的责任后立即退出，而不必等待应用程序完成。

如果的应用程序是通过Spark提交启动的，则应用程序jar会自动分发到所有工作节点。对于应用程序所依赖的任何其他jar，应该使用逗号作为分隔符，通过--jars标志指定它们，例如--jars
jar1,jar2。

此外，独立集群模式支持使用非零退出代码退出的应用程序自动重新启动，要使用此功能，我们可以在启动应用程序时传入--supervise标志给spark-submit命令，如果希望终止反复失败的应用程序，则可以通过以下方法进行：

./bin/spark-class org.apache.spark.deploy.Client kill \<master url\>
\<driver ID\>

命令 4.23

可以通过Standalone的主Web UI（类似http://\<master url\>:8080）找到驱动程序ID。

  - 资源调度

独立集群模式目前只支持跨应用程序的简单FIFO调度器，但是为了允许多个并发用户，可以控制每个应用程序将使用的最大资源数量。默认情况下，它将获取集群中的所有内核，只有一次运行一个应用程序才有意义，可以通过在SparkConf中设置spark.cores.max来封顶核心数量，例如：

val conf = new SparkConf()

.setMaster(...)

.setAppName(...)

.set("spark.cores.max", "10")

val sc = new SparkContext(conf)

代码 4.54

此外，我们可以在集群主进程上配置spark.deploy.defaultCores，如果应用程序没有设置spark.cores.max参数，将使用默认值而不是最多的核心数，为此请在conf/spark-env.sh中添加以下内容：：

export SPARK\_MASTER\_OPTS = "-Dspark.deploy.defaultCores=\<value\>"

命令 4.24

这对于共享群集而言非常有用，用户可能不能单独配置最大数量的内核。

  - 监控和记录

Spark的独立模式提供基于Web的用户界面来监控集群，主节点和每个工作节点都有自己的Web
UI，显示群集和作业统计信息。默认情况下，可以访问端口8080为的主服务器Web
UI，可以在配置文件或命令行选项中更改端口。此外，每个作业的详细日志输出也会写入每个从节点的工作目录（默认情况下为SPARK\_HOME/work）。将看到每个作业stdout和stderr两个文件，其中所有输出都写入其控制台。

  - 与Hadoop一起运行

可以在现有的Hadoop集群上运行Spark，只需在同一台机器上将其作为单独的服务启动。要从Spark访问Hadoop数据，只需使用hdfs://URL，通常为hdfs://\<namenode\>:9000/path，可以在Hadoop
Namenode的Web
UI上找到正确的URL，或者可以为Spark设置单独的群集，并且仍然可以通过网络访问HDFS；这将比磁盘本地访问速度慢，但是如果仍然在同一局域网中运行可能不会产生网络通讯的问题，例如将Hadoop上的每个机架上放置一些Spark机器。

#### YARN

Apache Hadoop YARN是一种新的 Hadoop
资源管理器，可为上层应用提供统一的资源管理和调度，它的引入为集群在利用率、资源统一管理和数据共享等方面带来了巨大好处。支持在YARN（Hadoop
NextGen）上运行的版本已添加到Spark 0.6.0版本的中，并在后续版本中得到改进。

YARN的基本思想是将JobTracker的两个主要功能（资源管理和作业调度/监控）分离，主要方法是创建一个全局的ResourceManager（RM）和若干个针对应用程序的ApplicationMaster（AM），这里的应用程序是指传统的MapReduce作业或作业的DAG（有向无环图）。YARN
分层结构的本质是 ResourceManager，这个实体控制整个集群并管理应用程序向基础计算资源的分配。ResourceManager
将各个资源部分（计算、内存、带宽等）精心安排给基础
NodeManager（YARN的每节点代理）。ResourceManager 还与
ApplicationMaster 一起分配资源，与NodeManager
一起启动和监视它们的基础应用程序。在此上下文中，ApplicationMaster承担了以前的TaskTracker的一些角色，ResourceManager
承担了 JobTracker 的角色。

ApplicationMaster 管理一个在YARN内运行的应用程序的每个实例。ApplicationMaster 负责协调来自
ResourceManager的资源，并通过NodeManager
监视容器的执行和资源使用（CPU、内存等的资源分配）。请注意，尽管目前的资源更加传统（CPU核心、内存），但未来会带来基于当前任务的新资源类型，比如图形处理单元或专用处理设备。从YARN角度讲，ApplicationMaster是用户代码，因此存在潜在的安全问题，YARN假设ApplicationMaster存在错误或者甚至是恶意的，因此将它们当作无特权的代码对待。

NodeManager管理一个YARN集群中的每个节点，NodeManager提供针对集群中每个节点的服务，从监督对一个容器的终生管理到监视资源和跟踪节点健康。MRv1通过插槽管理Map和Reduce任务的执行，而
NodeManager管理抽象容器，这些容器代表着可供一个特定应用程序使用的针对每个节点的资源。YARN继续使用HDFS层，它的主要NameNode用于元数据服务，而
DataNode 用于分散在一个集群中的复制存储服务。

要使用一个 YARN 集群，首先需要来自包含一个应用程序的客户的请求。ResourceManager 协商一个容器的必要资源，启动一个
ApplicationMaster 来表示已提交的应用程序。通过使用一个资源请求协议，ApplicationMaster
协商每个节点上供应用程序使用的资源容器。执行应用程序时，ApplicationMaster
监视容器直到完成。当应用程序完成时，ApplicationMaster 从 ResourceManager
注销其容器，执行周期就完成了。

确保HADOOP\_CONF\_DIR或YARN\_CONF\_DIR指向包含Hadoop集群的（客户端）配置文件的目录。这些配置用于写入HDFS并连接到YARN
ResourceManager。此目录中包含的配置将分发到YARN群集，以便应用程序使用的所有容器都使用相同的配置。如果配置引用了不受YARN管理的Java系统属性或环境变量，那么也应该在Spark应用程序的配置（驱动程序，执行程序和AM在客户端模式下运行时）中进行设置。

有两种可用于在YARN上启动Spark应用程序的部署模式。在cluster模式下，Spark驱动程序在由集群上的YARN管理的应用程序主进程中运行，客户端可以在启动应用程序后离开。在client模式下，驱动程序在客户端进程中运行，应用程序主程序仅用于从YARN请求资源。不同于Spark独立和Kubernetes模式，其中master地址在--master参数中指定，在YARN模式下，ResourceManager的地址从Hadoop配置中提取。因此，--
--master参数是yarn。要在cluster模式下启动Spark应用程序：

$ ./bin/spark-submit --class path.to.your.Class --master yarn
--deploy-mode cluster \[options\] \<app jar\> \[app options\]

命令 4.25

例如：

$ ./bin/spark-submit --class org.apache.spark.examples.SparkPi \\

\--master yarn \\

\--deploy-mode cluster \\

\--driver-memory 4g \\

\--executor-memory 2g \\

\--executor-cores 1 \\

\--queue thequeue \\

lib/spark-examples\*.jar \\

10

命令 4.26

上面启动了一个YARN客户端程序，该程序启动了默认的Application Master，然后SparkPi将作为Application
Master的子线程运行。 客户端将定期轮询Application
Master以获取状态更新，并将其显示在控制台中，应用程序完成运行后客户端将退出。要在client模式下启动Spark应用程序请执行相同操作，否则将client替换为cluster，以下显示如何在client模式下运行spark-shell：

$ ./bin/spark-shell --master yarn --deploy-mode client

命令 4.27

在cluster模式下，驱动程序在与客户机不同的机器上运行，因此SparkContext.addJar将不会与客户端本地的文件一起使用，要使客户端上的文件可用于SparkContext.addJar，请在启动命令中使用--jars选项包含它们。

$ ./bin/spark-submit --class my.main.Class \\

\--master yarn \\

\--deploy-mode cluster \\

\--jars my-other-jar.jar,my-other-other-jar.jar \\

my-main-jar.jar \\

app\_arg1 app\_arg2

命令 4.28

#### Kubernetes

在 Spark 4.x 中，生产环境推荐优先使用 Standalone、YARN 或 Kubernetes。Mesos 相关集成在新版本已不再作为主流选项，本节统一使用 Kubernetes 作为容器化部署范式。

Kubernetes 模式下，`spark-submit` 通过 `k8s://` 连接 API Server，驱动与执行器以 Pod 形式运行。最小提交流程如下：

```bash
./bin/spark-submit \
  --master k8s://https://<k8s-apiserver>:6443 \
  --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=3 \
  --conf spark.kubernetes.container.image=<your-spark-image> \
  local:///opt/spark/examples/jars/spark-examples_2.13-4.1.1.jar 1000
```

建议补充以下生产级配置：

（1）命名空间与服务账号：`spark.kubernetes.namespace`、`spark.kubernetes.authenticate.driver.serviceAccountName`。

（2）镜像与依赖：固定镜像版本，避免 `latest`；Python 任务通过 `--py-files` 或镜像内置依赖。

（3）资源与弹性：设置 `spark.executor.memory`、`spark.executor.cores`，并结合动态分配能力。

（4）可观测性：通过 Spark UI、Kubernetes Events 与日志系统统一排障。

（5）存储与数据访问：对象存储或 HDFS 凭据通过 Secret/ConfigMap 注入，避免明文配置。

当你需要跨环境一致交付（开发、测试、生产）时，Kubernetes 能显著降低运行时差异，通常是 Spark 4.x 的首选部署平台之一。
## 小结

本章讲述如何设置一个完整的开发环境来开发和调试Spark应用程序。本章使用Scala作为开发语言，sbt作为构建工具，讲述如何使用管理依赖项、如何打包和部署Spark应用程序。另外还介绍了Spark应用程序的几种部署模式。






