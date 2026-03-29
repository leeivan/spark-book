# 监视和优化


## 11.1 本章先看懂什么
- 先监控后优化：先定位瓶颈，再改参数。
- 常见瓶颈来源：Shuffle、序列化、内存、数据倾斜。
- 读懂 Spark UI 的关键页面与指标。

## 11.2 一个最小例子
现象：作业很慢。
1. 在 Spark UI 看最慢 Stage。
2. 检查是否有超大 Task（可能数据倾斜）。
3. 优化 Join 策略或分区数。
4. 复跑对比耗时。

调优不是“盲调参数”，而是“基于证据做改动”。

> **版本基线（更新于 2026-02-13）**
> 本书默认适配 Apache Spark 4.1.1（稳定版），并兼容 4.0.2 维护分支。
> 推荐环境：JDK 17+（建议 JDK 21）、Scala 2.13、Python 3.10+。
想理解 Spark 应用为什么慢，通常需要同时看两类信息：运行日志和 Spark UI。日志适合追踪细粒度事件与异常，Spark UI 更适合观察作业、阶段、任务、Shuffle 和存储使用的整体画像。两者结合，才能更快定位真正的瓶颈。

对 Spark 4.x 来说，性能优化不是“先调参数”，而是先确认瓶颈属于计算、网络、内存、序列化还是数据分布问题，再决定是否改代码、改分区、改 Join 策略或改资源配置。

## 11.3 工作原理

在深入了解Apache Spark的工作原理之前，先统一几个最常见的术语。

（1）作业（Job）：一次动作操作触发的一次完整执行过程，例如 `count`、`collect` 或写出结果。

（2）阶段（Stage）：作业会被拆分成若干阶段，阶段通常以Shuffle边界为分隔点；同一阶段内的计算可以流水线执行。

（3）任务（Task）：阶段内部的最小执行单元，通常一个分区对应一个任务。

（4）有向无环图（DAG）：由RDD及其依赖关系构成的逻辑图，用来描述“先做什么、后做什么”。

（5）执行器（Executor）：负责实际执行任务、缓存数据并回传状态的进程。

（6）驱动程序（Driver Program）：负责编排作业、生成执行计划、向执行器分发任务的进程。

（7）管理节点 / 工作节点：旧资料里常见 `Master / Slave` 说法，本书统一改为更中性的“管理节点 / 工作节点”。实际部署时，Driver并不一定固定运行在某个管理节点上，而是取决于部署模式与集群管理器。

### 11.3.1 依赖关系

Spark中的所有作业都由一系列操作组成，例如 `map`、`filter`、`reduce` 等，这些操作及其依赖关系共同构成DAG。更准确地说，Spark会先记录逻辑依赖，再在生成物理执行计划时尽量把可流水线化的窄依赖操作合并到同一阶段中执行。像 `map` 后接 `filter` 这样的链式转换，重点不在于“随意改写顺序”，而在于Spark会基于依赖关系与执行边界减少不必要的阶段切分与数据落盘。

基本上，RDD的评估本质上是延迟的，这意味着在RDD上执行一系列转换，并没有立即对其进行评估。虽然从现有的RDD创建新的RDD，但新的RDD还带有指向父RDD的指针。就这样所有的RDD之间的依赖关系被记录在DAG中，而不是产生实际数据，所以DAG记录了依赖关系，也称为谱系图（Lineage
Graph）。从一个例子开始，使用Cartesian或zip来理解RDD谱系图，当然也可以使用其他操作在Spark中构建RDD图。

![](media/11_monitoring_and_optimization/media/image1.jpeg)

图例 11‑1 RDD谱系图或DAG

上图描绘了一个RDD图，是以下一系列转换的结果：

```scala
scala> val r00 = sc.parallelize(0 to 9)
r00: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at
parallelize at <console>:24
scala> val r01 = sc.parallelize(0 to 90 by 10)
r01: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at
parallelize at <console>:24
scala> val r10 = r00 cartesian r01
r10: org.apache.spark.rdd.RDD[(Int, Int)] = CartesianRDD[2] at
cartesian at <console>:28
scala> val r11 = r00.map(n => (n, n))
r11: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[3] at
map at <console>:26
scala> val r12 = r00 zip r01
r12: org.apache.spark.rdd.RDD[(Int, Int)] = ZippedPartitionsRDD2[4]
at zip at <console>:28
scala> val r13 = r01.keyBy(_ / 20)
r13: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[5] at
keyBy at <console>:26
scala> val r20 = Seq(r11, r12, r13).foldLeft(r10)(_ union _)
r20: org.apache.spark.rdd.RDD[(Int, Int)] = UnionRDD[8] at union at
<console>:36
scala> r20.toDebugString
res1: String =
(10) UnionRDD[8] at union at <console>:36 []
| UnionRDD[7] at union at <console>:36 []
| UnionRDD[6] at union at <console>:36 []
| CartesianRDD[2] at cartesian at <console>:28 []
| ParallelCollectionRDD[0] at parallelize at <console>:24 []
| ParallelCollectionRDD[1] at parallelize at <console>:24 []
| MapPartitionsRDD[3] at map at <console>:26 []
| ParallelCollectionRDD[0] at parallelize at <console>:24 []
| ZippedPartitionsRDD2[4] at zip at <console>:28 []
| ParallelCollectionRDD[0] at parallelize at <console>:24 []
| ParallelCollectionRDD[1] at parallelize at <console>:24 []
| MapPartitionsRDD[5] at keyBy at <console>:26 []
| ParallelCollectionRDD[1] at parallelize at <console>:24 []
```

代码 11.1
在一个动作被调用之后，RDD的谱系图记录了需要执行什么转换，换句话说无论何时在现有RDD基础上创建新的RDD，使用谱系图管理这些依赖关系。基本上起到记录元数据作用，描述了与父RDD有什么类型的关系，每个RDD维护一个或多个父RDD指针。

Spark是分布式数据处理的通用框架，提供用于大规模数据操作的方法API、内存数据缓存和计算重用，对分区数据应用一系列粗粒度转换，并依赖数据集的谱系来重新计算失败时的任务。Spark围绕RDD和DAG的概念构建，DAG表示了转换和它们之间的依赖关系。

![http://datastrophic.io/content/images/2016/03/Spark-Overview--1-.png](media/11_monitoring_and_optimization/media/image2.png)

图例 11‑2Spark应用程序的执行过程

在高级别上，Spark应用程序（通常称为驱动程序或应用程序主控）由SparkContext和用户代码组成，用户代码与SparkContext交互创建RDD，并执行一系列转换以实现最终结果。RDD的这些转换过程会被Spark解释成DAG，并提交给调度器以在工作节点集群上执行。

RDD可以被认为是具有故障恢复可能性的不可变并行数据结构，提供了用于各种数据转换和实现的API，以及用于控制元素的缓存和分区以优化数据放置的API。RDD可以从外部存储或从另一个RDD创建，并存储有关其父项的信息以优化执行，并在出现故障时重新计算分区。从开发人员的角度来看，RDD代表分布式不可变数据和延迟评估操作，RDD接口定义了五个主要属性：

  - def getPartitions: Array\[Partition\]

列出分区

  - def getDependencies: Seq\[Dependency\[\_\]\]

其他RDD的依赖列表

  - def compute(split: Partition, context: TaskContext): Iterator\[T\]

计算每个分割的方法

  - def getPreferredLocations(split: Partition): Seq\[String\] = Nil

用于计算每个分割的首选位置列表

  - val partitioner: Option\[Partitioner\] = None

键值对RDD的分区器

对于这五种方法，可以通过代码 11.2 中把HDFS数据加载到RDD的例子来理解：

sparkContext.textFile("hdfs://...").map(…)

代码 11.2
首先在内存中加载HDFS块，然后应用map()过滤出键，创建两个RDD的键：

![http://datastrophic.io/content/images/2016/03/DAG-logical-vs-partitions-view--3-.png](media/11_monitoring_and_optimization/media/image3.png)

图例 11‑3在内存中加载HDFS块

两个RDD的属性分别为：

  - HadoopRDD

<!-- end list -->

  - > getPartitions = HDFS块

  - > getDependencies = 无

  - > compute = 加载内存中的块

  - > getPreferredLocations = HDFS块位置

  - > partitioner = 无

<!-- end list -->

  - MapPartitionsRDD

<!-- end list -->

  - > getPartitions = 与父类相同

  - > getDependencies = 父RDD

  - > compute = 计算父级并应用map()

  - > getPreferredLocations = 与父项相同

  - > partitioner = 无

### 11.3.2 划分阶段

在深入研究细节之前，可以先快速回顾一次执行流程：包含RDD转换的用户代码会先形成DAG，然后由DAG调度器按Shuffle边界切分为若干阶段。若一串操作不需要Shuffle或重新分区，它们通常会被合并进同一个阶段。之后，这些阶段再被展开为基于分区的任务，由任务调度器提交给集群管理器分发到执行器上执行。执行器运行任务、返回结果，并在需要时把中间数据写入内存或磁盘。这里要注意的是，并不是“每个作业都会启动一个新的Java虚拟机”；更准确地说，是驱动程序与执行器进程在既定资源内持续协作完成一个或多个作业。

![Image result for rdd
dag](media/11_monitoring_and_optimization/media/image4.jpeg)

图例 11‑4阶段划分

基本上，任何数据处理工作流都可以定义为读取数据源，然后应用一系列转换，最后以不同方式实现结果，转换创建RDD之间的依赖关系，通常被分类为“窄”和“宽”：

![alt](media/11_monitoring_and_optimization/media/image5.jpeg)

图例 11‑5两种转换关系

  - 窄依赖

<!-- end list -->

  - > 父RDD的每个分区被子RDD的最多一个分区使用

  - > 允许在一个集群节点上进行流水线的执行

  - > 故障恢复更有效，因为只有丢失的父分区需要重新计算

<!-- end list -->

  - 宽依赖

<!-- end list -->

  - > 父RDD的每个分区被子RDD的多个分区使用

  - > 要求所有父分区的数据可用，并在节点间进行洗牌

  - > 如果某个分区丢失了所有的祖先，则需要完整的重新计算

对于窄依赖，父RDD的每个分区由子RDD的最多一个分区使用，这意味着任务可以在本地执行，不必进行洗牌，例如map、flatMap、filter和sample等操作；对于宽依赖，多个子分区可能取决于父RDD的一个分区，这意味着必须对数据进行跨分区的洗牌，除非父RDD进行了散列分区，例如sortByKey、reduceByKey、groupByKey、cogroupByKey、join和cartesian等。由于采用了延迟评估技术，调度器将能够在提交作业之前优化阶段，窄依赖的操作被放到一个阶段，根据分区挑选连接算法，尽量减少洗牌，重用以前缓存的数据。将DAG分成几个阶段，通过打破洗牌边界处DAG创建的阶段。

![http://datastrophic.io/content/images/2016/03/Making-Stages-from-DAG--2-.png](media/11_monitoring_and_optimization/media/image6.png)

图例 11‑6通过打破Shuffle边界处的DAG创建Stage

### 11.3.3 实例分析

下面通过字数计数示例了解Spark应用程序的工作原理，可以在Spark交互界面中输入下面所示的代码。示例代码产生了wordcount，其定义了当调用动作时将使用的RDD有向无环图。在RDD上的操作创建新的RDD，它返回到其父母从而创建一个有向无环图，可以使用toDebugString打印出这个RDD谱系，如下所示。

```scala
scala> val file= sc.textFile("/usr/local/spark/README.md").cache()
file: org.apache.spark.rdd.RDD[String] = /root/data/11-0.txt
MapPartitionsRDD[1] at textFile at <console>:24
scala> val wordcount = file.flatMap(line => line.split(" ")).map(word
=> (word, 1)).reduceByKey(_ + _)
wordcount: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4]
at reduceByKey at <console>:26
scala> wordcount.toDebugString
res3: String =
(2) ShuffledRDD[4] at reduceByKey at <console>:26 []
+-(2) MapPartitionsRDD[3] at map at <console>:26 []
| MapPartitionsRDD[2] at flatMap at <console>:26 []
| /usr/local/spark/README.md MapPartitionsRDD[1] at textFile at
<console>:24 []
| CachedPartitions: 2; MemorySize: 11.3 KB; ExternalBlockStoreSize: 0.0
B; DiskSize: 0.0 B
| /usr/local/spark/README.md HadoopRDD[0] at textFile at
<console>:24 []
scala> wordcount.collect()
res1: Array[(String, Int)] = Array((package,1), (this,1),
(Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version),1),
(Because,1), (Python,2),
(page](http://spark.apache.org/documentation.html).,1), (cluster.,1),
(its,1), ([run,1), (general,3), (have,1), (pre-built,1), (YARN,,1),
(locally,2), (changed,1), (locally.,1), (sc.parallelize(1,1), (only,1),
(several,1), (This,2), (basic,1), (Configuration,1), (learning,,1),
(documentation,3), (first,1), (graph,1), (Hive,2), (info,1),
(["Specifying,1), ("yarn",1), ([params]`.,1), ([project,1),
(prefer,1), (SparkPi,2), (<http://spark.apache.org/>,1), (engine,1),
(version,1), (file,1), (documentation,,1), (MASTER,1), (example,3),
(["Parallel,1), (are,1), (params,1), (scala>,1), (DataFrames,,1),
(provides,...
```

代码 11.3
第一个RDD `file` 是通过 `sc.textFile()` 创建的 `HadoopRDD`；这条谱系上的最后一个RDD `wordcount` 则是由 `reduceByKey()` 产生的 `ShuffledRDD`。如图例 11‑7 所示，左侧展示的是 `wordcount` 对应的逻辑DAG，图中小方块表示各RDD的分区；当调用 `collect` 这类动作时，Spark调度器会在此基础上生成对应的物理执行计划（Physical Execution Plan）并把它拆分为可提交的阶段和任务。

![](media/11_monitoring_and_optimization/media/image7.jpeg)

图例 11‑7创建一个物理执行计划

调度器会根据依赖关系把DAG拆成多个阶段。由于窄依赖通常不需要跨节点重分布数据，因此往往会被合并到同一个阶段里执行；而一旦出现 `reduceByKey` 这类引入Shuffle的操作，就会在此处形成新的阶段。这个例子最终得到两个阶段：前面的 `textFile`、`flatMap`、`map` 属于第一个阶段，生成 `ShuffledRDD` 及其后续动作则进入第二个阶段。

![](media/11_monitoring_and_optimization/media/image8.jpeg)

图例 11‑8通过集群管理器启动任务

每个阶段是由任务组成，其基于RDD的分区，它将并行执行相同的计算，调度程序将任务集提交给任务调度程序，通过集群管理器启动任务，下图显示了示例Hadoop集群中的Spark应用程序：

![](media/11_monitoring_and_optimization/media/image9.jpeg)

图例 11‑9Hadoop集群中的Spark应用程序

然后，可以使用Spark
Web界面来查看Spark应用程序的行为和性能，网址为<http://localhost:4040>，这是运行单词计数作业后的Web
UI的屏幕截图（图例 11‑10）。在Jobs选项卡下，将看到已安排或运行的作业列表，在此示例中是计数的collect作业，Jobs页面显示作业、阶段和任务进度。

![](media/11_monitoring_and_optimization/media/image10.jpeg)

图例 11‑10 Jobs页面显示作业、阶段和任务进度

可以点击进入到Job 0的详细信息界面，可以看到DAG的行为，可以对照一下上面的分析结果：

![](media/11_monitoring_and_optimization/media/image11.jpeg)

图例 11‑11 Job 0的详细信息界面

在Stages选项卡下，可以看到阶段的详细信息，以下是单词计数作业的阶段页面，Stage0以阶段管道中的最后一个RDD转换命名，并且Stage
1以动作collect命名。

![](media/11_monitoring_and_optimization/media/image12.jpeg)

图例 11‑12阶段的详细信息

可以在Storage选项卡中查看缓存的RDD。

![](media/11_monitoring_and_optimization/media/image13.jpeg)

图例 11‑13查看缓存的RDD

在Executors选项卡下，可以看到每个执行器的处理和存储。可以通过单击Thread Dump链接来查看线程调用堆栈。

![](media/11_monitoring_and_optimization/media/image14.jpeg)

图例 11‑14Executors选项卡

## 11.4 洗牌机制

可以把洗牌（Shuffle）理解为一次“跨分区重组数据”的过程。当上游计算结果需要按照新的键分布重新汇集，例如 `reduceByKey`、`groupByKey`、`join`、`repartition` 这类操作出现时，Spark就必须把部分数据从一个执行器传输到另一个执行器。由于这个过程通常会伴随网络传输、磁盘读写、序列化与反序列化，所以它往往是性能瓶颈最集中的环节之一。下面这幅图借用了经典MapReduce流程来帮助理解“洗牌发生在前后两个计算阶段之间”。

![](media/11_monitoring_and_optimization/media/image15.tiff)

图例 11‑15 洗牌阶段是介于Map阶段和Reduce阶段之间

在分布式系统里，洗牌很难完全避免。原因很简单：数据最初是分散在不同分区、不同节点上的，而某些计算又要求“相同键的数据必须被汇集到一起”。一旦需要重新分布这些数据，就会发生洗牌。在Spark里，阶段划分本身就与这种依赖关系紧密相关，因此只要作业里存在宽依赖，洗牌通常就是绕不过去的成本。

另一个常见性能问题是数据倾斜，也就是某些键、某些分区明显比其他分区承载更多数据。数据倾斜往往会在洗牌阶段被放大，最终表现为个别任务特别慢、内存压力异常、磁盘溢写增多，甚至拖慢整个Stage。因此，在讨论Spark优化之前，先把Shuffle机制和数据倾斜之间的关系看清楚，通常比直接改参数更有效。

![park
Shuffle设计](media/11_monitoring_and_optimization/media/image16.jpeg)

图例 11‑16Shuffle操作

想象一下，有一张电话详细记录列表，想要计算每天发生的通话量。这样可以将日期设置为键，对于每个记录（即每个呼叫）将增加1作为值，之后汇集每个键的值，这将是问题的答案，即每一天的记录总量。但是，当将数据存储在集群中时，如何汇集存储在不同机器上的相同键的值？唯一的方法是使相同键的所有值在同一台机器上，之后可以汇集出来。

有许多不同的任务需要整个集群中的数据进行洗牌，例如join操作，要在字段“id”上连接两个表，必须确保所有的数据存储在相同块中。想象一下，整数键的范围从1到1000000。通过将数据存储在相同的块中，例如将两个表中的键为1到100之间的值都存储在单个分区或块中，而不是对第一个表的每个分区都遍历整个第二个表。这样可以直接将一个表的分区加入到另一个分区，因为知道1到100键的对应值只存储在这两个分区中，为了实现这两个表应该具有相同数量的分区，这样的连接将需要更少的计算，所以现在可以了解洗牌的重要性。

为便于说明，下面暂时沿用 MapReduce 风格的命名：把发送数据的一侧称为 “Mapper”，把接收并聚合数据的一侧称为 “Reducer”。对应到Spark内部，实现细节会更复杂，但这种类比足够帮助我们理解数据在洗牌过程中是如何移动的。与Shuffle直接相关的两个常见压缩参数是：

  - spark.shuffle.compress：引擎是否会压缩Shuffle输出

  - spark.shuffle.spill.compress：是否压缩中间Shuffle溢出文件

这两个参数的默认值通常都是 `true`，并且都会使用 `spark.io.compression.codec` 指定的编解码器。对大多数作业来说，先保持默认压缩设置是合理的；真正更值得关注的，往往是分区数、序列化方式、是否发生数据倾斜，以及是否出现了不必要的Shuffle。旧资料里常会展开讨论不同Shuffle实现的历史差异，但对Spark 4.x读者来说，更实用的重点是先读懂“哪里发生了Shuffle、为什么发生、代价有多大”，再决定是否调参数。

## 11.5 内存管理

本节将介绍Spark中的内存管理，然后讨论用户可以采取的具体策略，以便在应用程序中更有效地使用内存。具体来说，将介绍如何确定对象的内存使用情况，以及如何改进数据结构，或通过以串行格式存储数据。然后将介绍调整Spark的缓存大小和Java垃圾回收器。

Spark中的堆内存，通常可以先从两类用途来理解：执行内存（Execution Memory）和存储内存（Storage Memory）。前者主要服务于Shuffle、Join、排序、聚合这类运行期计算；后者主要用于缓存RDD/DataFrame分区、广播变量以及部分中间结果。

在统一内存模型下，两者共享同一块由Spark管理的内存区域 `M`。当执行侧压力较小时，存储侧可以占用更多空间；当执行侧需要更多空间时，又可以向存储侧借用一部分容量。这里常见的 `R` 可以理解为存储区域中的一个保留阈值，用来帮助缓存块尽量避免被过早驱逐。这个设计的核心目标很简单：让不缓存数据的应用能把更多空间用于执行，让大量依赖缓存的应用也至少保留一部分稳定的存储空间，同时尽量减少用户手动调参的负担。常见的两个配置如下：

（1）spark.memory.fraction

表示M的大小，默认为0.6，剩余的空间（40％）保留用于用户数据结构，Spark中的内部元数据，并且在稀疏和异常大的记录情况下保护OOM（Out
Of Memory）错误。

（2）spark.memory.storageFraction

表示R的大小作为M，默认为0.5。R是M内的存储空间，其中缓存的块免于被执行驱逐。

从Spark 1.6.0开始，Spark把早期较固定的内存划分方式切换到了统一内存模型（UnifiedMemoryManager）。如果你在阅读更老的资料，可能还会看到“Legacy”内存管理的说法；那更多是历史背景。对Spark 4.x读者而言，本书建议把注意力放在统一内存模型本身，也就是“执行内存与存储内存共享同一块可动态伸缩的区域”这一核心机制上。下图展示了这种模型的基本结构：

![](media/11_monitoring_and_optimization/media/image17.jpeg)

图例 11‑17 3个主要的内存区域

可以在图表上看到3个主要的内存区域：

  - 保留内存

这是系统保留的内存，其大小是被硬编码在Spark发布程序中，不可以调整的。从Spark
1.6.0起，它的值为300MB，这意味300MB的内存不参与Spark内存区域大小的计算，如果Spark没有重新编译或设置spark.testing.reservedMemory参数，保留内存的大小不能以任何方式改变。请注意，这个内存只被称为“Reserved”，实际上它并没有被Spark使用，但它设置了可以为Spark分配使用的限制。无法使用所有Spark的Java堆来缓存数据，因为这个保留部分将保持备用（实际上，它会存储大量的Spark内部对象）。如果不给Spark执行器至少1.5
\* 300MB= 450MB堆内存，将显示“please use larger heap size”错误消息。

  - 用户内存

这是在Spark
Memory分配后保留的内存池，完全由用户决定以任何方式使用，可以将数据结构存储在这里用于RDD转换，例如可以重写Spark聚合，通过使用mapPartitions转换维护散列表来以便此聚合运行，这将消耗所谓的用户内存。在Spark
1.6.0中，此内存池的大小可以计算为：

(Java Heap – Reserved Memory) \* （1.0 - spark.memory.fraction）

公式 11‑1

默认值等于：

（Java Heap - 300MB）\* 0.25

公式 11‑2

例如，使用4GB
Java堆，将拥有949MB的用户内存。另外，这是用户内存，完全用户决定将存储在这个内存中的数据，以及如何存储，Spark完全不会考虑在用户内存中做什么，是否遵守这个边界。如果在用户代码中不遵守此边界可能会导致OOM错误。

  - Spark内存

最后，这是由Spark管理的内存池。它的大小可以计算为：

(Java Heap – Reserved Memory) \* （spark.memory.fraction）

公式 11‑3

使用Spark 1.6.0默认值为：

（Java Heap - 300MB）\* 0.75

公式 11‑4

例如，如果Java进程的堆为4GB，这个池的大小将是2847MB。整个池分为2个区域：存储内存和执行内存，它们之间的边界由spark.memory.storageFraction参数设置，默认为0.5。这种新的内存管理方案的优点是，这个边界不是静态的，而在内存压力的情况下，边界将被移动，即一个区域将通过借用另一个空间来增长。稍后会讨论关于移动这个边界，现在关注这两个内存如何被使用：

（1）存储内存

此池用于存储Spark缓存数据和为了临时空间序列化数据的展开，而且所有的广播变量都存储在此缓存块中，如果没有足够的内存来适应整个展开的分区，它将直接将其放置到驱动程序中，如果持久性级别允许这样做，所有广播变量都存储在具有MEMORY\_AND\_DISK持久性级别的缓存中。

（2）执行内存

此内存池用于存储执行Spark任务期间所需的对象，例如它用于Map阶段在内存中存储洗牌中间缓冲区，也用于存储散列集合的散列表。如果没有足够的内存可用，此池还支持在磁盘上溢出，但是此池的块不能被其他线程（任务）强制驱逐。

现在关注存储内存和执行内存之间的移动边界。由于执行内存的性质，不能强制地从此池中驱逐内存块，因为这是中间计算中使用的数据，并且如果未找到引用的块，则需要此块的进程将失败。但是存储内存不是这样，它只是存储在内存中的块缓存，可以通过更新块元数据从那里驱逐该块，实际上块被驱逐到硬盘上（或简单地删除），当访问此块时Spark尝试从硬盘中读取，或重新计算当持久性级别不允许溢出在硬盘上。所以，可以强制地从存储内存中取出块，但是不能从执行内存中执行相同操作。什么时候执行内存可以从存储内存中借用一些空间？
这发生在以下任一方面：

（1）存储内存池中有可用空间，即缓存块没有使用所有可用的内存。然后，它只会减少存储内存池大小，从而增加执行内存池。

（2）存储内存池大小超过了初始存储内存区域大小，并且具有所有这些空间。这种情况会导致来自存储内存池的强制驱逐，除非它达到其初始大小。

反过来，只有在执行内存池中有可用空间可用时，存储内存池才能从执行内存池中借用一些空间，初始化存储区域大小计算为：

"Spark Memory" \* spark.memory.storageFraction

等于：

("Java Heap"–"Reserved Memory") \* spark.memory.fraction \*
spark.memory.storageFraction

使用默认值，这等于：

("Java Heap"– 300MB) \* 0.75 \* 0.5 =("Java Heap"–300MB) \* 0.375

对于4GB堆，这将导致初始存储区域中的1423.5MB的RAM。这意味着如果使用Spark缓存，并且执行器上缓存的数据总量与初始存储区域大小至少相同，那么保证存储区域大小至少等于其初始大小。但是，如果在填写存储内存区域之前，执行内存区域已经超出其初始大小，则无法强制地从执行内存中删除条目，因此在执行保留其块时，最终会导致更小的存储区域在内存中。

## 11.6 优化策略

优化并不是“把所有参数都调一遍”，而是先判断瓶颈落在什么地方：CPU、网络、内存、序列化，还是数据分布。很多 Spark 作业在数据能够放入内存后，真正的成本会集中到 Shuffle、对象开销和网络传输上。本节主要讨论三件最常见的事情：序列化、内存使用以及并行度和数据局部性。

### 11.6.1 数据序列化 

序列化直接影响网络传输、磁盘落盘和缓存占用。对象越大、编码越低效，Shuffle 和持久化的成本就越高。因此，当任务明显受网络或内存压力影响时，序列化往往是最先值得检查的一环。Spark 常见的两类序列化方式如下：

  - Java序列化

默认情况下，Spark 可以使用 Java 的 `ObjectOutputStream` 体系序列化对象。它的优点是兼容性强，只要对象实现了 `java.io.Serializable` 基本就能工作；缺点是编码通常较大、速度也偏慢。

  - Kryo序列化

Spark 也支持 Kryo。与 Java 序列化相比，Kryo 通常更紧凑、也更高效，因此在网络密集型或缓存密集型场景里更值得优先考虑。它的代价是：为了获得最好效果，通常需要提前注册自定义类型。

如果要显式切换到 Kryo，可以在 `SparkConf` 中设置 `spark.serializer=org.apache.spark.serializer.KryoSerializer`。这个配置不仅影响节点之间的数据传输，也会影响RDD落盘时的序列化方式。对于包含简单类型、数组或字符串的常见数据结构，Spark 内部已经做了不少优化；但如果应用里存在大量自定义对象，显式切换到 Kryo 往往仍然值得尝试。

Spark自动包含AllScalaRegistrar，涵盖许多常用的核心Scala类的Kryo序列化程序。要使用Kryo注册自己的自定义类，使用registerKryoClasses方法。

val conf = new SparkConf().setMaster(...).setAppName(...)

conf.registerKryoClasses(Array(classOf\[MyClass1\],
classOf\[MyClass2\]))

val sc = new SparkContext(conf)

代码 11.4
如果对象很大，还可能需要调高 `spark.kryoserializer.buffer`，确保缓冲区足以容纳最大的单个对象。即使不注册自定义类，Kryo 仍然能工作；只是它需要在字节流中额外保存完整类名，效率会打一些折扣。

### 11.6.2 内存调优

内存调优可以先抓住三个问题：对象本身占了多少空间、这些对象访问起来是否高效，以及垃圾回收是否已经开始反过来拖慢任务。Java/Scala 对象虽然使用方便，但其真实内存开销往往明显大于字段本身，常见原因包括：

（1）每个不同的Java对象都有一个头，大约是16个字节，包含一个指向其类的指针，对于一个数据很少的对象（比如一个Int字段），其可以比数据大。

（2）Java
字符串在原始字符串数据上有大约40字节的开销（因为它将存储在Char数组中并保留额外的数据，例如长度），并且由于String的内部使用UTF-16编码，其将每个字符存储为两个字节，因此一个10个字符的字符串可以容易地消耗60个字节。

（3）通用集合类使用链接的数据结构，如HashMap和LinkedList，其中每个条目都有一个包装器对象，例如Map.Entry，该对象不仅具有头部，还包括指针指向列表中的下一个对象，通常为8个字节。

（4）原始类型的集合通常将它存储为封装对象，如java.lang.Integer。

  - 确定内存消耗

估算数据集真实占用内存，最直接的方式通常是把它缓存起来，再到 Spark UI 的 `Storage` 页面查看结果。若要粗估某类对象或广播变量的体积，可以使用 `SizeEstimator.estimate`。这对比较不同数据结构方案、判断广播是否过大都很有帮助。

  - 调整数据结构

减少内存占用的第一步，通常不是先改参数，而是先减少对象层面的额外开销，例如避免过多包装对象、深层嵌套结构和高指针密度的数据结构。常见做法包括：

（1）设计数据结构采用对象数组和原始类型，而不是标准的Java或Scala集合类，例如HashMap。fastutil库为原始类型提供方便的收集类，这与Java标准库兼容。

（2）如果可能的话，避免带有很多小的对象和指针嵌套的结构。

（3）考虑使用数字ID或枚举对象而不是键的字符串。

（4）如果RAM小于32
GB，请设置JAVA虚拟机标志-XX:+UseCompressedOops，使指针为四个字节而不是八个字节，可以在spark-env.sh添加这些选项。

  - 序列化存储

如果对象结构已经尽量压缩，但缓存后仍然太占内存，可以考虑使用序列化持久化级别，例如 `MEMORY_ONLY_SER`。这样每个RDD分区会以大块字节数组形式缓存，通常能明显减小占用；代价是访问时需要反序列化，因此 CPU 成本会增加。若走这条路线，通常优先配合 Kryo 使用。

  - 垃圾收集调整

垃圾回收问题通常出现在两类场景：对象数量太多，或者执行内存与缓存内存互相挤压。要记住的核心原则很简单：GC 成本与对象数量强相关，所以减少小对象、减少包装层级、必要时使用序列化缓存，往往比单纯调 GC 参数更先见效。

调整 GC 的第一步是先拿到统计信息，而不是盲目改 JVM 参数。可以把 `-verbose:gc`、`-XX:+PrintGCDetails` 和 `-XX:+PrintGCTimeStamps` 加到 Java 选项里，例如：

./bin/spark-submit --name "My app" --master local\[4\] --conf
spark.eventLog.enabled=false

```text
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps" myApp.jar
```

代码 11.5
下次运行作业时，就可以在工作节点日志里看到 GC 信息，而不只是驱动程序日志。理解这些日志时，可以先抓住一个简化模型：Java 堆通常分为 Young 和 Old 两大区域，前者更适合短命对象，后者保留生命周期较长的对象。Spark 调优的目标之一，就是尽量让任务临时对象在 Young 区里完成回收，而不要过早把压力推到 Old 区。

从实践上看，如果在单个任务完成前就频繁发生 Full GC，通常说明执行内存不足；如果 Minor GC 特别频繁，则可能说明 Young 区太小、对象创建过于密集，或者缓存与执行内存互相挤占。处理顺序通常是：先减少对象数量和缓存体积，再考虑调整 `spark.memory.fraction`、Young 区大小或具体 GC 选项。

如果日志里显示 Old 区长期接近打满，常见做法包括：降低 `spark.memory.fraction` 以减少缓存挤占、减少对象数量、必要时使用 G1GC，并结合执行器堆大小观察 `NewRatio` 或 Young 区大小是否合理。这里没有一套放之四海而皆准的参数模板，真正有效的方式还是看 GC 日志、看 Spark UI、改一处、再复测一处。

### 11.6.3 其他方面

如果并行度设置得太低，再多的机器也跑不满；如果并行度过高，又会引入额外调度开销。Spark 会为很多操作推断默认并行度，例如文件输入、`groupByKey`、`reduceByKey` 等；必要时也可以通过参数或 `spark.default.parallelism` 显式覆盖。经验上，常见起点是让每个 CPU 内核对应 2 到 3 个任务，再结合任务耗时和 Shuffle 情况继续调。

有时候看到 `OutOfMemory`，并不意味着整个RDD放不下，而是某个单独任务的工作集太大，例如 `groupByKey` 或 `join` 在单个分区上聚集了过多数据。这时最直接的办法往往是提高并行度、缩小单任务输入规模，或者从源头上减少数据倾斜与不必要的聚合压力。

广播变量可以显著减少任务闭包大小和重复网络传输。如果任务依赖驱动程序上的大对象，例如静态字典、维表快照或规则集，通常应优先考虑把它们做成广播变量。Spark UI 和日志里也能看到任务序列化大小，这些信息有助于判断闭包是否已经过大。

  - 数据局部性

数据局部性会直接影响作业性能。原则很简单：尽量让计算靠近数据，而不是让大块数据在网络里来回移动。通常代码比数据小得多，所以 Spark 的调度策略会尽量优先选择“更靠近数据”的执行位置。

Spark 会按从近到远的顺序区分几个局部性级别：

> （1）PROCESS\_LOCAL：数据和代码在同一个 Java 进程内，这是最理想的情况。
> 
> （2）NODE\_LOCAL：数据和代码位于同一台机器上，但不在同一进程里。
> 
> （3）NO\_PREF：数据从哪里读都差不多，没有明显局部性偏好。
> 
> （4）RACK\_LOCAL：数据在同一机架内的其他机器上，需要经过局域网络传输。
> 
> （5）ANY：数据在更远的网络位置上，代价最高。

Spark 会优先等待更好的局部性机会，但不会无限等待。也就是说，它会先尝试把任务调度到最合适的位置；如果迟迟没有合适资源，才退而求其次接受更差的局部性。每个级别之间的等待时间都可以配置，但在大多数场景里默认值已经足够，只有在任务很长、数据很大且跨网络代价明显时，才值得专门调整这些参数。

## 11.7 最佳实践

### 11.7.1 系统配置

Spark提供三个位置来配置系统：

> （1）Spark属性：控制了大多数应用程序参数，可以使用SparkConf对象或通过Java系统属性进行设置。
> 
> （2）环境变量：可用于通过conf/spark-env.sh每个节点上的脚本来设置每台计算机的设置，例如IP地址。
> 
> （3）日志记录：可以通过配置log4j.properties。

#### 11.7.1.1 Spark属性

Spark属性控制大多数应用程序设置，并针对每个应用程序单独配置，这些属性可以直接通过SparkConf设置并传递给SparkContext。SparkConf允许配置一些常用属性，例如主URL和应用程序名称，以及通过该set()方法设置任意键值对，例如可以用两个线程初始化一个应用程序，如下所示：

val conf = new SparkConf()

.setMaster("local\[2\]")

.setAppName("CountingSheep")

val sc = new SparkContext(conf)

代码 11.6
这里使用local
\[2\]意味着两个线程，表示最小并行性，这可以帮助检测只有在分布式环境中运行时才存在的错误。指定某个持续时间的属性应该使用时间单位进行配置，以下格式被接受：

25ms (milliseconds)

5s (seconds)

10m or 10min (minutes)

3h (hours)

5d (days)

1y (years)

代码 11.7
指定字节大小的属性应该使用大小单位进行配置。以下格式被接受：

1b (bytes)

1k or 1kb (kibibytes = 1024 bytes)

1m or 1mb (mebibytes = 1024 kibibytes)

1g or 1gb (gibibytes = 1024 mebibytes)

1t or 1tb (tebibytes = 1024 gibibytes)

1p or 1pb (pebibytes = 1024 tebibytes)

代码 11.8
虽然没有单位的数字通常被解释为字节，但少数解释为KiB或MiB，请参阅各个配置属性的文档，在可能的情况下指定单位是可取的。在某些情况下，可能希望避免在SparkConf中对某些配置进行硬编码，例如如果想用不同的Spark集群或不同的内存运行相同的应用程序，Spark允许简单地创建一个空的conf：

val sc = new SparkContext(new SparkConf())

代码 11.9
然后可以在运行时提供配置值：

./bin/spark-submit --name "My app" --master local\[4\] --conf
spark.eventLog.enabled=false

```text
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps" myApp.jar
```

代码 11.10
Spark
shell和spark-submit工具支持两种动态加载配置的方式，第一个是命令行选项如上所示--master。spark-submit可以使用该--conf
标志接受任何Spark属性，但对于启动Spark应用程序的属性使用特殊标志。运行./bin/spark-submit
--help将显示这些选项的完整列表。bin/spark-submit还将读取配置选项conf/spark-defaults.conf，其中每行包含一个由空格分隔的键和值，例如：

spark.master spark://5.6.7.8:7077

spark.executor.memory 4g

spark.eventLog.enabled true

spark.serializer org.apache.spark.serializer.KryoSerializer

代码 11.11
指定为标志或属性文件中的任何值都将传递到应用程序，并与通过SparkConf指定的值合并。直接在SparkConf上设置的属性具有最高的优先级，然后将标志传递给spark-submit或spark-shell，最后选择spark-defaults.conf文件中的选项。自早期版本的Spark以来，一些配置键已被重命名；在这种情况下，旧键名仍然可以接受，但优先级低于新键的任何实例。

Spark属性主要可以分为两种：一种是与部署相关的，比如“spark.driver.memory”，spark.executor.instances。对于这些属性，在通过SparkConf编程进行设置，而运行时可能不会受到影响，或者行为取决于选择的集群管理器和部署模式，因此建议通过配置文件或spark-submit命令行选项进行设置；另一个主要与Spark运行时控制有关，比如spark.task.maxFailures，这种属性可以用任何方式设置。

Spark提供了应用程序Web界面
http://\<driver\>:4040，其列出了“Environment”选项卡中的Spark属性，这里可以检查以确保属性设置正确。请注意，只有通过spark-defaults.conf和SparkConf明确规定或在命令行中指定的值才会出现。对于所有其他配置属性，可以假定使用默认值。大多数控制内部设置的属性都有合理的默认值，一些最常见的选项可以参考Spark官方技术文档。

#### 11.7.1.2 环境变量

某些Spark设置可以通过环境变量进行配置，这些环境变量是从安装Spark目录（或Windows环境上的conf/spark-env.cmd）的脚本conf/spark-env.sh中读取的。在Standalone和Kubernetes模式下，该文件可以为机器提供特定信息，例如主机名。在运行本地Spark应用程序或提交脚本时，它也是来源。请注意，在默认安装情况下，conf/spark-env.sh不存在，但是可以复制conf/spark-env.sh.template以创建它，确保脚本是可执行文件。以下变量可以在spark-env.sh中设置：

| 环境变量                    | 含义                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| JAVA\_HOME              | Java的安装位置，如果它不在默认路径。                                                                     |
| PYSPARK\_PYTHON         | Python二进制可执行文件，用于PySpark在驱动程序和工作节点中，建议显式设置为python3（如python3.10+），如果已设置spark.pyspark.python属性则以其为准 |
| PYSPARK\_DRIVER\_PYTHON | Python二进制可执行文件，用于PySpark在驱动程序和工作节点中，默认为PYSPARK\_PYTHON，如果已设置spark.pyspark.python属性优先     |
| SPARKR\_DRIVER\_R       | R二进制可执行文件，用于SparkR shell（默认为R），如果已设置spark.r.shell.command属性则优先                           |
| SPARK\_LOCAL\_IP        | 要绑定计算机的IP地址。                                                                             |
| SPARK\_PUBLIC\_DNS      | Spark程序的主机名将通告给其他机器。                                                                     |

表格 11‑1spark-env.sh中设置的变量

除上述之外，还可以选择设置Spark
Standalone集群脚本，例如每台计算机上使用的内核数量和最大内存。由于spark-env.sh是一个交互命令脚本，其中一些可以通过程序设置，例如可以通过查找特定网络接口的IP来进行计算SPARK\_LOCAL\_IP。在cluster模式中，在YARN上运行Spark时，需要使用conf/spark-defaults.conf文件中的spark.yarn.appMasterEnv.\[EnvironmentVariableName\]属性设置环境变量。设置的环境变量spark-env.sh不会反映在cluster模式中的YARN
Application Master进程中。

#### 11.7.1.3 设置日志

Spark使用log4j进行日志记录，可以通过在conf目录中添加log4j.properties文件来配置它，可以复制位于conf目录中log4j.properties.template文件产生，原来文件的内容显示如下：

```bash
# Set everything to be logged to the console
log4j.rootCategory=ERROR, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p
%c{1}: %m%n
# Set the default spark-shell log level to ERROR. When running the
spark-shell, the
# log level for this class is used to overwrite the root logger's log
level, so that
# the user can have different defaults for the shell and regular Spark
apps.
log4j.logger.org.apache.spark.repl.Main=ERROR
# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark_project.jetty=ERROR
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=ERROR
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=ERROR
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
# SPARK-9183: Settings to avoid annoying messages when looking up
nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```

代码 11.12
把log4j.rootCategory=INFO, console改为log4j.rootCategory=WARN,
console即可抑制Spark把INFO级别的日志打到控制台上。如果要显示全面的信息，则把INFO改为DEBUG。如果希望一方面把代码中的println打印到控制台，另一方面又保留Spark
本身输出的日志，可以将它输出到日志文件中。配置根Logger，其语法为：

log4j.rootLogger = \[level\],appenderName,appenderName2,...

代码 11.13
level是日志记录的优先级，分为OFF、TRACE、DEBUG、INFO、WARN、ERROR、FATAL、ALL。Log4j建议只使用四个级别，优先级从低到高分别是DEBUG、INFO、WARN、ERROR。通过在这里定义的级别，可以控制到应用程序中相应级别的日志信息的开关，比如在这里定义了INFO级别，则应用程序中所有DEBUG级别的日志信息将不被打印出来。appenderName就是指定日志信息输出到哪个地方，可同时指定多个输出目的。配置日志信息输出目的地Appender，其语法为：

log4j.appender.appenderName = fully.qualified.name.of.appender.class

log4j.appender.appenderName.optionN = valueN

代码 11.14
Log4j提供的appender有以下几种：

> （1）org.apache.log4j.ConsoleAppender，输出到控制台

\-Threshold = DEBUG：指定日志消息的输出最低层次

\-ImmediateFlush = TRUE：默认值是true,所有的消息都会被立即输出

\-Target = System.err：默认值System.out,输出到控制台（err为红色，out为黑色）

> （2）org.apache.log4j.FileAppender，输出到文件

\-Threshold = INFO：指定日志消息的输出最低层次

\-ImmediateFlush = TRUE：默认值是true,所有的消息都会被立即输出

\-File = C:\\log4j.log：指定消息输出到C:\\log4j.log文件

\-Append = FALSE:默认值true，将消息追加到指定文件中，false指将消息覆盖指定的文件内容

\-Encoding = UTF-8:可以指定文件编码格式

> （3）org.apache.log4j.DailyRollingFileAppender，每天产生一个日志文件

\-Threshold = WARN：指定日志消息的输出最低层次

\-ImmediateFlush = TRUE：默认值是true,所有的消息都会被立即输出

\-File = C:\\log4j.log：指定消息输出到C:\\log4j.log文件

\-Append = FALSE：默认值true,将消息追加到指定文件中，false指将消息覆盖指定的文件内容

\-DatePattern='.'yyyy-ww：每周滚动一次文件，即每周产生一个新的文件。还可以按用以下参数：

'.'yyyy-MM:每月

'.'yyyy-ww:每周

'.'yyyy-MM-dd:每天

'.'yyyy-MM-dd-a:每天两次

'.'yyyy-MM-dd-HH:每小时

'.'yyyy-MM-dd-HH-mm:每分钟

\-Encoding = UTF-8:可以指定文件编码格式

> （4）org.apache.log4j.RollingFileAppender，文件大小到达指定尺寸的时候产生一个新的文件

\-Threshold = ERROR：指定日志消息的输出最低层次

\-ImmediateFlush = TRUE：默认值是true,所有的消息都会被立即输出

\-File = C:/log4j.log：指定消息输出到C:/log4j.log文件

\-Append = FALSE：默认值true,将消息追加到指定文件中，false指将消息覆盖指定的文件内容

\-MaxFileSize = 100KB：后缀可以是KB\\MB\\GB。在日志文件到达该大小时，将会自动滚动。如：log4j.log.1

\-MaxBackupIndex = 2：指定可以产生的滚动文件的最大数

\-Encoding = UTF-8：可以指定文件编码格式

> （5）org.apache.log4j.WriterAppender，将日志信息以流格式发送到任意指定的地方

配置日志信息的格式，其语法为：

log4j.appender.appenderName.layout =
fully.qualified.name.of.layout.class

log4j.appender.appenderName.layout.optionN = valueN

Log4j提供的layout有以下几种：

> （6）org.apache.log4j.HTMLLayout，以HTML表格形式布局

\-LocationInfo = TRUE：默认值false,输出java文件名称和行号

\-Title=Struts Log Message：默认值Log4J Log Messages

> （7）org.apache.log4j.PatternLayout，可以灵活地指定布局模式

\-ConversionPattern = %m%n：格式化指定的消息

> （8）org.apache.log4j.SimpleLayout，包含日志信息的级别和信息字符串
> 
> （9）org.apache.log4j.TTCCLayout，包含日志产生的时间、线程、类别等信息)
> 
> （10）org.apache.log4j.xml.XMLLayout，以XML形式布局

\-LocationInfo = TRUE：默认值false，输出java文件名称和行号

Log4J采用类似C语言中的printf函数的打印格式格式化日志信息，打印参数如下：

%m 输出代码中指定的消息

%p 输出优先级，即DEBUG\\INFO\\WARN\\ERROR\\FATAL

%r 输出自应用启动到输出该log信息耗费的毫秒数

%c 输出所属的类目,通常就是所在类的全名

%t 输出产生该日志事件的线程名

%n 输出一个回车换行符，Windows平台为“\\r\\n”，Unix平台为“\\n”

%d 输出日志时间点的日期或时间，默认格式为ISO8601，也可以在其后指定格式 ， 如%d{yyyy年MM月dd日
HH:mm:ss,SSS}，输出类似：2012年01月05日 22:10:28,921

%l 输出日志事件的发生位置，包括类目名、发生的线程，以及在代码中的行数，如Testlog.main(TestLog.java:10)

%F 输出日志消息产生时所在的文件名称

%L 输出代码中的行号

%x 输出和当前线程相关联的NDC(嵌套诊断环境),像java servlets多客户多线程的应用中

%% 输出一个"%"字符

可以在%与模式字符之间加上修饰符来控制其最小宽度、最大宽度、和文本的对齐方式，如：

%5c：输出category名称，最小宽度是5，category\<5，默认的情况下右对齐

%-5c：输出category名称，最小宽度是5，category\<5，"-"号指定左对齐,会有空格

%.5c：输出category名称，最大宽度是5，category\>5，就会将左边多出的字符截掉，\<5不会有空格

%20.30c:category名称\<20补空格，并且右对齐，\>30字符，就从左边较远输出的字符截掉

### 11.7.2 程序调优

#### 11.7.2.1 collect

当在RDD上调用 `collect` 时，数据会被拉回到Driver进程。如果结果集太大而无法放入Driver内存，就可能触发内存异常。因此，排查数据问题时更安全的做法通常是使用 `take`、`takeSample` 这类带上限的动作；另一种办法是先取到分区数组，再只查看单个分区的数据：

val parallel = sc.parallelize(1 to 9)

val parts = parallel.partitions

代码 11.15
然后创建一个更小的RDD，过滤掉除了单个分区以外的所有内容，从较小的RDD收集数据并遍历单个分区的值：

for(p \<- parts){

val idx = p.index

val partRDD = parallel.mapPartitionsWithIndex((index: Int, it:
Iterator\[Int\]) =\> if(index == idx) it else Iterator(), true)

val data = partRDD.collect

// 从单个分区中data包含所有的值，以数组的形式

}

代码 11.16
也可以使用foreachPartition操作：

parallel.foreachPartition(partition =\> {

partition.toArray

// 代码

})

代码 11.17
因为只有当分区中的数据足够小时，才会起到作用。可以使用coalesce方法随时增加分区数量：

rdd.coalesce(numParts, true)

代码 11.18
#### 11.7.2.2 count

当你不需要返回确切的行数时，不要使用count()，可以使用：

DataFrame inputJson = sqlContext.read().json(...);

if (inputJson.take(1).length == 0) {}

代码 11.19
代替使用：

if (inputJson.count() == 0) {}

代码 11.20
#### 11.7.2.3 迭代器列表

通常当读入一个文件时，要使用由某个分隔符分隔的每行中包含的各个值，分割分隔线是一项简单的操作：

newRDD = textRDD.map(line =\> line.split(","))

代码 11.21
但是这里的问题是返回的RDD将是迭代器组成的，想要的是调用split函数后获得的各个值，换句话说需要一个Array\[String\]不是Array\[Array\[String\]\]，为此将使flatMap方法：

```scala
scala> val mappedResults = mapped.collect ()
mappedResults: Array[Array[String]] = Array(Array(foo, bar, baz),
Array(larry, moe, curly), Array(one, two, three))
scala> val flatMappedResults = flatMapped.collect ();
flatMappedResults: Array[String] = Array(foo, bar, baz, larry, moe,
curly, one, two, three)
scala> println (mappedResults.mkString (" : ") )
[Ljava.lang.String;@2a70c8d5 : [Ljava.lang.String;@6d0ef6dc :
[Ljava.lang.String;@2936f48a
scala> println (flatMappedResults.mkString (" : ") )
foo : bar : baz : larry : moe : curly : one : two : three
```

代码 11.22
#### 11.7.2.4 groupByKey

正如所看到的，Map示例返回一个包含3个Array\[String\]实例的数组，而该flatMap调用返回了包含在一个数组中的各个值。假设有一个RDD项目，例如：

(3922774869,10,1)

(3922774869,11,1)

(3922774869,12,2)

(3922774869,13,2)

(1779744180,10,1)

(1779744180,11,1)

(3922774869,14,3)

(3922774869,15,2)

(1779744180,16,1)

(3922774869,12,1)

(3922774869,13,1)

(1779744180,14,1)

(1779744180,15,1)

(1779744180,16,1)

(3922774869,14,2)

(3922774869,15,1)

(1779744180,16,1)

(1779744180,17,1)

(3922774869,16,4)

...

代码 11.23
代表(id, age, count)，希望将这些行生成一个数据集，其中的每一行代表的是每个id的年龄分布（ID，age），这是唯一的，例如：

(1779744180, (10,1), (11,1), (12,2), (13,2) ...)

(3922774869, (10,1), (11,1), (12,3), (13,4) ...)

代码 11.24
这是代表(id,(age,count),age,count)…)，最简单的方法是首先聚合两个字段，然后使用groupBy：

rdd.map { case (id, age, count) =\> ((id, age), count) }.reduceByKey(\_
+ \_)

.map { case ((id, age), count) =\> (id, (age, count)) }.groupByKey()

代码 11.25
其中返回一个RDD\[(Long, Iterable\[(Int, Int)\])\]，对于上面的输入它将包含这两个记录：

(1779744180,CompactBuffer((16,3), (15,1), (14,1), (11,1), (10,1),
(17,1)))

(3922774869,CompactBuffer((11,1), (12,3), (16,4), (13,3), (15,3),
(10,1), (14,5)))

代码 11.26
但是如果有一个非常大的数据集，为了减少洗牌我们不应该使用groupByKey()，而是可以使用aggregateByKey()：

import scala.collection.mutable

val rddById = rdd.map { case (id, age, count) =\> ((id, age), count)
}.reduceByKey(\_ + \_)

val initialSet = mutable.HashSet.empty\[(Int, Int)\]

val addToSet = (s: mutable.HashSet\[(Int, Int)\], v: (Int, Int)) =\> s
+= v

val mergePartitionSets = (p1: mutable.HashSet\[(Int, Int)\], p2:
mutable.HashSet\[(Int, Int)\]) =\> p1 ++= p2

val uniqueByKey = rddById.aggregateByKey(initialSet)(addToSet,
mergePartitionSets)

代码 11.27
这将导致的结果为：

uniqueByKey: org.apache.spark.rdd.RDD\[(AnyVal,
scala.collection.mutable.HashSet\[(Int, Int)\])\]

代码 11.28
能够将值打印为：

```scala
scala> uniqueByKey.foreach(println)
(1779744180,Set((15,1), (16,3)))
(1779744180,Set((14,1), (11,1), (10,1), (17,1)))
(3922774869,Set((12,3), (11,1), (10,1), (14,5), (16,4), (15,3), (13,3)))
```

代码 11.29
洗牌可能是一个很大的瓶颈，以下是比groupByKey更好的推荐方法：combineByKey和foldByKey。

#### 11.7.2.5 reduceByKey

考虑编写一个转换，查找与每个键相对应的所有唯一字符串。一种方法是使用map将每个元素转换为一个Set，然后使用reduceByKey将这些Set组合：

rdd.map(kv =\> (kv.\_1, new Set\[String\]() + kv.\_2)) .reduceByKey(\_
++ \_)

代码 11.30
此代码导致大量不必要的对象创建，因为必须为每条记录分配一个新的Set。最好使用aggregateByKey()，可以更高效地执行聚合，就是尽量将聚合发生在Map阶段：

val zero = new collection.mutable.Set\[String\]()

rdd.aggregateByKey(zero)( (set, v) =\> set += v, (set1, set2) =\> set1
++= set2)

代码 11.31
#### 11.7.2.6 广播变量

Spark的难点之一是理解跨集群执行代码时变量和方法的范围和生命周期，如果RDD操作修改了范围之外的变量可能经常造成混淆​​。在下面的示例中，将查看foreach()用于增加计数器的代码，其他操作也会出现类似的问题。考虑以下简单的RDD元素求和，根据执行是否发生在同一个JAVA虚拟机中，这可能会有不同的表现。一个常见的例子是在local模式中运行或者将Spark应用程序部署到集群（例如，通过spark-submit
to YARN）：

var counter = 0

var rdd = sc.parallelize(data)

// Wrong: Don't do this\!\!

rdd.foreach(x =\> counter += x)

println("Counter value: " + counter)

代码 11.32
上述代码的行为是未定义的，并且可能无法按预期工作。为了执行作业，Spark将RDD操作的处理分解为任务，每个任务由执行器完成。在执行之前，Spark会计算任务的闭合。闭合是执行器在RDD上执行其计算的那些可见的变量和方法，例如代码中的foreach()。该闭合被序列化并发送给每个执行器。

如在集群环境，发送给每个执行器闭合中的变量现在被拷贝，因此当在foreach函数内引用counter()时，它不再是驱动程序节点上的counter()。驱动程序节点的内存中仍有一个counter()，但对于执行器来说是不可见的，执行器只能看到序列化后闭合的副本，因此counter()的最终值仍然为零，因为counter()上的所有操作都引用了序列化闭包内的值。

在本地模式下，foreach函数实际上将在与驱动程序相同的JAVA虚拟机内执行，并且会引用相同的原始计数器，并可能实际更新它。为了确保在这些场景中明确定义的行为，应该使用一个Accumulator。Spark中的累加器专门用于提供一种机制，用于在集群中安全地更新变量，当执行在工作节点之间被拆分时。

一般来说闭合结构，像循环或本地定义的方法，不应该被用来改变一些全局状态。Spark不会定义或保证从闭合外引用的对象的改变行为。这样做的一些代码可能在本地模式下工作，但是这种代码在分布式模式下的行为不可预期。在可用执行器上运行每个任务之前，Spark会计算任务的闭合。如果一个巨大的数组需要从Spark
闭合中，则此数组将通过闭包运送到每个Spark集群的工作节点上；如果有10个工作节点，每个工作节点10个分区，总共具有100个分区，则此数组将至少分配100次。如果使用broadcast方法，它将使用高效的p2p协议在每个节点上分发一次。

val array: Array\[Int\] = // some huge array

val broadcasted = sc.broadcast(array)

代码 11.33
还有一些RDD

val rdd: RDD\[Int\] =

代码 11.34
下面的代码，数组每次将与闭合传输。

rdd.map(i =\> array.contains(i))

代码 11.35
如果使用broadcasted，将会得到巨大的性能优势

rdd.map(i =\> broadcasted.value.contains(i))

代码 11.36
一旦向工作节点广播了该值，就不应该对其值进行更改，以确保每个节点具有完全相同的数据副本，修改后的值可能会发送到另一个节点，这会产生意外的结果。

如果RDD足够小以适应每个工作节点的内存，可以将其变成广播变量，并将整个操作转变为所谓的更大RDD的map-side连接。通过这种方式，更大的RDD根本不需要Shuffle。如果较小的RDD是维度表，这很容易发生。

val smallLookup = sc.broadcast(smallRDD.collect.toMap)

largeRDD.flatMap { case(key, value) =\>

smallLookup.value.get(key).map { otherValue =\>

(key, (value, otherValue))

}

}

代码 11.37
如果中等规模的RDD不能完全适应内存，但它的键集却可以。由于join操作会放弃大RDD中与小RDD中键没有匹配的所有元素，因此可以使用小RDD的键集在Shuffle之前执行此操作。如果有大量的条目被这种方式抛弃，则最终的Shuffle将需要传输很少的数据。

val keys = sc.broadcast(mediumRDD.map(\_.\_1).collect.toSet)

val reducedRDD = largeRDD.filter{ case(key, value) =\>
keys.value.contains(key) }

reducedRDD.join(mediumRDD)

代码 11.38
值得注意的是，这里的效率增益取决于实际filter操作减小多少RDD的尺寸。如果在这里减少的条目不多，可能因为小RDD中的键是大RDD的大部分，那么这种策略就没有什么作用。

#### 11.7.2.7 存储级别

仅仅是因为可以在存储器中缓存RDD，并不意味着应该盲目地这样做。取决于访问数据集的次数以及这样做所涉及的工作量，重新计算可能更快。毫无疑问，如果只是一次读取数据集，没有必要缓存数据集，那么它实际上会让你的工作变慢。从Spark
Shell可以看到缓存数据集的大小。通过默认，Spark将使用MEMORY\_ONLY级别cache()数据，MEMORY\_AND\_DISK\_SER可以帮助减少GC，并避免昂贵的重新计算。

| 存储级别                                  | 含义                                                                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| MEMORY\_ONLY                          | 将RDD作为反序列化的Java对象存储在JAVA虚拟机中。如果RDD不适合内存，则某些分区将不会被缓存，并会在每次需要时重新计算。这是默认级别。                                                              |
| MEMORY\_AND\_DISK                     | 将RDD作为反序列化的Java对象存储在JAVA虚拟机中。如果RDD不适合内存，请存储不适合磁盘的分区，并在需要时从中读取它们。                                                                      |
| MEMORY\_ONLY\_SER                     | 将RDD存储为序列化的 Java对象（每个分区一个字节的数组）。与反序列化的对象相比，这通常更节省空间，特别是在使用 [快速序列化器时](https://spark.apache.org/docs/4.1.1/tuning.html)，但需要更多的CPU密集型读取。 |
| MEMORY\_AND\_DISK\_SER                | 与MEMORY\_ONLY\_SER类似，但将不适合内存的分区溢出到磁盘上，而不是每次需要时重新计算它们。                                                                                 |
| DISK\_ONLY                            | 将RDD分区仅存储在磁盘上。                                                                                                                        |
| MEMORY\_ONLY\_2，MEMORY\_AND\_DISK\_2等 | 与上面的级别相同，但复制两个集群节点上的每个分区。                                                                                                             |
| OFF\_HEAP（实验）                         | 与MEMORY\_ONLY\_SER类似，但将数据存储在[堆内存储器中](https://spark.apache.org/docs/4.1.1/configuration.html#memory-management)。这需要启用堆堆内存。             |

表格 11‑2存储级别

以Tachyon的序列化格式存储RDD。与MEMORY\_ONLY\_SER相比，OFF\_HEAP减少了垃圾回收开销，并允许执行程序更小并共享内存池，使其在具有大堆或多个并发应用程序的环境中更具吸引力。此外，由于RDD驻留在Tachyon中，执行程序的崩溃不会导致内存缓存丢失。在这种模式下，Tachyon中的内存是可丢弃的。因此，Tachyon不会尝试重建它从记忆中消失的区块。

## 11.8 案例分析

这一节回到一个更具体的问题：一段 Spark 用户代码，最终是怎样被拆成作业、阶段和任务的。理解这条链路，有助于把前面讲过的 DAG、Stage、Shuffle 和执行器行为真正对应到实际运行过程。

### 11.8.1 执行模型

先来看看逻辑计划，考虑从csv文件加载SFPD数据的早期课程的示例。将以此作为一个例子，通过看一看Spark执行模型的组件怎样运行的。

```scala
scala> val inputRDD = sc.textFile("/root/data/sfpd.csv")
inputRDD: org.apache.spark.rdd.RDD[String] = /root/data/sfpd.csv
MapPartitionsRDD[1] at textFile at <console>:24
scala> val sftpdRDD = inputRDD.map(x=>x.split(","))
sftpdRDD: org.apache.spark.rdd.RDD[Array[String]] =
MapPartitionsRDD[4] at map at <console>:26
scala> val catRDD = sftpdRDD.map(x=>(x(1),1)).reduceByKey((a,b)=>a+b)
catRDD: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[7] at
reduceByKey at <console>:28
```

代码 11.39
第一行语句从sfpd.csv文件创建名为inputRDD的RDD；第二行创建的RDD为sfpdRDD，其将基于所述逗号分隔符输入RDD的数据；第三条语句通过map和reduceByKey转换创建catRDD。上面的代码还没有执行任何动作，只是定义了这些RDD对象的DAG。每个RDD维护指向其所依赖RDD的指针，以及这个依赖关系的元数据。RDD使用这些关系数据来跟踪其关联的RDD，要显示的RDD谱系，使用toDebugString方法：

```scala
scala> catRDD.toDebugString
res0: String =
(2) ShuffledRDD[7] at reduceByKey at <console>:28 []
+-(2) MapPartitionsRDD[6] at map at <console>:28 []
| MapPartitionsRDD[4] at map at <console>:26 []
| /root/data/sfpd.csv MapPartitionsRDD[3] at textFile at
<console>:24 []
| /root/data/sfpd.csv HadoopRDD[2] at textFile at <console>:24 []
```

代码 11.40
在这个例子中，显示了catRDD谱系。谱系显示了catRDD所有依赖结构。sc.textFile首先创建一个HadoopRDD，然后是MapPartitionsRDD。每次应用map转换时，它会产生MapPartitionsRDD。当应用reduceByKey转换时，它会产生ShuffledRDD。

目前为止还没有进行任何生成RDD的计算，因为没有执行任何动作操作。当在catRDD上添加collect动作时，collect动作触发了RDD计算。Spark调度程序创建一个物理计划来计算所需的RDD。当调用collect动作时，RDD的每个分区都会被实现，并传输到启动程序上。此时，Spark调度程序从catRDD开始逆向运作，建立必要的物理规划计算所有依赖的RDD。

![](media/11_monitoring_and_optimization/media/image18.jpeg)

图例 11‑24 物理规划

调度器并不会机械地为“每一个 RDD”都单独生成一个阶段。更准确地说，Spark 会沿着依赖链向前分析：只要一串转换之间不需要 Shuffle，也不需要从已经物化的结果重新起步，它们就有机会被压进同一个 Stage 里执行。这种把多步窄依赖转换串起来连续执行的方式，通常就叫流水线化（pipelining）。

在图中的例子里，两个 `map` 都只是对各自分区做局部计算，不涉及跨分区数据交换，所以可以放在同一个 Stage 中顺序完成；而 `reduceByKey` 需要把相同键的数据重新汇集到一起，因此会形成新的 Shuffle 边界，并被拆到下一阶段。理解 Stage 切分时，最重要的不是记住某个固定规则，而是先问一句：这里有没有跨分区重组数据，或者有没有从已缓存/已物化结果重新开始？

当某个动作（例如 `collect()`）触发执行时，逻辑 DAG 会被细化成真正要运行的作业。一个作业可以包含多个阶段，每个阶段又会针对其输入分区生成一组任务。任务才是最终被提交到执行器上的最小运行单元：它读取输入分区、执行本阶段的那串算子，并把结果返回给下游阶段、Driver，或者外部存储系统。

（1）获取输入（从数据存储、现有的RDD或Shuffle输出）

（2）执行必要的操作来计算所需的RDD

（3）输出给下一个Shuffle操作、外部存储或返回到驱动程序（例如count、collect）

现有总结几个重要的概念：

（1）一个任务是对应于一个RDD分区的工作单元

（2）一个阶段一组任务，并行执行相同的计算。

（3）洗牌是在阶段之间的传输数据。

（4）对于一个特定动作（如count）的一系列阶段是一个作业（Job）。

（5）当RDD从上级依赖RDD产生而不移动数据时，调度器会进行流水线操作

（6）DAG定义了RDD操作的逻辑关系。

（7）RDD是具有分区的并行数据集。

同时，Spark的程序执行经历了三个时期：

（1）用户代码定义了DAG或RDD

用户代码定义RDD和RDD上的操作。当对RDD应用转换时，会创建指向其上级RDD依赖关系图，从而导致DAG。

（2）动作负责将DAG转换为物理执行计划

当调用动作时，必须计算RDD。这导致按照谱系计算上级RDD。调度程序将每个操作提交作业以计算所有必需的RDD。此作业有一个或多个阶段，而这些阶段又由在分区上并行运行的任务组成。一个阶段对应产生一个RDD，除非由于流水线而使谱系合并。

（3）任务在集群上进行调度和执行

阶段按顺序执行。当作业的最后阶段完成时，动作被认为执行完成。

当创建新的RDD时，这三个时期可能会发生多次。

  - 通常作业中的阶段数等于DAG中的RDD数量。但是，调度程序可以在什么情况下合并谱系？

### 11.8.2 监控界面

接下来把视角切到 Spark Web UI。它是定位执行瓶颈、观察阶段划分和查看缓存/执行器状态的第一现场。默认情况下，每个 `SparkContext` 都会启动一个 Web UI，通常监听在 `4040` 端口；应用运行期间，可以在这里看到与作业进度和性能相关的大量细节，包括：

（1）调度程序阶段和任务的列表

（2）RDD大小和内存使用情况的摘要

（3）环境变量信息

（4）有关正在运行的执行程序信息

只需在Web浏览器中打开地址http://\<driver-node\>:4040，即可访问此界面。如果多个SparkContext在同一主机上运行，它们将绑定到以4040（4041、4042等）开头的连续端口上。请注意，此信息仅在应用程序默认配置情况下可用。要想在应用程序运行之后查看Web
UI，需要在启动应用程序之前将spark.eventLog.enabled设置为true，具体操作方法可以参考官方手册。在本例中，可以根据下面代码示例来看一看作业和阶段等关键信息：

```scala
scala> val inputRDD = sc.textFile("/root/data/sfpd.csv")
inputRDD: org.apache.spark.rdd.RDD[String] = /root/data/sfpd.csv
MapPartitionsRDD[1] at textFile at <console>:24
scala> val sftpdRDD = inputRDD.map(x=>x.split(","))
sftpdRDD: org.apache.spark.rdd.RDD[Array[String]] =
MapPartitionsRDD[2] at map at <console>:26
scala> val catRDD = sftpdRDD.map(x=>(x(1),1)).reduceByKey((a,b)=>a+b)
catRDD: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at
reduceByKey at <console>:28
scala> catRDD.cache()
res0: catRDD.type = ShuffledRDD[4] at reduceByKey at <console>:28
scala> catRDD.collect()
res1: Array[(String, Int)] = Array((PROSTITUTION,1316),
(DRUG/NARCOTIC,14300), (EMBEZZLEMENT,392), (FRAUD,7416),
(WEAPON_LAWS,3975), (BURGLARY,15374), (EXTORTION,75), (WARRANTS,17508),
(DRIVING_UNDER_THE_INFLUENCE,1038), (TREA,6), (LARCENY/THEFT,96955),
(BAD CHECKS,69), (RECOVERED_VEHICLE,760), (LIQUOR_LAWS,494),
(SUICIDE,182), (OTHER_OFFENSES,50611), (VEHICLE_THEFT,17581),
(DRUNKENNESS,1870), (MISSING_PERSON,11560), (DISORDERLY_CONDUCT,1052),
(FAMILY_OFFENSES,201), (ARSON,690), (ROBBERY,9658),
(SUSPICIOUS_OCC,13659), (GAMBLING,46), (KIDNAPPING,1268),
(RUNAWAY,521), (VANDALISM,17987), (BRIBERY,159), (NON-CRIMINAL,50269),
(SECONDARY_CODES,4972), (SEX_OFFENSES/NON_FORCIBLE,49),
(PORNOGRAPHY/OBSCENE MAT,10), (SEX_OFFENSES/FORCIBLE,2043),
(FORGERY/COUNTERFEITING,2025), (TRESPASS,2930), (ASS...
scala> catRDD.collect()
res2: Array[(String, Int)] = Array((PROSTITUTION,1316),
(DRUG/NARCOTIC,14300), (EMBEZZLEMENT,392), (FRAUD,7416),
(WEAPON_LAWS,3975), (BURGLARY,15374), (EXTORTION,75), (WARRANTS,17508),
(DRIVING_UNDER_THE_INFLUENCE,1038), (TREA,6), (LARCENY/THEFT,96955),
(BAD CHECKS,69), (RECOVERED_VEHICLE,760), (LIQUOR_LAWS,494),
(SUICIDE,182), (OTHER_OFFENSES,50611), (VEHICLE_THEFT,17581),
(DRUNKENNESS,1870), (MISSING_PERSON,11560), (DISORDERLY_CONDUCT,1052),
(FAMILY_OFFENSES,201), (ARSON,690), (ROBBERY,9658),
(SUSPICIOUS_OCC,13659), (GAMBLING,46), (KIDNAPPING,1268),
(RUNAWAY,521), (VANDALISM,17987), (BRIBERY,159), (NON-CRIMINAL,50269),
(SECONDARY_CODES,4972), (SEX_OFFENSES/NON_FORCIBLE,49),
(PORNOGRAPHY/OBSCENE MAT,10), (SEX_OFFENSES/FORCIBLE,2043),
(FORGERY/COUNTERFEITING,2025), (TRESPASS,2930), (ASS...
scala> catRDD.count()
res3: Long = 39
```

代码 11.41
要访问Web
UI，使用Web浏览器打开驱动程序的ip地址和端口4040。Jobs页面提供活动和最近完成的Spark作业的详细执行信息，提供Job的表现，以及运行Job的进度、阶段和任务。

![](media/11_monitoring_and_optimization/media/image19.jpeg)

图例 11‑25 Spark作业的详细执行信息

Job0是被第一个执行的，对应于collect动作，由2个阶段组成，每个阶段由4个任务组成。Job1对应于第二个collect动作，由1个阶段组成，其由两个任务组成。Job2对应于count动作，并且还由1个阶段组成，其包含两个任务。需要注意的是第一个Job花了2秒，而对比Job1历时36毫秒。

  - Job1和Job2为什么仅有一个阶段，而跳过了一个阶段？

第一个collect首先计算有两个阶段的RDD，分别是map和reduceByKey，然后将输出的RDD缓存。第二个collect和第三个count直接使用已经被缓存的RDD，调度器合并了RDD谱系，结果导致跳过计算RDD的阶段。这也导致Job1为36毫秒比Job0的2秒快，虽然都是运行collect操作。

单击“Jobs”页面上“Description”列中的链接，将进入“Job
Details”页面。此页面提供了运行作业的进度、阶段和任务。注意Job0中的collect在这里需要0.1秒。

![](media/11_monitoring_and_optimization/media/image20.jpeg)

图例 11‑26 Job Details页面

而在Job1的详细信息中，可以看到跳过了map阶段。

![](media/11_monitoring_and_optimization/media/image21.jpeg)

图例 11‑27Job1的详细信息

在此页面中，可以看到完成的阶段和跳过阶段的细节。请注意，这里的collect只用了30毫秒。一旦确定了感兴趣的阶段，可以点击链接深入到阶段的详细信息页面。

![](media/11_monitoring_and_optimization/media/image22.jpeg)

图例 11‑28阶段的详细信息

这里为所有任务的汇总指标。

![](media/11_monitoring_and_optimization/media/image23.png)

图例 11‑29Storage页面

Storage页面提供有关持久化RDD的信息。如果在RDD上调用persist和cache操作，并且随后执行了一个动作，那么这个RDD就会被持久化。该页面告诉RDD哪个部分被缓存，并且包括多少比例的RDD被缓存，已经在不同存储介质中的大小，以查看重要数据集是否适合内存。

![](media/11_monitoring_and_optimization/media/image24.png)

图例 11‑30有关持久化RDD的信息

Environment页面列出了运行Spark应用程序的环境变量。当想要查看启用了哪些配置标志时，请使用此页面。请注意，只有通过Spark-default.conf、SparkConf或者在命令行中指定的值将在这里显示。对于所有其它配置属性，则使用默认值。

![](media/11_monitoring_and_optimization/media/image25.png)

图例 11‑31Environment页面

Executors页面列出了应用程序中活动的执行器，还包括关于每个执行器的处理和存储的一些指标。使用此页面确认的应用程序是否具有期望的资源数量。

![](media/11_monitoring_and_optimization/media/image26.png)

图例 11‑32Executors页面

  - 可用通过Web UI监控哪些事情？

可以使用Jobs页面和Stages选项卡查看哪些阶段运行缓慢，比较一个阶段的指标，看看每个任务；查看Executors选项卡，查看应用程序是否具有预期的资源；使用Storage选项卡查看数据集是否适合内存，哪些部分被缓存。

### 11.8.3 调试优化

现在将看看如何调试和调整的Spark应用程序。以下是调试性能问题的一些方法。要检测Shuffle问题，请查看Web
UI，查看任何非常慢的任务。当在某些小型任务比其他任务需要更长时间时，数据并行系统中就会出现的一些常见的性能问题，也可以成为偏斜（skewness）问题，就是指任务运行时间的不对称。要查看是否存在偏斜问题，请查看Stages详细信息页面，看看是否存在运行速度明显慢于其他的任务。向下钻取以查看是否有少量任务读取或写入比其他更多的数据。在Stages页面中，还可以确定某些节点上的任务是否运行缓慢。从Web
UI，还可以找到那些在读取、计算和写入上花费太多时间的任务。在这种情况下，查看代码是否有任何高代价的操作被执行，通常导致性能下降的常见问题是：

（1）平行度水平

（2）洗牌操作期间使用的序列化格式

（3）管理内存以优化应用程序

接下来将更深入地研究这些内容。RDD被分成一组分区，其中每个分区包含数据的子集。调度程序将为每个分区创建一个任务。每个任务需要集群中的单个核心。默认情况下，Spark将基于它认为最佳的并行度进行分区。

![](media/11_monitoring_and_optimization/media/image27.jpeg)

图例 11‑33 Spark将基于它认为最佳的并行度进行分区

  - 并行性水平如何影响性能？

如果并行性水平太小，Spark可能会让资源的闲置。如果太多的并行性，与每个分区相关联的开销加起来变得显着。

  - 如何找到分区数？

可以通过Web UI
中的Stages选项卡中进行操作。由于阶段中的任务对应到RDD中的单个分区，所以任务总数就是分区数。还可以使用rdd.partitions.size来获得RDD分区的数量。如果要调整并行度，可以下面几种方法：

（1）指定分区的数量，当调用操作需要Shuffle数据时，例如reduceByKey。

（2）在RDD中重新分配数据，这可以通过增加或减少分区数来完成，可以使用repartition方法来指定分区或coalesce减少分区的数量。

当 Shuffle 期间需要在网络上传输大量数据时，Spark 会先把对象序列化成二进制字节流。序列化格式会直接影响 CPU 开销、网络负载和内存占用，因此它经常成为性能调优里的关键变量。Java 序列化通用但体积较大、速度也通常不占优；如果你的数据类型适合 Kryo，切换到 Kryo 往往能获得更紧凑的对象表示和更高的传输效率。

内存部分也不建议再用固定比例去死记硬背。现代 Spark 更强调统一内存管理：执行内存和存储内存在整体预算内动态平衡，具体表现会受到算子类型、缓存行为、广播变量和任务并发度共同影响。真正值得关注的是三件事：哪些数据需要缓存、哪些算子会放大执行期内存压力，以及对象表示和序列化方式是否过重。

在缓存策略上，`cache()` 等价于默认的 `persist()` 存储级别，适合那些会被重复访问且放得进默认存储层的数据；如果数据量较大、重算成本又高，`MEMORY_AND_DISK` 往往比一味追求纯内存更稳妥。是否使用序列化缓存、是否需要减少分区粒度或调整并发，不应靠固定配方决定，而应结合 Spark UI、执行时间、GC 日志和失败模式一起判断。

Spark日志子系统基于log4j，记录级别或日志输出可以自定义。log4j的配置属性的一个例子在Spark安装目录conf中提供，可以被复制并适当地进行编辑的。Spark日志文件的位置取决于部署模式。在Spark独立模式下，日志文件位于每个Worker的Spark部署目录中。在Kubernetes中，日志通常通过kubectl logs或集中式日志系统采集，
而在YARN中可通过YARN日志收集工具访问。

如果可能的话，避免Shuffle大量数据。在使用聚合操作的情况下，尽量使用aggregateByKey。对于大量数据，使用groupByKey的结果会产生大量的Shuffle操作。如果可能的话使用reduceByKey，还可以使用combineByKey或
foldByKey。collect动作试图将在RDD中每一个元素传送到驱动程序上。如果有一个非常大的RDD，这可能会导致驱动程序崩溃。countByKey、countByValue和collectAsMap也会出现同样的问题。过滤掉尽可能多的数据集。如果有很多空闲的任务，则需要减少分区。如果没有使用集群中的所有插槽，则重新分区。

  - 可以使用哪些方法提高Spark的性能？

## 11.9 小结

本章从“先监控、再优化”的思路出发，介绍了 Spark 作业的执行模型、Shuffle 与内存机制，以及定位瓶颈时最常看的 UI 页面和调优手段。读完本章后，读者应能根据作业现象判断问题更可能出在数据倾斜、分区数量、序列化、缓存策略还是资源配置，并据此做出有证据支撑的优化决策。

