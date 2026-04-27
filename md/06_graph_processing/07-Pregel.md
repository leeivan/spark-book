# 6.7 Pregel

&emsp;&emsp;从数学角度来说，图是非常有用的抽象可以用来解决许多实际的计算问题，例如借助PageRank图算法，今天我们可以搜索近50亿个网页；除了网络搜索之外，还有其他应用程序，例如社交媒体需要对其进行迭代图处理。图本质上具有递归数据结构，因为点的属性取决于其相邻节点的属性，而邻居的属性又依赖于其邻居的属性。因此，许多重要的图算法迭代地重新计算每个点的属性，直到达到一个固定点的条件，目前提出了一系列图并行抽象来表达这些迭代算法。在本节中，我们将学习如何使用计算模型Pregel完成此类任务。Pregel最初由Google提出，并已被Spark用作迭代图形计算的通用编程接口。我们将了解Pregel计算模型，通过具有示例说明Spark中Pregel运算符的接口和实现，并且将能够使用Pregel接口制定自己的算法。

&emsp;&emsp;Pregel的计算过程是由一系列被称为“超步”的迭代组成的。一次块同步并行计算模型 （Bulk Synchronous Parallel
&emsp;&emsp;Computing Model，BSP）计算过程包括一系列全局超步，所谓的超步就是计算中的一次迭代，每个超步主要包括三个组件：

&emsp;&emsp;局部计算：每个参与的处理器都有自身的计算任务。

&emsp;&emsp;通讯：处理器群相互交换数据。

&emsp;&emsp;栅栏同步（Barrier Synchronization）：当一个处理器遇到路障或栅栏，会等到其他所有处理器完成它们的计算步骤。

&emsp;&emsp;在每个超步中，每个顶点上面都会并行执行用户自定义的函数，该函数描述了一个顶点V在一个超步S中需要执行的操作。该函数可以读取前一个超步(S-1)中其他顶点发送给顶点V的消息，执行相应计算后，修改顶点V及其出射边的状态，然后沿着顶点V的出射边发送消息给其他顶点，而且，一个消息可能经过多条边的传递后被发送到任意已知ID的目标顶点上去。这些消息将会在下一个超步(S+1)中被目标顶点接收，然后像上述过程一样开始下一个超步(S+1)的迭代过程。在第0个超步，所有顶点处于活跃状态。当一个顶点不需要继续执行进一步的计算时，就会把自己的状态设置为“停机”，进入非活跃状态。当一个处于非活跃状态的顶点收到来自其他顶点的消息时，Pregel计算框架必须根据条件判断来决定是否将其显式唤醒进入活跃状态。当图中所有的顶点都已经标识其自身达到“非活跃（inactive）”状态，并且没有消息在传送的时候，算法就可以停止运行。在Pregel计算过程中，一个算法什么时候可以结束，是由所有顶点的状态决定的。

&emsp;&emsp;与其他标准的Pregel实现不同，GraphX中的点只能将消息发送到相邻点，并且使用用户定义的消息传递函数并行完成消息构造，这些限制允许在GraphX之中进行额外优化。

### 6.7.1 一个例子

&emsp;&emsp;在正式介绍 Pregel API 之前，先用一个“朋友之间逐步转移资金”的玩具例子来理解它的消息传递模型。假设社交网络里的每个人都会比较自己和朋友当前拥有的财富，并把一部分资金转给更穷的朋友。这个设定本身并不重要，关键在于它能直观展示 Pregel 每轮迭代的三个核心动作：接收上一轮消息、根据当前状态更新顶点属性、再向邻居发送新消息。在这个例子里，消息类型可以直接用 `Double` 表示金额；每次超步开始时，顶点先汇总上一轮从邻居那里收到的款项，因此第一步需要一个 `mergeMsg` 函数来合并入站消息：

```scala
scala> def mergeMsg(fromA: Double, fromB: Double): Double = fromA +
fromB
mergeMsg: (fromA: Double, fromB: Double)Double
```



&emsp;&emsp;其次，他们还将需要一个称为顶点程序的函数，以计算在上一个超集中收到钱后所拥有的钱：

```scala
scala> def vprog(id: VertexId, balance: Double, credit: Double) =
balance + credit
vprog: (id: org.apache.spark.graphx.VertexId, balance: Double, credit:
Double)Double
```



&emsp;&emsp;最后，还需要一个名为sendMsg的函数来在朋友之间进行汇款：

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
def sendMsg(t: EdgeTriplet[Double, Int]) =
if (t.srcAttr <= t.dstAttr) Iterator.empty
else Iterator((t.dstId, t.srcAttr * 0.05), (t.srcId, -t.srcAttr *
0.05))
// Exiting paste mode, now interpreting.
sendMsg: (t:
org.apache.spark.graphx.EdgeTriplet[Double,Int])Iterator[(org.apache.spark.graphx.VertexId,
Double)]
```



&emsp;&emsp;从上一个函数签名可以看出，sendMsg将边缘三元组作为输入而不是顶点，因此我们可以访问源节点和目标节点。
&emsp;&emsp;`sendMsg` 的具体实现放在下一节展开。为了先把思路讲清楚，可以把问题进一步简化成“三个朋友构成的三角网络”：

```scala
scala> val nodes: RDD[(Long,Double)] =
sc.parallelize(List((1,10.0),(2,3.0),(3,5.0)))
nodes: org.apache.spark.rdd.RDD[(Long, Double)] =
ParallelCollectionRDD[403] at parallelize at <console>:33
scala> val edges =
sc.parallelize(List(Edge(1,2,1),Edge(2,1,1),Edge(1,3,1),Edge(3,1,1),Edge(2,3,1),Edge(3,2,1)))
edges: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] =
ParallelCollectionRDD[404] at parallelize at <console>:33
scala> val graph = Graph(nodes, edges)
graph: org.apache.spark.graphx.Graph[Double,Int] =
org.apache.spark.graphx.impl.GraphImpl@5a94bd21
scala> graph.vertices.foreach(println)
(2,3.0)
(3,5.0)
(1,10.0)
```



&emsp;&emsp;先看一下输出结果：

```scala
scala> val afterOneIter = graph.pregel(0.0, 1)(vprog, sendMsg,
mergeMsg)
afterOneIter: org.apache.spark.graphx.Graph[Double,Int] =
org.apache.spark.graphx.impl.GraphImpl@2616f1aa
scala> afterOneIter.vertices.foreach(println)
(3,5.25)
(1,9.0)
(2,3.75)
```



&emsp;&emsp;到这里逻辑已经是通的；如果继续增加最大迭代次数，结果会发生什么？

```scala
scala> val afterTenIter = graph.pregel(0.0, 10)(vprog, sendMsg,
mergeMsg)
afterTenIter: org.apache.spark.graphx.Graph[Double,Int] =
org.apache.spark.graphx.impl.GraphImpl@dbebab8
scala> afterTenIter.vertices.foreach(println)
(1,5.999611965064453)
(2,6.37018749852539)
(3,5.630200536410156)
scala> val afterHundredIters = graph.pregel(0.0, 100)(vprog, sendMsg,
mergeMsg)
afterHundredIters: org.apache.spark.graphx.Graph[Double,Int] =
org.apache.spark.graphx.impl.GraphImpl@7cc6b019
scala> afterHundredIters.vertices.foreach(println)
(1,6.206716647163644)
(3,5.586245079113054)
(2,6.207038273723298)
```



&emsp;&emsp;即使迭代 100 次，账户余额也没有精确收敛到理想值 6 美元，而是在其附近波动。在这个玩具示例里，这是符合预期的。

### 6.7.2 Pregel运算符

&emsp;&emsp;现在把 Pregel 算子的编程接口正式写出来：
```scala
class GraphOps[VD, ED] {
  def pregel[A](
    initialMsg: A,
    maxIter: Int = Int.MaxValue,
    activeDir: EdgeDirection = EdgeDirection.Out
  )(
    vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A
  ): Graph[VD, ED]
}
```


&emsp;&emsp;Pregel方法在属性图上调用，并返回具有相同类型和结构的新图。 当边保持完整时，顶点的属性可能从一个超集更改为下一个超集。
&emsp;&emsp;Pregel接受以下两个参数列表。
&emsp;&emsp;第一个列表包含：•用户定义的类型A的初始消息-算法开始时，每个顶点都会接收到此消息•迭代的最大次数•发送消息所沿的边缘方向。

&emsp;&emsp;当没有更多消息要发送时，或者达到指定的最大迭代次数时，Pregel算法终止。
&emsp;&emsp;在实施算法时，务必限制迭代次数，尤其是在不能保证算法收敛的情况下。如果未指定有效边沿方向，则Pregel会假定仅针对每个顶点的出站边发送消息。
&emsp;&emsp;而且，如果一个顶点在上一个超集中未接收到消息，则在当前超集的末尾，将不会沿其输出边缘发送任何消息。

&emsp;&emsp;此外，第二个参数列表必须包含三个函数：

  - vprog: (VertexId, VD, A) =\> VD

&emsp;&emsp;此函数是点程序，将更新点的属性，这些点从先前迭代中接收到消息。

  - mergeMsg: (A, A) =\> A)

&emsp;&emsp;此函数合并每个点要接收的消息。

  - sendMsg: EdgeTriplet\[VD, ED\] =\> Iterator\[(VertexId, A)\]

&emsp;&emsp;此函数采用三元组，并创建要发送到起始节点或目标节点的消息。

### 6.7.3 标签传播算法

&emsp;&emsp;下面用 Pregel 接口实现一个社区检测算法。标签传播算法（Label Propagation Algorithm, LPA）是一类基于图的半监督方法，核心思路是让已标记节点的标签沿着图结构逐步向邻居扩散，并在迭代中不断更新未标记节点的标签估计。对于无向图来说，节点越相似、连接越紧密，彼此标签就越容易趋同；当迭代稳定后，标签接近的一组节点往往可以视作同一个社区。对本章来说，理解到这一层就足够了：LPA 依赖的是“节点反复接收邻居信息，再更新自身标签”的过程，因此非常适合用 Pregel 这样的消息传递模型来表达。

&emsp;&emsp;通过在Pregel中实现此算法，我们希望获得一个图，其中点属性是社区隶属关系的标签。因此，我们将首先通过将每个点的标签设置为其标识符来初始化LPA图：
```scala
val lpaGraph = graph.mapVertices { case (vid, _) => vid }
```


&emsp;&emsp;接下来，我们将定义发送给Map \[Label，Long\]的消息的类型，该消息将社区标签与具有该标签的邻居数量相关联。
&emsp;&emsp;将发送到每个节点的初始消息只是一个空映射：
```scala
type Label = VertexId
val initialMessage = Map[Label, Long]()
```


&emsp;&emsp;遵循Pregel编程模型，我们定义了sendMsg函数，每个节点使用该函数将其当前标签通知其邻居。
&emsp;&emsp;对于每个三元组，源节点将收到目标节点的标签，反之亦然：
```scala
def sendMsg(e: EdgeTriplet[Label, ED]): Iterator[(VertexId, Map[Label, Long])] =
  Iterator(
    (e.srcId, Map(e.dstAttr -> 1L)),
    (e.dstId, Map(e.srcAttr -> 1L))
  )
```


&emsp;&emsp;上一个函数在每次迭代中都会返回其大多数邻居当前所属的社区的标签（即VertexId属性）。我们还需要一个mergeMsg函数来合并节点收到的所有消息。
&emsp;&emsp;它的邻居变成一张地图。 如果两个消息都包含相同的标签，我们只需简单地为该标签求和相应的邻居数：
```scala
def mergeMsg(count1: Map[Label, Long], count2: Map[Label, Long]): Map[VertexId, Long] = {
  (count1.keySet ++ count2.keySet).map { i =>
    val count1Val = count1.getOrElse(i, 0L)
    val count2Val = count2.getOrElse(i, 0L)
    i -> (count1Val + count2Val)
  }.toMap
}
```


&emsp;&emsp;最后，我们可以通过调用图中的pregel方法来运行LPA算法，以实现社会财富均等化：
```scala
lpaGraph.pregel(initialMessage, 50)(vprog, sendMsg, mergeMsg)
```


&emsp;&emsp;LPA的主要优点是它的简单性和时间效率。 实际上，已经观察到收敛的迭代次数与图的大小无关，而每次迭代都具有线性时间复杂度。
&emsp;&emsp;尽管标签传播算法有其优点，但不一定会收敛，并且还可能导致不感兴趣的解决方案，例如将每个节点标识为单个社区。
&emsp;&emsp;实际上，该算法可能会为二分图或接近二分图的结构振动。

### 6.7.4 PageRank算法

&emsp;&emsp;PageRank即网页排名，又称网页级别，是Google创始人拉里·佩奇和谢尔盖·布林于1997年构建早期的搜索系统原型时提出的链接分析算法，该算法成为其他搜索引擎和学术界十分关注的计算模型。目前很多重要的链接分析算法都是在PageRank算法基础上衍生出来的。PageRank也可以用来测量图中每个点的重要性，假设从 $u$ 到 $v$ 的边表示为 $u$ 对 $v$ 重要性的一种度量。例如如果一个用户被很多其他用户关注，则此用户会有更高的排名。GraphX自带的PageRank静态和动态方法实现，静态的PageRank运行一个固定次数的迭代，而动态的PageRank运行直到排名最终收敛到一个指定的误差范围，我们可以直接通过Graph调用这些GraphOps中的方法。

&emsp;&emsp;GraphX还包括一个社交网络数据集可以运行PageRank，用户为data/graphx/users.txt，用户之间的关系为data/graphx/followers.txt，计算每个用户的排名级别，如下所示：

```scala
scala> import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.GraphLoader
```

&emsp;&emsp;//加载边为Graph

```scala
scala> val graph = GraphLoader.edgeListFile(sc,
"/spark/data/graphx/followers.txt")
graph: org.apache.spark.graphx.Graph[Int,Int] =
<org.apache.spark.graphx.impl.GraphImpl@153f8c6a>
```

&emsp;&emsp;//运行PageRank

```scala
scala> val ranks = graph.pageRank(0.0001).vertices
ranks: org.apache.spark.graphx.VertexRDD[Double] =
VertexRDDImpl[3479] at RDD at VertexRDD.scala:57
```

&emsp;&emsp;//用户名连接排名联结

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
val users = sc.textFile("/spark/data/graphx/users.txt").map { line =>
val fields = line.split(",")
(fields(0).toLong, fields(1))
}
// Exiting paste mode, now interpreting.
users: org.apache.spark.rdd.RDD[(Long, String)] =
MapPartitionsRDD[3488] at map at <pastie>:34
scala> :paste
// Entering paste mode (ctrl-D to finish)
val ranksByUsername = users.join(ranks).map {
case (id, (username, rank)) => (username, rank)
}
// Exiting paste mode, now interpreting.
ranksByUsername: org.apache.spark.rdd.RDD[(String, Double)] =
MapPartitionsRDD[3492] at map at <pastie>:37
```

&emsp;&emsp;//打印出结果

```scala
scala> println(ranksByUsername.collect().mkString("\n"))
(justinbieber,0.15007622780470478)
(matei_zaharia,0.7017164142469724)
(ladygaga,1.3907556008752426)
(BarackObama,1.4596227918476916)
(jeresig,0.9998520559494657)
(odersky,1.2979769092759237)
```



&emsp;&emsp;前面已经看过 GraphX 自带页面排名算法的用法。下面换一个角度，用 Pregel 自己实现一次 PageRank，这样更容易看清它的消息传递过程：

&emsp;&emsp;（1）首先我们需要初始化 PageRank 图，将每个边属性设置为 1 除以出度，每个点属性设置为 1.0：
```scala
val rankGraph: Graph[(Double, Double), Double] =
  graph.outerJoinVertices(graph.outDegrees) {
    (vid, vdata, deg) => deg.getOrElse(0)
  }.mapTriplets(e => 1.0 / e.srcAttr)
    .mapVertices((id, attr) => (0.0, 0.0))
```


&emsp;&emsp;（2）按照Pregel的抽象定义，实现PageRank所需的三个函数。首先我们定义点程序如下：
```scala
val resetProb = 0.15

def vProg(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
  val (oldPR, lastDelta) = attr
  val newPR = oldPR + (1.0 - resetProb) * msgSum
  (newPR, newPR - oldPR)
}
```


&emsp;&emsp;接下来是创建消息函数：
```scala
val tol = 0.001

def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
  if (edge.srcAttr._2 > tol) {
    Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
  } else {
    Iterator.empty
  }
}
```


&emsp;&emsp;第三个函数为mergeMsg，只是简单地增加等级：
```scala
def mergeMsg(a: Double, b: Double): Double = a + b
```


&emsp;&emsp;然后我们将获得点排名，如下所示：
```scala
rankGraph
  .pregel(initialMessage, activeDirection = EdgeDirection.Out)(vProg, sendMsg, mergeMsg)
  .mapVertices((vid, attr) => attr._1)
```





