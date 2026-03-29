# 机器学习


## 7.1 本章先看懂什么
- Spark ML Pipeline 的核心链路：特征 -> 模型 -> 评估。
- 训练集/测试集划分与指标选择。
- 如何避免“能跑但不准”的常见问题。

## 7.2 一个最小例子
需求：预测用户是否会流失（二分类）。
1. 组装特征向量。
2. 训练 LogisticRegression。
3. 用 AUC/Accuracy 评估。
4. 调整正则化参数再训练。

先形成“训练-评估-迭代”的闭环，再追求更复杂模型。

> **版本基线（更新于 2026-02-13）**
> 本书默认适配 Apache Spark 4.1.1（稳定版），并兼容 4.0.2 维护分支。
> 推荐环境：JDK 17+（建议 JDK 21）、Scala 2.13、Python 3.10+。
在 Spark 4.x 中，本章的目标不是罗列所有算法名词，而是建立一条可落地的机器学习工作流：准备数据、构造特征、训练模型、评估效果、调节参数并产出可复用的推理流程。书中仍保留部分 `spark.mllib` / RDD 风格示例，用于解释历史 API 与存量系统；如果你是新项目，请优先把注意力放在 `spark.ml`、DataFrame 和 Pipeline 上。

## 7.3 Spark 4.x 机器学习主线

Spark 机器学习 API 历史上分为两层：

- `spark.ml`：构建在 DataFrame/Dataset 之上的主线 API，适合特征工程、模型训练、评估、调参与 Pipeline 组合。
- `spark.mllib`：构建在 RDD 之上的早期 API，目前处于维护模式，主要用于兼容历史代码和少数旧示例。

对 Spark 4.x 读者来说，本章应按下面方式理解：

（1）训练数据优先组织成 DataFrame，通常以 `label`、`features` 等列为核心。

（2）特征工程、模型、评估器和调参器优先使用 `org.apache.spark.ml`。

（3）训练、验证、推理尽量放到同一条 Pipeline 中，减少训练时和上线时预处理不一致的问题。

（4）遇到 `spark.mllib` 示例时，把它看作“历史写法/迁移资料”，重点理解算法思想与 API 差异。

![https://cdn.infoq.com/statics\_s1\_20171010-0642/resource/articles/apache-sparkml-data-pipelines/en/resources/3fig2.jpg](media/07_machine_learning/media/image1.jpeg)

图例 7‑1Spark的生态系统

为什么主线转向基于 DataFrame 的 API？因为它同时带来更统一的数据源接口、SQL 与 Catalyst/Tungsten 优化、跨语言一致性，以及更自然的 Pipeline 组织方式。对真实项目来说，这比单独调用某个算法 API 更重要。

需要注意的是，`spark.mllib` 并不是完全不可用，而是已经不再作为新项目首选。它仍然能帮助你理解旧代码、线性代数类型、分布式矩阵以及部分经典案例；但如果目标是构建新的业务模型，优先级应当明显低于 `spark.ml`。

## 7.4 数据类型

本节介绍本章后续会反复用到的数据表示方式。对于 Spark 4.x 新项目，最常见的入口是 DataFrame 中的 `features` 向量列和 `label` 列，以及 `org.apache.spark.ml.linalg` 下的向量/矩阵类型。分布式矩阵与部分 `spark.mllib` 类型仍有学习价值，但更多属于底层能力或历史 API 视角，而不是多数业务建模任务的起点。

### 7.4.1 局部向量

局部向量将具有整数型、基于0的索引和双精度类型的值，存储在一台机器上。MLlib支持两种类型的局部向量：稠密和稀疏。稠密向量由表示其输入值的双精度数组支持，而稀疏向量由两个并行数组支持：索引和值。例如一个向量（1.0,0.0,3.0）可以用稠密格式表示为\[1.0,0.0,3.0\]，或者以(3,\[0,2\],\[1.0,3.0\])的稀疏格式表示，其中第一个值3为向量的大小，第二个值表示向量的索引，第三个值表示向量的值。局部向量的基类是Vector，提供了两个实现：DenseVector和SparseVector。建议使用Vectors中实现的工厂方法来创建局部向量。Scala默认导入scala.collection.immutable.Vector，所以必须明确地导入org.apache.spark.ml.linalg.Vector。

```scala
scala> import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.ml.linalg.{Vectors, Vector}
```

代码 7‑1

  - 创建稠密向量(1.0, 0.0, 3.0)

```scala
scala> val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
dv: org.apache.spark.ml.linalg.Vector = [1.0,0.0,3.0]
```

代码 7‑2

  - 通过指定非零条目的索引和值，创建一个稀疏向量（1.0，0.0，3.0）。

```scala
scala> val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0,
3.0))
sv1: org.apache.spark.ml.linalg.Vector = (3,[0,2],[1.0,3.0])
```

代码 7‑3

  - 通过指定非零条目来创建一个稀疏向量（1.0，0.0，3.0）。

```scala
scala> val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
sv2: org.apache.spark.ml.linalg.Vector = (3,[0,2],[1.0,3.0])
```

代码 7‑4

代码 7‑2、代码 7‑3、代码 7‑4分别创建相同的局部向量。

### 7.4.2 标签向量

标签向量是一个稠密或稀疏的局部向量，而且关联了标签。在MLlib中，标签向量用于有监督学习算法。使用双精度来存储标签，所以可以在回归和分类中使用标签向量。对于二元分类，标签应该是0（负）或1（正）。对于多类分类，标签应该是从零开始的类索引：0,1,2
...。标签向量由案例类LabeledPoint表示。

```scala
scala> import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors
scala> import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.LabeledPoint
```

代码 7‑5

  - 使用正标签和稠密特征向量创建标签向量

```scala
scala> val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
pos: org.apache.spark.ml.feature.LabeledPoint = (1.0,[1.0,0.0,3.0])
```

代码 7‑6

  - 使用负标签和稀疏特征向量创建标签向量

```scala
scala> val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2),
Array(1.0, 3.0)))
neg: org.apache.spark.ml.feature.LabeledPoint =
(0.0,(3,[0,2],[1.0,3.0]))
```

代码 7‑7

在实践中，使用稀疏的训练数据是很常见的。Spark
ML支持以LIBSVM格式存储的阅读训练样本，这是LIBSVM和LIBLINEAR使用的默认格式。这是一种文本格式，其中每行代表使用以下格式标记的稀疏特征向量：

label index1:value1 index2:value2 ...

代码 7‑8

索引从一开始并按升序排列。
加载后，特征索引将转换为基于零的索引。libsvm包用于将LIBSVM数据加载为DataFrame的数据源API。加载的DataFrame有两列：包含作为双精度存储的标签和包含作为向量存储的特征。要使用LIBSVM格式数据源，需要在DataFrameReader中将格式设置为“libsvm”，并可以指定option，例如：

val df = spark.read.format("libsvm").option("numFeatures", "780")

.load("data/mllib/sample\_libsvm\_data.txt")

代码 7‑9

libsvm数据源支持以下选项：

  - numFeatures

指定特征的数量，如果未指定或不是正数，特征的数量将自动确定，但需要额外计算的代价。当数据集已经被分割成多个文件并且你想单独加载时这也是有用的，因为某些特征可能不存在于某些文件中，这导致特征数量可能不一致，需要特别指定。

  - vectorType

特征向量类型，稀疏（默认）或稠密。

  - LIBSVM

LIBSVM是台湾大学林智仁(Lin
Chih-Jen)教授等开发设计的一个简单、易于使用和快速有效的SVM模式识别与回归的软件包，他不但提供了编译好的可在Windows系列系统的执行文件，还提供了源代码，方便改进、修改以及在其它操作系统上应用；该软件对SVM所涉及的参数调节相对比较少，提供了很多的默认参数，利用这些默认参数可以解决很多问题；并提供了交互检验(Cross
Validation)的功能。该软件可以解决C-SVM、ν-SVM、ε-SVR和ν-SVR等问题，包括基于一对一算法的多类模式识别问题。Libsvm
和 Liblinear 都是国立台湾大学的 Chih-Jen Lin 博士开发的，Libsvm主要是用来进行非线性svm
分类器的生成，而Liblinear则是应对large-scale的data
classification，因为linear分类器的训练比非线性分类器的训练计算复杂度要低很多，时间也少很多，而且在large
scale data上的性能和非线性的分类器性能相当，所以Liblinear是针对大数据而生的。

两者都是一个跨平台的通用工具库，支持windows/linux/mac
os,代码本身是c++写的，同时也有matlab，python，java，c/c++扩展接口，方便不同语言环境使用，可以说是科研和企业人员的首选！像我这样在学校的一般用matlab/c++，而我同学在百度则主要用的是python/c++，所以只是各自侧重不一样，但所使用的核心还是其svm库。

### 7.4.3 局部矩阵

局部矩阵具有整数类型的行和列索引以及双重类型的值，它们存储在单个计算机上。MLlib支持密集矩阵（其条目值以列优先顺序图例 7‑2存储在单个双精度数组中）和稀疏矩阵（其非零条目值以列优先顺序和压缩稀疏列格式存储）。

![](media/07_machine_learning/media/image2.jpeg)

图例 7‑2按列排序从左到右，从上到下

例如下面的稠密矩阵：

\[\begin{pmatrix}
1.0 & 2.0 \\
3.0 & 4.0 \\
5.0 & 6.0 \\
\end{pmatrix}\]

公式 7‑1

以矩阵大小（3,2）存储在一维数组\[1.0,3.0,5.0,2.0,4.0,6.0\]中。局部矩阵的基类是Matrix，提供了两个实现：DenseMatrix和SparseMatrix。建议使用在矩阵中实现的工厂方法来创建局部矩阵。请记住，MLlib中的局部矩阵以列优先顺序存储。

```scala
scala> import org.apache.spark.ml.linalg.{Matrix, Matrices}
import org.apache.spark.ml.linalg.{Matrix, Matrices}
```

代码 7‑10

  - 创建稠密矩阵((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))

```scala
scala> val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0,
4.0, 6.0))
dm: org.apache.spark.ml.linalg.Matrix =
1.0 2.0
3.0 4.0
5.0 6.0
```

代码 7‑11

  - 创建稀疏矩阵 ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))

```scala
scala> val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0,
2, 1), Array(9, 6, 8))
sm: org.apache.spark.ml.linalg.Matrix =
3 x 2 CSCMatrix
(0,0) 9.0
(2,1) 6.0
(1,1) 8.0
scala> sm.toDense
res4: org.apache.spark.ml.linalg.DenseMatrix =
9.0 0.0
0.0 8.0
0.0 6.0
```

代码 7‑12

  - def sparse(numRows: Int, numCols: Int, colPtrs: Array\[Int\],
    rowIndices: Array\[Int\], values: Array\[Double\]): Matrix

使用列优先顺序格式，创建一个稀疏矩阵。

  - > numRows：行数

  - > numCols：列数

  - > colPtrs：对应与新列开始的索引

  - > rowIndices：行索引

  - > values：按列分布的非零值

通过对上面的例子详细描述，学习怎样创建稀疏矩阵。例子中，sparse方法的参数分别为numRows=3、numCols=2、colPtrs=
Array(0, 1, 3)、rowIndices= Array(0, 2, 1)、values=Array(9, 6,
8)，numRows和numCols代表此矩阵为3行2列；values代表矩阵中的非零数值为9、6、8，其顺序是按列分布排序的；rowIndices数组的长度与数值的个数相同，数组中的每个值代表对应数值的行索引；colPtrs数组的长度等于numCols+1，一般第一个元素为0，代表从第一个值9开始，第二个元素为1-0=1，代表第一列只包含一个值9，第三个元素为3-1=2，代表第二列包括两个值6和8。

### 7.4.4 分布式矩阵（历史/底层能力）

本节介绍的分布式矩阵类型主要位于 `spark.mllib.linalg.distributed`，更适合用来理解 Spark 早期在线性代数与分布式矩阵上的底层抽象。对 Spark 4.x 新项目来说，多数机器学习任务不会直接从这些类型起步，而是优先使用 DataFrame 中的 `features` 向量列、`spark.ml` 估算器以及 Pipeline；只有在需要底层矩阵运算、兼容旧代码或学习历史接口时，才会直接接触这些对象。

分布式矩阵具有长整型行和列索引以及双精度值，它们以分布式方式存储在一个或多个 RDD 中。选择正确的格式来存储大型分布式矩阵非常重要，因为不同格式之间的转换往往需要全局洗牌，成本较高。这里保留四种典型类型：`RowMatrix`、`IndexedRowMatrix`、`CoordinateMatrix` 和 `BlockMatrix`。阅读时建议重点理解“什么时候需要索引”“什么时候矩阵足够稀疏”“什么时候按块切分更合适”。

#### 7.4.4.1 RowMatrix（历史/兼容）

RowMatrix就是将每行对应一个RDD，将矩阵的每行分布式存储，矩阵的每行是一个局部向量。由于每一行均由局部向量表示，因此列数受整数范围限制，但实际上应小得多。

```scala
scala> import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
scala> import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
```

代码 7‑13

创建RDD\[Vector\]：

```scala
scala> val trainRDD = spark.sparkContext.parallelize(Seq(
| Vectors.dense(2.0, 3.0, 4.0),
| Vectors.dense(5.0, 5.0, 5.0),
| Vectors.dense(2.0, 3.0, 4.0)))
trainRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] =
ParallelCollectionRDD[0] at parallelize at <console>:25
```

代码 7‑14

从RDD\[Vector\]创建RowMatrix：

```scala
scala> val mat: RowMatrix = new RowMatrix(trainRDD)
mat: org.apache.spark.mllib.linalg.distributed.RowMatrix =
<org.apache.spark.mllib.linalg.distributed.RowMatrix@14e8304b>
```

代码 7‑15

得到RowMatrix的长度：

```scala
scala> val m = mat.numRows()
m: Long = 3
scala> val n = mat.numCols()
n: Long = 3
```

代码 7‑16

#### 7.4.4.2 IndexedRowMatrix（历史/兼容）

IndexedRowMatrix类似于RowMatrix，但行索引有意义。它由带索引行的RDD存储，因此每行都由长整型索引和局部向量表示。IndexedRowMatrix可以用RDD
\[IndexedRow\]实例创建，其中IndexedRow是一个基于(Long,Vector)的包装器。IndexedRowMatrix可以通过删除行索引来转换为RowMatrix。

```scala
scala> import org.apache.spark.mllib.linalg.distributed.{IndexedRow,
IndexedRowMatrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow,
IndexedRowMatrix}
scala> val rows = spark.sparkContext.parallelize(Seq(
| IndexedRow(0, Vectors.dense(1, 3)),
| IndexedRow(1, Vectors.dense(4, 5))))
rows:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.distributed.IndexedRow]
= ParallelCollectionRDD[9] at parallelize at <console>:28
```

代码 7‑17

用RDD\[IndexedRow\]创建IndexedRowMatrix：

```scala
scala> val mat02: IndexedRowMatrix = new IndexedRowMatrix(rows)
mat02: org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix =
<org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix@46b4cddb>
```

代码 7‑18

得到长度：

```scala
scala>val m02 = mat02.numRows()
m02: Long = 2
scala>val n02 = mat02.numCols()
n02: Long = 2
```

代码 7‑19

去掉行索引：

```scala
scala> val rowMat: RowMatrix = mat02.toRowMatrix()
rowMat: org.apache.spark.mllib.linalg.distributed.RowMatrix =
<org.apache.spark.mllib.linalg.distributed.RowMatrix@435e857c>
```

代码 7‑20

#### 7.4.4.3 CoordinateMatrix（历史/兼容）

CoordinateMatrix也是分布式矩阵，每个条目由RDD保存。每个条目是(i:Long,j：Long,value：Double)的一个元组，其中i是行索引，j是列索引，value是条目值。CoordinateMatrix只有在矩阵的两个维度都很大且矩阵非常稀疏时才能使用。CoordinateMatrix可以由RDD
\[MatrixEntry\]实例创建，其中MatrixEntry是基于(Long,Long,Double)的包装器。可以通过调用toIndexedRowMatrix将CoordinateMatrix转换为具有稀疏行的IndexedRowMatrix。目前还不支持CoordinateMatrix的其他计算。

```scala
scala> import org.apache.spark.mllib.linalg.distributed.{MatrixEntry,
CoordinateMatrix}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry,
CoordinateMatrix}
scala> val entries03 = spark.sparkContext.parallelize(Seq(
| MatrixEntry(0, 1, 1), MatrixEntry(0, 2, 2), MatrixEntry(0, 3, 3),
| MatrixEntry(0, 4, 4), MatrixEntry(2, 3, 5), MatrixEntry(2, 4, 6),
| MatrixEntry(3, 4, 7)))
entries03:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.distributed.MatrixEntry]
= ParallelCollectionRDD[13] at parallelize at <console>:30
```

代码 7‑21

用RDD\[MatrixEntry\]创建CoordinateMatrix：

```scala
scala>val mat03: CoordinateMatrix = new CoordinateMatrix(entries03)
mat03: org.apache.spark.mllib.linalg.distributed.CoordinateMatrix =
<org.apache.spark.mllib.linalg.distributed.CoordinateMatrix@17c158ca>
```

代码 7‑22

得到长度：

```scala
scala> val m03 = mat03.numRows()
m03: Long = 4
scala>val n03 = mat03.numCols()
n03: Long = 5
```

代码 7‑23

转换成IndexRowMatrix，其中的行为稀疏向量：

```scala
scala> val indexedRowMatrix = mat03.toIndexedRowMatrix()
indexedRowMatrix:
org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix =
<org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix@c34e260>
```

代码 7‑24

#### 7.4.4.4 BlockMatrix（历史/兼容）

BlockMatrix是分布式矩阵，其中的MatrixBlock是由RDD方式保存。MatrixBlock是((Int，Int),Matrix)的元组，其中(Int,Int)是块的索引，Matrix是给定索引的子矩阵，其大小为rowsPerBlock\*colsPerBlock。BlockMatrix支持加和乘另一个BlockMatrix。BlockMatrix还有一个帮助函数validate，可以用来检查BlockMatrix是否正确设置。

BlockMatrix可以通过调用toBlockMatrix方便地从IndexedRowMatrix或CoordinateMatrix创建。toBlockMatrix默认创建大小为1024\*1024的块。用户可以通过toBlockMatrix(rowsPerBlock,colsPerBlock)方法提供值来改变块的大小。

```scala
scala> import org.apache.spark.mllib.linalg.distributed.{MatrixEntry,
CoordinateMatrix, BlockMatrix}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry,
CoordinateMatrix, BlockMatrix}
scala> val entries04 = spark.sparkContext.parallelize(Seq(
| MatrixEntry(0, 0, 1.2),
| MatrixEntry(1, 0, 2.1),
| MatrixEntry(6, 1, 3.7)))
entries04:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.distributed.MatrixEntry]
= ParallelCollectionRDD[18] at parallelize at <console>:31
```

代码 7‑25

用RDD\[MatrixEntry\]创建CoordinateMatrix：

```scala
scala> val coordMat: CoordinateMatrix = new CoordinateMatrix(entries04)
coordMat: org.apache.spark.mllib.linalg.distributed.CoordinateMatrix =
<org.apache.spark.mllib.linalg.distributed.CoordinateMatrix@2142b70d>
```

代码 7‑26

将CoordinateMatrix转换为BlockMatrix：

```scala
scala> val matA: BlockMatrix = coordMat.toBlockMatrix().cache()
matA: org.apache.spark.mllib.linalg.distributed.BlockMatrix =
<org.apache.spark.mllib.linalg.distributed.BlockMatrix@42e58f8e>
```

代码 7‑27

验证BlockMatrix是否设置正确。当它是无效的，抛出一个异常。

```scala
scala> matA.validate()
```

代码 7‑28

计算A^T A：

```scala
scala> val ata = matA.transpose.multiply(matA)
ata: org.apache.spark.mllib.linalg.distributed.BlockMatrix =
<org.apache.spark.mllib.linalg.distributed.BlockMatrix@7e09407a>
```

代码 7‑29

## 7.5 统计基础

给定一个数据集，数据分析师一般会先观察一下数据集的基本情况，称之为汇总统计或者概要性统计。一般的概要性统计用于概括一系列观测值，包括位置或集中趋势（比如算术平均值、中位数、众数和四分位均值），展型（比如四分位间距、绝对偏差和绝对距离偏差、各阶矩等），统计离差，分布的形状，依赖性等。对 Spark 4.x 而言，这部分内容的主线应优先理解为 `spark.ml.stat` 与 DataFrame 上的统计分析能力；`spark.mllib` 的部分工具仍有参考价值，但更多属于历史接口与兼容场景。

### 7.5.1 相关分析

计算两个系列数据之间的相关性是统计中的常见操作。今天更推荐直接使用 `org.apache.spark.ml.stat` 在 DataFrame 的向量列上完成相关性分析；如果阅读旧资料，仍会看到 `spark.mllib` 中基于 RDD 的相关性接口。下面示例采用现代 `spark.ml.stat.Correlation` 写法。

```scala
scala> import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
scala> import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.stat.Correlation
scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
scala> val data = Seq(
| Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
| Vectors.dense(4.0, 5.0, 0.0, 3.0),
| Vectors.dense(6.0, 7.0, 0.0, 8.0),
| Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
| )
data: Seq[org.apache.spark.ml.linalg.Vector] =
List((4,[0,3],[1.0,-2.0]), [4.0,5.0,0.0,3.0], [6.0,7.0,0.0,8.0],
(4,[0,3],[9.0,1.0]))
scala> val df = data.map(Tuple1.apply).toDF("features")
df: org.apache.spark.sql.DataFrame = [features: vector]
scala> val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
coeff1: org.apache.spark.ml.linalg.Matrix =
1.0 0.055641488407465814 NaN 0.4004714203168137
0.055641488407465814 1.0 NaN 0.9135958615342522
NaN NaN 1.0 NaN
0.4004714203168137 0.9135958615342522 NaN 1.0
scala> val Row(coeff2: Matrix) = Correlation.corr(df, "features",
"spearman").head
coeff2: org.apache.spark.ml.linalg.Matrix =
1.0 0.10540925533894532 NaN 0.40000000000000174
0.10540925533894532 1.0 NaN 0.9486832980505141
NaN NaN 1.0 NaN
0.40000000000000174 0.9486832980505141 NaN 1.0
```

代码 7‑30

  - 皮尔逊相关系数(Pearson's correlation coefficient)

皮尔逊相关系数评估两个连续变量之间的线性关系。
当一个变量的变化与另一变量的比例变化相关时，关系是线性的。例如，可能使用皮尔逊相关系数来评估生产设备温度的升高是否与巧克力涂层厚度的降低有关。皮尔逊相关系数是一个介于-1和1之间的值，当两个变量的线性关系增强时，相关系数趋于1或-1；当一个变量增大，另一个变量也增大时，表明它之间是正相关的，相关系数大于0；如果一个变量增大，另一个变量却减小，表明它之间是负相关的，相关系数小于0；如果相关系数等于0，表明它们之间不存在线性相关关系。皮尔森相关系数计算公式如下：

\[\rho_{X,Y} = \frac{\text{cov}(X,Y)}{\sigma_{X}\sigma_{Y}}\]

公式 7‑4

分子是协方差，分母是两个变量标准差的乘积。显然要求X和Y的标准差都不能为0。

  - 斯皮尔曼相关系数(Spearman's correlation coefficient)

斯皮尔曼相关系数评估两个连续变量之间的单调关系。单调关系中，变量趋于一起变化，但不一定以恒定速率变化。斯皮尔曼相关系数评估两个连续或有序变量之间的单调关系。
在单调关系中，变量倾向于一起变化，但不一定以恒定的速率变化。
斯皮尔曼相关系数基于每个变量的排名值，而不是原始数据。斯皮尔曼相关系数通常用于评估涉及序数变量的关系。例如，可以使用斯皮尔曼相关系数来评估员工完成测试练习的顺序是否与他们受雇的月数有关。

\[r_{s} = \rho_{\text{rg}_{X},\text{rg}_{Y}} = \frac{\text{cov}(\text{rg}_{X},\text{rg}_{Y})}{\sigma_{\text{rg}_{X}}\sigma_{\text{rg}_{Y}}}\]

公式 7‑5

### 7.5.2 假设检验

假设检验是统计学中一个强大的工具，用来确定一个结果是否具有统计显着性，这个结果是否偶然发生。spark.ml目前支持皮尔森卡方检验（\(x^{2}\)）独立性测试。ChiSquareTest针对标签上的每个特征进行皮尔逊独立性测试。对于每个特征，将(feature,
label)对转换为列矩阵，针对该列矩阵计算卡方统计量。所有标签和特征值必须是分类的。

```scala
scala> import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{Vector, Vectors}
scala> import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.ChiSquareTest
scala> val data = Seq(
| (0.0, Vectors.dense(0.5, 10.0)),
| (0.0, Vectors.dense(1.5, 20.0)),
| (1.0, Vectors.dense(1.5, 30.0)),
| (0.0, Vectors.dense(3.5, 30.0)),
| (0.0, Vectors.dense(3.5, 40.0)),
| (1.0, Vectors.dense(3.5, 40.0))
| )
data: Seq[(Double, org.apache.spark.ml.linalg.Vector)] =
List((0.0,[0.5,10.0]), (0.0,[1.5,20.0]), (1.0,[1.5,30.0]),
(0.0,[3.5,30.0]), (0.0,[3.5,40.0]), (1.0,[3.5,40.0]))
scala> val df = data.toDF("label", "features")
df: org.apache.spark.sql.DataFrame = [label: double, features: vector]
scala> val chi = ChiSquareTest.test(df, "features", "label").head
chi: org.apache.spark.sql.Row =
[[0.6872892787909721,0.6822703303362126],WrappedArray(2,
3),[0.75,1.5]]
scala> println("pValues = " + chi.getAs[Vector](0))
pValues = [0.6872892787909721,0.6822703303362126]
scala> println("degreesOfFreedom = " +
chi.getSeq[Int](1).mkString("[", ",", "]"))
degreesOfFreedom = [2,3]
scala> println("statistics = " + chi.getAs[Vector](2))
statistics = [0.75,1.5]
```

代码 7‑31

  - 卡方检验

卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，卡方值越大，越不符合；卡方值越小，偏差越小，越趋于符合，若两个值完全相等时，卡方值就为0，表明理论值完全符合。卡方检验是以χ2分布为基础的一种常用假设检验方法，它的无效假设\(\mathbf{H}_{\mathbf{0}}\)是：观察频数与期望频数没有差别。该检验的基本思想是：首先假设\(\mathbf{H}_{\mathbf{0}}\)成立，基于此前提计算出χ2值，它表示观察值与理论值之间的偏离程度。根据χ2分布及自由度可以确定在H0假设成立的情况下获得当前统计量及更极端情况的概率P。如果P值很小，说明观察值与理论值偏离程度太大，应当拒绝无效假设，表示比较资料之间有显著差异；否则就不能拒绝无效假设，尚不能认为样本所代表的实际情况和理论假设有差别。

### 7.5.3 摘要统计

在spark.ml包中，Summarizer提供了DataFrame的向量列摘要统计信息。可用的度量是每列数据的最大值、最小值、平均值、方差和非零数以及总数。下面的示例演示如何使用Summarizer为输入DataFrame的向量列（带有和不带有权重列）计算均值和方差。

```scala
scala> import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{Vector, Vectors}
scala> import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.stat.Summarizer
scala> import spark.implicits._
import spark.implicits._
scala> import Summarizer._
import Summarizer._
scala> val data = Seq(
| (Vectors.dense(2.0, 3.0, 5.0), 1.0),
| (Vectors.dense(4.0, 6.0, 7.0), 2.0)
| )
data: Seq[(org.apache.spark.ml.linalg.Vector, Double)] =
List(([2.0,3.0,5.0],1.0), ([4.0,6.0,7.0],2.0))
scala> val df = data.toDF("features", "weight")
df: org.apache.spark.sql.DataFrame = [features: vector, weight:
double]
scala> val (meanVal, varianceVal) = df.select(metrics("mean",
"variance")
| .summary($"features", $"weight").as("summary")).select("summary.mean",
"summary.variance").as[(Vector, Vector)].first()
meanVal: org.apache.spark.ml.linalg.Vector =
[3.333333333333333,5.0,6.333333333333333]
varianceVal: org.apache.spark.ml.linalg.Vector = [2.0,4.5,2.0]
scala> println(s"with weight: mean = ${meanVal}, variance =
${varianceVal}")
with weight: mean = [3.333333333333333,5.0,6.333333333333333],
variance = [2.0,4.5,2.0]
scala> val (meanVal2, varianceVal2) = df.select(mean($"features"),
variance($"features")).as[(Vector, Vector)].first()
meanVal2: org.apache.spark.ml.linalg.Vector = [3.0,4.5,6.0]
varianceVal2: org.apache.spark.ml.linalg.Vector = [2.0,4.5,2.0]
scala> println(s"without weight: mean = ${meanVal2}, sum =
${varianceVal2}")
without weight: mean = [3.0,4.5,6.0], sum = [2.0,4.5,2.0]
```

## 7.6 算法概述

MLlib
中包括了Spark的机器学习功能，实现了可以在计算机集群上并行完成的机器学习算法。MLlib拥有多种机器学习算法用于二元以及多元分类和回归问题的解决，这些方法包括线性模型、决策树和朴素贝叶斯方法等；使用基于交替最小二乘算法建立协同过滤推荐模型。在这一部分，将使用协同过滤算法来预测用户会喜欢什么样的电影。MLlib还提供了基于K均值的聚类，经常用于大型数据集的数据挖掘，支持使用RowMatrix类进行降维，提供奇异值分解和主成分分析的功能（表格 7‑1）。

| **机器学习算法** | **描述**                                |
| ---------- | ------------------------------------- |
| 分类和回归      | 包括线性模型、决策树和朴素贝叶斯                      |
| 协同过滤       | 支持基于交替最小二乘算法的协同过滤推荐模型                 |
| 聚集         | 支持K均值算法                               |
| 降维         | 支持使用RowMatrix类进行降维，其提供奇异值分解和主成分分析的功能。 |
| 特征抽取和转换    | 包括常用的几个特征转换的类                         |

表格 7‑1MLib中的机器学习算法

目前机器学习领域包括许多算法，同时还有更多新颖的算法被设计和开发。如果要搞清楚所有算法的原理，其学习曲线会非常陡峭。初学者经常面临着如何从各种各样的机器学习算法中选择解决不同问题的方法，要解决该问题可以考虑几个因素：数据量的大小和性质；可以接收的计算时间；任务的紧迫程度以及期望挖掘的内容。即使是经验丰富的数据科学家也无法在尝试所有可能的算法之前，就可以确定哪个算法性能最好。但是，初学者应该知道机器学习算法的功能和适用的实际问题。本小结只是希望能够提供一些指导性的建议，可以根据一些因素选择用于解决问题可能性较大的算法。本节提供两种方式来对机器学习算法进行分类：首先是通过学习方式进行分类，包括有监督学习和无监督学习，有监督学习算法使用带有标记的数据训练模型，而无监督学习算法不需要；第二种是按形式或功能上的相似性将算法分组。

![](media/07_machine_learning/media/image3.png)

图例 7‑3机器学习算法

### 7.6.1 有监督学习

利用一组已知类别的样本训练分类器（用于分类的机器学习算法称为分类器），然后调整分类器的参数使其达到一定的分类精度，这个过程被称为有监督学习。有监督学习是通过有标记的训练数据进行建模，然后进行推断的机器学习任务。训练数据包括实际的分类结果，可以将其理解为试卷的正确答案，学生可以通过正确答案判断是否学会了对应的知识点，这个过程可以理解为有监督学习的建模。对于分类器来说，就是找到特定方程的解法。对于相似的新问题，学生通过获得地知识找对应的答案，这个过程可以看作分类器进行推断的过程（使用上面的解法带入新的数据，最后得到结果）。在有监督学习中，每个训练数据实例都是由一个输入对象（通常为一组向量，称为特征）和一个期望的输出值（也称为标签）组成。监督学习算法分析训练数据，产生一个可以用来推断的运算方程，可以通过新的输入对象计算出新的输出值。在训练过程中，如果得到性能最佳的运算方程，将允许使用该运算方程来预测那些未分类的实例。这就要求机器学习算法是通过监督学习的方式从一种已知数据推断出未知的结果。监督学习可以分为两类：分类和回归。

分类（Classification）：如果数据用来预测一个类别变量，那么监督学习算法又称作分类，比如对一张图片标注标签（狗或者猫）。如果只有两个标签，则称作二元分类问题，当类别大于两个时，称作多元分类问题。分类的例子包括垃圾邮件检测、客户流失预测、情感分析、犬种检测等。

回归（Regression）：当预测变量为连续变量时，问题就转化为一个回归问题。回归问题根据先前观察到的数据预测数值；回归的例子包括房价预测、股价预测、身高-体重预测等。

分类算法是解决分类问题的方法，是数据挖掘、机器学习和模式识别中一个重要的研究领域。分类算法通过对已知类别训练集的分析，从中发现分类规则，以此预测新数据的类别。分类算法的应用非常广泛，银行中风险评估、客户类别分类、文本检索和搜索引擎分类、安全领域中的入侵检测以及软件项目中的应用等等。基于监督学习的分类算法将输入数据指定属于若干预先定义的分类中。一些常见的使用分类的案例包括信用卡欺诈检测和垃圾邮件检测，这两者都是二元分类问题，只有两种类别（是与非）。分类数据被标记，例如标记为垃圾邮件或非垃圾邮件、欺诈或非欺诈。分类算法通过训练模型为新数据分配标签或类别，可以根据预先确定的特征进行分类。

一个容易理解的分类例子是垃圾邮件识别：训练数据里已经标注了“垃圾”或“非垃圾”，模型要学习的是如何根据发件人、主题、正文特征为新邮件分配标签。类似地，信用风险判断、欺诈检测、用户流失预测，也都属于“根据已知标签学习分类边界”的问题。与之相对，后面要讲的协同过滤更偏向推荐场景，它关心的是在历史行为基础上预测用户可能喜欢什么。

### 7.6.2 无监督学习

如果训练数据没有标签，任务就转成了无监督学习。此时模型不会直接学习“正确答案”，而是尝试从数据本身的分布中找结构、找模式、找相似性。无监督学习适合那些人工标注成本较高，或者我们本来就想先探索数据结构的场景，例如客户分群、新闻聚类、相似商品发现和异常点检测。

聚类是最典型的无监督学习任务之一。它不依赖预先给定的类别，而是根据样本之间的相似程度把数据划分成若干组。理想情况下，同一簇内部的样本更相似，不同簇之间的样本差异更大。图例 7‑4 给出了最直观的示意：原始数据点分布在平面上，聚类算法试图把它们整理成若干具有业务意义的分组。

![](media/07_machine_learning/media/image4.png)

图例 7‑4聚类分组

以 K-Means 为例，算法会先初始化若干个簇中心，然后在每一轮迭代中完成两件事：先把每个样本分配给最近的中心，再根据新分配结果重新计算各簇中心。这个过程会持续到中心变化足够小、达到最大迭代次数，或者目标函数基本稳定为止。K-Means 简单高效，因此常被用作无监督学习的入门算法；但它对簇数选择、异常值和尺度差异比较敏感，实践中通常需要结合特征标准化与指标评估一起使用。

### 7.6.3 多种算法介绍

从功能角度来说，可以根据机器学习算法的共性（比如功能或运作方式）对机器学习算法分类，例如基于树的方法和基于神经网络的方法。一般来说，这是对机器学习算法进行分组的最有效的方法，但仍然有一些算法可以适合多个类别，例如“学习矢量量化”既是神经网络启发性方法又是基于实例的方法。下面的介绍尽量选择主观上最适合的分组来处理这些情况。

#### 7.6.3.1 回归算法

回归算法涉及将变量之间的关系进行建模，然后使用模型进行的预测，通过度量预测值与实际值之间的误差度量完成变量之间关系的迭代完善。回归算法是数学建模、分类和预测中最古老但功能非常强大的工具之一，已被选入统计机器学习中。目前，可能的回归算法包括：普通最小二乘回归、线性回归、逻辑回归、逐步回归、多元自适应回归样条、局部估计的散点图平滑。

#### 7.6.3.2 基于实例算法

基于实例的学习模型是一个决策问题，模型中包含训练数据的实例。通过与存储在模型中的训练数据做比较，对新输入的实例生成一个类别标签。模型训练过程不会从具体实例中生成抽象结果。这样的算法通常为示例数据建立的数据库，并使用相似性度量将新数据与数据库中的数据进行比较，以便找到最佳匹配并做出预测。基于实例算法的重点是存储实例以及实例之间的相似性度量。目前，可能的基于实例算法包括：k最近邻居、学习矢量量化、自组织图、本地加权学习、支持向量机。

#### 7.6.3.3 正则化算法

正则化算法是对另一种算法（通常是回归算法）的扩展。该算法根据模型的复杂性对模型进行优化，而倾向于更易于泛化的简单模型。在这里单独列出了正则化算法，因为它们是流行的，功能强大的，且通常简单的对其他算法进行修改。目前，可能的正则化算法包括：岭回归、最小绝对收缩和选择算子、弹性网、最小角度回归。

#### 7.6.3.4 决策树算法

决策树方法构建了一个决策模型，是基于数据中属性的实际值制定的。决策树有很多不同的变体，但是他们都是做相同的事情，细分特征空间到最相似标签的区域。决策树最容易理解和实现，但是当分支耗费过多而且树非常深时，很容易导致过拟合。决策树可以进行有关分类和回归问题的数据训练。决策树通常快速、准确，在机器学习中很受欢迎。目前，可能决策树算法是：分类和回归树、迭代二分频器3（ID3）、C4.5和C5.0（不同版本）、卡方自动互动检测（CHAID）、决策树桩、M5、条件决策树。

#### 7.6.3.5 贝叶斯算法

贝叶斯算法是将贝叶斯定理明确应用于分类和回归等问题。当前，可能的贝叶斯算法是：朴素贝叶斯、高斯朴素贝叶斯、多项式朴素贝叶斯、平均一依赖估计量（AODE）、贝叶斯信仰网络（BBN）、贝叶斯网络（BN）。

#### 7.6.3.6 聚类算法

像回归一样，聚类描述问题的类别和方法的类别。聚类方法通常通过建模方法（例如基于质心和层次的方法）进行组织，涉及使用数据中的固有结构来最优地将数据组织成具有最大共性的集合。当前，可能的聚类算法包括：k均值、k中位数、期望最大化（EM）、层次聚类。

#### 7.6.3.7 关联规则学习算法

关联规则学习方法提取的规则，可以最佳地解释数据中变量之间的关系。这些规则可以在大型多维数据集中发现重要和有用的关联。目前，可能的关联规则学习算法包括：Apriori算法、离散算法。

#### 7.6.3.8 人工神经网络算法

人工神经网络是受生物神经网络的结构和功能启发而设计的模型。它们属于模式匹配的类别，通常用于回归和分类问题。实际上，这个算法是一个巨大的子类别，由数百种算法和各种问题类型的变体组成。深度学习也是神经网络一个当前发展最快分支，单独进行分类，这里只是列举了比较经典的人工神经网络算法。目前，可能的人工神经网络算法包括：感知器、多层感知器（MLP）、反向传播、随机梯度下降、霍普菲尔德网络、径向基函数网络（RBFN）。

#### 7.6.3.9 深度学习算法

深度学习方法是人工神经网络的一种现代更新，利用当前大量廉价的计算实现。深度学习算法关注的是构建更大和更复杂的神经网络，并且如上所述，许多算法都涉及被标记的模拟超大数据集，例如图像、文本、音频和视频。目前，可能的深度学习算法包括：卷积神经网络（CNN）、递归神经网络（RNN）、长短期记忆网络（LSTM）、堆叠式自动编码器、深玻尔兹曼机（DBM）、深度信仰网络（DBN）。

#### 7.6.3.10 降维算法

像聚类方法一样，降维算法寻找和利用数据中的固有结构，通过无监督学习的方式用较少的信息来汇总或描述数据。如果数据维度很高，可视化会变得相当困难，降维算法可以增强数据可视化，也可以用于删除冗余特征解决多重共线性问题。另外，低维数据有助于减少存储空间和训练的时间。目前，可能的减维算法包括：主成分分析（PCA）、主成分回归（PCR）、偏最小二乘回归（PLSR）、萨蒙地图、多维缩放（MDS）、投影追踪、线性判别分析（LDA）、混合判别分析（MDA）、二次判别分析（QDA）、弹性判别分析（FDA）。

#### 7.6.3.11 集成算法

集成算法是将多个较弱的模型组成，每个模型经过独立训练，其预测以某种方式组合在一起进行总体预测。集成算法主要解决是组合哪些弱学习模型以及如何将它们组合在一起。目前，可能的集成算法包括：提升（Boosting）、自举聚合（Bagging）、AdaBoost、加权平均值（Blending）、堆叠概括（Stacking）、梯度提升机（GBM）、梯度增强回归树（GBRT）、随机森林。

  - 有监督和无监督机器学习的区别是什么？

  - 为什么分类被看作是有监督学习的算法，而聚类被看做无监督学习的算法？

  - 基于客户原来的评价，分析或许喜欢哪个餐厅，应该使用什么算法？

  - 检测通过欺诈尝试登录Web网站，应该使用什么算法？

  - 判断哪些学生可以通过考试，哪些不能，应该使用什么算法？

  - 基于类别元数据，对音乐专辑进行分类，应该使用什么算法？

### 7.6.4 协同过滤

推荐系统的目标，是根据用户历史行为推断“这个用户接下来可能喜欢什么”。在电商、视频、音乐和资讯场景里，这通常意味着结合评分、点击、收藏、停留时长等信号，为用户排序出一组候选内容。协同过滤是其中最经典的一类方法，它不要求先理解商品的完整语义，而是直接从“用户和物品之间的交互关系”中学习偏好模式。协同过滤最常见的两种思路如下：

（1）基于用户的协同过滤：先找出“和目标用户行为相似的一群用户”，再根据这些相似用户喜欢但目标用户尚未接触的项目生成推荐。它强调的是“相似用户会做出相似选择”。

![](media/07_machine_learning/media/image5.png)

图例 7‑5 基于用户的协同过滤

图例 7‑5 展示了一个最小示意。假设用户 A 和用户 C 都喜欢项目 A、C，而用户 C 还喜欢项目 D，那么系统就可能把 D 推荐给 A，因为 C 的历史偏好为 A 提供了相似用户参考。

（2）基于项目的协同过滤：先根据所有用户的行为数据计算“项目与项目之间的相似度”，再把与用户已喜欢项目相近的其他项目推荐给该用户。它强调的是“喜欢 A 的人往往也喜欢和 A 相似的 B”。

![](media/07_machine_learning/media/image6.png)

图例 7‑6基于项目的协同过滤

图例 7‑6 展示的就是这种思路：如果大量用户同时喜欢项目 A 和项目 C，那么系统会认为这两个项目相近；当用户 C 表现出对 A 的兴趣时，就有理由把 C 推荐给他。

## 7.7 交叉验证

交叉验证用于评估机器学习模型应用到新数据上的稳定性，主要用于目标是预测的任务中，估计预测模型在实践中的执行准确度。在训练数据上拟合的模型，是无法保证能够准确地应用于从未见过的真实数据，这是因为训练数据和真实数据中样本的概率分布可能不同，可能会有噪声产生对模型训练的影响。所以，在训练模型是，需要尽量保证可以正确地从数据中获取大多数模式，并且噪声不会产生过多影响。于是可以先在一个子集上做分析，而其他子集则用来后续进行分析的确认及验证。一开始的子集被称为训练集。而其他的子集则被称为验证集或测试集合。拟合的过程是对模型的参数进行调整，以使模型尽可能反映训练集的特征。如果从同一个训练样本中选择独立的样本作为验证集合，当模型因训练集过小或参数不合适而产生过拟合时，可以通过验证集的予以评估。所以，交叉验证也是一种预测模型拟合性能的方法。常见的交叉验证形式包括：

  - Hold-Out验证

随机从最初的样本中选出部分，形成交叉验证数据，而剩余的就当做训练数据。Hold-Out是基本的划分方法，字面意思就是留出来一部分，即将数据集直接按照一定比例划分，例如常用的2/8与3/7，含义为训练数据为
80% 或 70%，相应的测试数据占 20% 或
30%。然而，这种方式只进行了一次划分，数据结果具有偶然性，如果在某次划分中，训练集的数据不具有普遍性，而测试集里的数据存在不同的统计分布，这样就会导致最终的预测结果不尽如意。

  - 标准K折交叉验证

标准K折交叉验证，将训练集分割成K个子样本，一个单独的子样本被保留作为验证模型的数据，其他K −
1个样本进行训练。交叉验证重复k次，每个子样本被验证一次，平均K次的结果或使用其他结合方式，最终得到一个单一模型估计测度。其中，K一般取5或10。如果采用holdout验证方式，数据划分具有偶然性；交叉验证通过多次划分，大大降低了这种由一次随机划分带来的偶然性，同时通过多次划分，多次训练，模型也能遇到各种各样的数据，从而提高其泛化能力。与Hold-Out相比，对数据的使用效率更高，如果Hold-Out训练数据与测试数据比例为3:1，如果是5折交叉验证，训练集比测试集为4:1；如果是10折交叉验证，训练集比测试集为9:1，数据量越大，模型准确率越高。

  - 分层K折交叉验证

在某些情况下，训练数据的响应变量可能存在很大的不平衡。例如，在有关房屋价格的数据集中，可能有大量具有高价格的房屋；或者在分类的情况下，阴性样品可能比阳性样品多几倍。对于此类问题，对标准K折交叉验证中进行细微的变化，使每折数据包含每个目标类别的样本百分比与全部数据中的样本相同，或者对于预测问题，平均响应值约为在所有倍数上相等。这种变化也称为分层K折。

上面说明的交叉验证技术也称为非穷举交叉验证，不需要计算所有分割原始样本的方法，只需要确定需要分割出多少个子集即可。下面说明的方法称为穷举交叉验证，该方法计算将数据分为训练集和测试集的所有可能方式。

  - 留一验证

留一验证意指只使用一个样本作为验证数据，而剩余的则留下来当做训练数据，是一种特殊的交叉验证方式。如果样本容量为N，则K=N，进行N折交叉验证，每次留下一个样本进行验证，主要针对小样本数据。

## 7.8 机器学习管道

本节是本章最重要的工程化部分。现实中的机器学习任务很少只是“调用一个算法就结束”，通常还包括数据清洗、特征提取、模型训练、评估、调参与推理输出。Pipeline 的作用，就是把这些阶段组织成一条可重复、可保存、可复用的处理链路。

在 Spark 中，机器学习管道主要由三类对象协作完成：Transformer 负责把一个 DataFrame 转成另一个 DataFrame；Estimator 负责在训练数据上调用 `fit()` 生成模型；Evaluator 负责度量模型效果并为调参提供依据。把这些部件按顺序组合起来，才能让训练集、验证集和线上推理使用同一套前处理逻辑。

从 Spark 4.x 的实践角度看，Pipeline 不是“可选高级话题”，而是 `spark.ml` 的默认工作方式。只要任务包含特征工程和模型训练，就应该优先思考如何把流程写成可复用的 Pipeline。

### 7.8.1 概念介绍

![https://miro.medium.com/max/1360/1\*c4SNMDj18FHQakGS6Gmgsg.png](media/07_machine_learning/media/image7.png)

图例 7‑7 Spark 机器学习工作流

在构建管道的初始阶段，数据质量及其可访问性是将遇到的两个主要挑战。如果要想从原始数据中获得有意义的信息，机器学习管道的首要任务就是定义数据收集和数据清理，但是在实际应用环境中会遇到很大的难度。机器学习管道通常还涉及一系列数据转换和训练模型阶段，会按照一定的标准被定义为操作序列，其中包括转换器（Transformer）、估算器（Estimator）、评价器（Evaluator）。这些阶段按顺序执行，并且在流经管道中的每个阶段时输入的数据被进行了转换。

机器学习开发框架需要通过支持分布式计算的实用程序来帮助组装管道组件，还需要的功能包括容错能力、资源管理、可扩展性和可维护性。另外，机器学习开发框架还包括模型导入/导出、交叉验证以选择参数以及从多个数据源汇总数据；特征提取、选择和统计；管道的持久化，可以保存和加载模型以备。

### 7.8.2 Spark 管道

Spark 的机器学习管道 API 位于 `org.apache.spark.ml` 包中，建立在 DataFrame 之上，能够与特征工程、模型、评估器和参数网格搜索自然组合。这也是为什么本章把 Pipeline 视为新项目主线，而把 RDD 风格 API 视为兼容材料。

  - 转换器（Transformer）

转换器是一种抽象，包含特征转换器和训练过的模型。在技术上讲，转换器实现了方法transform()，其功能就是将一个DataFrame附加一个或多个列转换成另一个。例如，特征转换器可以使用DataFrame读取初始数据列，然后将其映射成新的特征向量列，生成新的DataFrame；训练过的模型通过DataFrame读取包含特征向量的列预测每个特征向量的标签，然后输出一个新的DataFrame包括了预测标签。

  - 估算器（Estimator）

估算器抽象了学习算法的概念，或者任何拟合和训练数据的算法。在技术上讲，估算器实现了方法fit()，用来接受DataFrame并产生模型（转换器）。例如，LogisticRegression算法是一个估算器，调用fit()训练出LogisticRegressionModel，它是模型也是转换器。

在真实项目里，模型训练通常不是“一个算法调用”就结束了，而是由一串有顺序约束的步骤组成。例如在文本分类任务中，你可能先做分词，再做向量化，最后训练分类器；在推荐或风控任务中，则常常先做数据清洗、类别编码、特征拼接，再进入模型训练和评估。Spark 把这类流程统一抽象为 Pipeline：它由多个 `PipelineStage` 顺序组成，每个阶段要么负责转换数据，要么负责根据训练数据拟合模型。

运行时可以把 Pipeline 理解成一条固定的数据处理链。对于 Transformer，Spark 会直接调用 `transform()` 把一个 DataFrame 变成另一个 DataFrame；对于 Estimator，Spark 会先调用 `fit()` 产出训练后的模型，再把这个模型作为后续链路中的 Transformer 使用。训练完成后，整条链路会固化成一个 `PipelineModel`，从而让训练集、验证集和线上推理都复用同一套预处理步骤。下图（图例 7‑8）展示了这条链路如何作用在训练数据上。

![L Pipeline Example](media/07_machine_learning/media/image8.jpeg)

图例 7‑8训练模型阶段使用的机器学习管道

图例 7‑8 中，`Tokenizer` 和 `HashingTF` 负责把原始文本逐步变成可训练的特征列，`LogisticRegression` 则负责在这些特征上拟合分类模型。调用 `Pipeline.fit()` 时，前面的转换器依次改写 DataFrame，最后由估算器产出训练后的模型；等整条链路完成拟合后，Spark 会返回一个 `PipelineModel`。随后，测试集或线上数据只需要调用这个 `PipelineModel` 的 `transform()`，就能沿着相同的步骤得到预测结果。这样做的价值在于：训练时用什么预处理逻辑，推理时就沿用什么逻辑，从而减少“训练和上线不一致”的风险。

![L PipelineModel Example](media/07_machine_learning/media/image9.jpeg)

图例 7‑9机器学习管道应用在测试数据的过程

Pipeline 中的阶段按顺序放在一个数组里。最常见的是线性链路，也就是后一阶段直接消费前一阶段产出的列；如果数据流能组成有向无环图，也可以构造更复杂的非线性 Pipeline，但这时就需要格外注意各阶段的输入列、输出列和先后顺序。

由于 Pipeline 要处理的是运行时 DataFrame，而不是编译期静态类型，很多合法性检查都会推迟到运行阶段完成。Spark 会根据 DataFrame 的 schema 校验列名、列类型和阶段依赖是否匹配。因此，给每个阶段明确设置输入列和输出列，是保持管道可读性与可维护性的关键做法。

另外，Pipeline 里的阶段实例应该保持唯一。如果你需要两次使用 `HashingTF`，最好创建两个独立实例并分别配置各自参数，而不是把同一个对象重复塞进阶段数组。这样做既能避免 ID 冲突，也更方便排查参数配置问题。

估算器和转换器使用统一的API来指定参数。Param是具有独立文件的命名参数，ParamMap是一组键值对(参数,值)，将参数传递给算法有两种主要方法：

（1）为实例设置参数，如果lr是LogisticRegression的实例，则可以调用lr.setMaxIter(10)使lr.fit()最多使用10次迭代。

（2）将ParamMap传递给fit()或transform()方法。ParamMap中的任何参数都将覆盖以前通过setter方法指定的参数。

参数属于估计器和转换器的特定实例。如果有两个LogisticRegression实例lr1和lr2，则可以使用指定的两个maxIter参数构建ParamMap，ParamMap(lr1.maxIter-\>
10，lr2.maxIter-\>
20)。如果管道中有两个带有maxIter参数的算法，这种方式将很有用。通常，将模型或管道保存到存储中可以供以后使用。从Spark
1.6开始，模型导入和导出的功能已添加到API中。下面的示例描述了文本处理的机器学习管道执行的过程。

```scala
scala> import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
scala> import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegression
scala> import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
scala> import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vector
scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
```

代码 7‑32

准备训练数据：

```scala
scala> val training = spark.createDataFrame(Seq(
| (0L, "a b c d e spark", 1.0),
| (1L, "b d", 0.0),
| (2L, "spark f g h", 1.0),
| (3L, "hadoop mapreduce", 0.0)
| )).toDF("id", "text", "label")
training: org.apache.spark.sql.DataFrame = [id: bigint, text: string
... 1 more field]
scala> training.show
+---+----------------+-----+
| id| text|label|
+---+----------------+-----+
| 0| a b c d e spark| 1.0|
| 1| b d| 0.0|
| 2| spark f g h| 1.0|
| 3|hadoop mapreduce| 0.0|
+---+----------------+-----+
```

代码 7‑33

配置ML管道，由三个阶段组成：tokenizer、hashingTF和lr：

```scala
scala>val tokenizer = new
Tokenizer().setInputCol("text").setOutputCol("words")
tokenizer: org.apache.spark.ml.feature.Tokenizer = tok_d33aebff5942
scala>val hashingTF = new
HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
hashingTF: org.apache.spark.ml.feature.HashingTF =
hashingTF_214a5ff53c51
scala>val lr = new
LogisticRegression().setMaxIter(10).setRegParam(0.001)
lr: org.apache.spark.ml.classification.LogisticRegression =
logreg_6b1ab2bbcffb
scala>val pipeline = new Pipeline().setStages(Array(tokenizer,
hashingTF, lr))
pipeline: org.apache.spark.ml.Pipeline = pipeline_b07867b9cf3e
```

代码 7‑34

使用fit()方法将训练文档传递给管道：

```scala
scala> val model = pipeline.fit(training)
model: org.apache.spark.ml.PipelineModel = pipeline_b07867b9cf3e
```

代码 7‑35

现在，可以将拟合后的管道保存：

```scala
scala>
model.write.overwrite().save("/tmp/spark-logistic-regression-model")
```

代码 7‑36

也可以将没有执行fit()的管道保存：

```scala
scala> pipeline.write.overwrite().save("/tmp/unfit-lr-model")
```

代码 7‑37

加载保存的管道：

```scala
scala> val sameModel =
PipelineModel.load("/tmp/spark-logistic-regression-model")
sameModel: org.apache.spark.ml.PipelineModel = pipeline_b07867b9cf3e
```

代码 7‑38

准备测试文档，为没有标记的元组(id, text)：

```scala
scala> val test = spark.createDataFrame(Seq(
| (4L, "spark i j k"),
| (5L, "l m n"),
| (6L, "spark hadoop spark"),
| (7L, "apache hadoop")
| )).toDF("id", "text")
test: org.apache.spark.sql.DataFrame = [id: bigint, text: string]
```

代码 7‑39

在测试文档上进行预测：

```scala
scala> model.transform(test).select("id", "text", "probability",
| "prediction").collect().foreach { case Row(id: Long,
| text: String, prob: Vector, prediction: Double) =>
| println(s"($id, $text) --> prob=$prob, prediction=$prediction")
| }
(4, spark i j k) --> prob=[0.15964077387874118,0.8403592261212589],
prediction=1.0
(5, l m n) --> prob=[0.8378325685476612,0.16216743145233875],
prediction=0.0
(6, spark hadoop spark) -->
prob=[0.06926633132976273,0.9307336686702373], prediction=1.0
(7, apache hadoop) --> prob=[0.9821575333444208,0.01784246665557917],
prediction=0.0
```

代码 7‑40

打印出参数对（名称：数值），名称中包含LogisticRegression实例的唯一ID(20733c862f55)：

```scala
scala> println("Model was fit using parameters: " + lr.extractParamMap)
Model was fit using parameters: {
logreg_20733c862f55-aggregationDepth: 2,
logreg_20733c862f55-elasticNetParam: 0.0,
logreg_20733c862f55-family: auto,
logreg_20733c862f55-featuresCol: features,
logreg_20733c862f55-fitIntercept: true,
logreg_20733c862f55-labelCol: label,
logreg_20733c862f55-maxIter: 10,
logreg_20733c862f55-predictionCol: prediction,
logreg_20733c862f55-probabilityCol: probability,
logreg_20733c862f55-rawPredictionCol: rawPrediction,
logreg_20733c862f55-regParam: 0.001,
logreg_20733c862f55-standardization: true,
logreg_20733c862f55-threshold: 0.5,
logreg_20733c862f55-tol: 1.0E-6
}
```

代码 7‑41

可替代地使用ParamMap中的不同方法指定参数：

```scala
scala> import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.ParamMap
scala> val paramMap = ParamMap(lr.maxIter -> 20).put(lr.maxIter,
30).put(lr.regParam -> 0.1, lr.threshold -> 0.55)
paramMap: org.apache.spark.ml.param.ParamMap =
{
logreg_20733c862f55-maxIter: 30,
logreg_20733c862f55-regParam: 0.1,
logreg_20733c862f55-threshold: 0.55
}
```

代码 7‑42

从上面的例子中可以看到，maxIter参数被重新赋值，并替代了原来的值，而且一个put()可以定义多个参数。

组合多个ParamMap：

```scala
scala> val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
paramMap2: org.apache.spark.ml.param.ParamMap =
{
logreg_20733c862f55-probabilityCol: myProbability
}
scala> val paramMapCombined = paramMap ++ paramMap2
paramMapCombined: org.apache.spark.ml.param.ParamMap =
{
logreg_20733c862f55-maxIter: 30,
logreg_20733c862f55-probabilityCol: myProbability,
logreg_20733c862f55-regParam: 0.1,
logreg_20733c862f55-threshold: 0.55
}
```

代码 7‑43

使用paramMapCombined中定义的参数学习新的模型，并且覆盖之前lr中参数：

```scala
scala> val model2 = pipeline.fit(training, paramMapCombined)
model2: org.apache.spark.ml.PipelineModel = pipeline_7a5c909d61d5
scala> val logRegModel =
model2.stages.last.asInstanceOf[org.apache.spark.ml.classification.LogisticRegressionModel]
logRegModel: org.apache.spark.ml.classification.LogisticRegressionModel
= LogisticRegressionModel: uid = logreg_20733c862f55, numClasses = 2,
numFeatures = 1000
scala> println("Model 2 was fit using parameters: " +
logRegModel.extractParamMap)
Model 2 was fit using parameters: {
logreg_20733c862f55-aggregationDepth: 2,
logreg_20733c862f55-elasticNetParam: 0.0,
logreg_20733c862f55-family: auto,
logreg_20733c862f55-featuresCol: features,
logreg_20733c862f55-fitIntercept: true,
logreg_20733c862f55-labelCol: label,
logreg_20733c862f55-maxIter: 30,
logreg_20733c862f55-predictionCol: prediction,
logreg_20733c862f55-probabilityCol: myProbability,
logreg_20733c862f55-rawPredictionCol: rawPrediction,
logreg_20733c862f55-regParam: 0.1,
logreg_20733c862f55-standardization: true,
logreg_20733c862f55-threshold: 0.55,
logreg_20733c862f55-tol: 1.0E-6
}
```

代码 7‑44

### 7.8.3 模型选择

本节介绍如何使用MLlib提供的工具调整机器学习算法和管道，其内置的交叉验证和其他工具允许用户优化算法和管道中的超参数。机器学习中的一个重要任务是选择模型，或者使用数据为给定任务找到最佳模型或参数，这也被称为调优。可以对单个估计器进行调整，也可以对整个机器学习管道（包括多个算法、特征工程和其他步骤）进行调整，可以一次调优整个管道，而不是分别对管道中的每个元素进行调优。MLlib支持使用CrossValidator和TrainValidationSplit等工具进行模型选择，其中涉及到的组件和项目包括：估计器是需要调优的的算法或管道；ParamMap设置可供选择的参数，有时通过ParamGridBuilder设置参数网格；评估器（Evaluator）是用来在测试数据上衡量拟合模型表现如何的指标。当在程序中应用这些组件和项目时，其工作的过程如下：

（1）将输入的数据按照一定的比例和方法分成独立的训练和测试数据集；

（2）对于每个训练数据和测试数据的组合，遍历ParamMap中的参数，使用这些参数训练估算器得到模型，并使用评估器验证模型的性能；

（3）然后选择性能最佳的参数组产生最终的模型。

针对回归问题，评估器可以选择RegressionEvaluator；针对二元分类问题，可以选择BinaryClassificationEvaluator；或针对多元分类问题，可以选择MulticlassClassificationEvaluator。在选择最佳ParamMap的参数组时，默认度量指标可以由评估器中的setMetricName方法设置。

#### 7.8.3.1 CrossValidator

CrossValidator首先将数据集分成几组，可以分别用作训练数据和测试数据。例如，当K =
3倍时，CrossValidator将生成3个(training, test)
数据组对，其中每对使用了2/3的数据用来训练模型和1/3的数据用来测试模型。CrossValidator首先将数据集分成一组折叠，这些折叠用作单独的训练和测试数据集。为了评估特定的ParamMap，通过将估算器拟合到3个不同的(training,
test)
数据集对上，CrossValidator为3个模型计算出平均评估指标。确定最佳的ParamMap之后，CrossValidator最终使用最佳的ParamMap和整个数据集重新拟合估算器。

下面的示例演示如何使用CrossValidator从参数网格中进行选择。请注意，在参数网格上进行交叉验证的成本很高。在下面的示例中，参数网格中hashingTF.numFeatures有3个值和lr.regParam有2个值，而CrossValidator使用2折。将这几个数相乘，会有
(3\*2)\*2=12不同模型被训练。在实际设置中，尝试更多的参数并使用更多的折叠数（通常k = 3和k =
10）是很常见的。换句话说，虽然使用CrossValidator可能成本很高，需要的计算时间比较长，但是这也是一种公认的用于选择参数的方法，该方法在统计上比启发式手动调整更合理。

```scala
scala> import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.Pipeline
scala> import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegression
scala> import
org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
scala> import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
scala> import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vector
scala> import org.apache.spark.ml.tuning.{CrossValidator,
ParamGridBuilder}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
```

代码 7‑45

准备训练数据：

```scala
scala> val training = spark.createDataFrame(Seq(
| (0L, "a b c d e spark", 1.0),
| (1L, "b d", 0.0),
| (2L, "spark f g h", 1.0),
| (3L, "hadoop mapreduce", 0.0),
| (4L, "b spark who", 1.0),
| (5L, "g d a y", 0.0),
| (6L, "spark fly", 1.0),
| (7L, "was mapreduce", 0.0),
| (8L, "e spark program", 1.0),
| (9L, "a e c l", 0.0),
| (10L, "spark compile", 1.0),
| (11L, "hadoop software", 0.0)
| )).toDF("id", "text", "label")
training: org.apache.spark.sql.DataFrame = [id: bigint, text: string
... 1 more field]
scala> training.show
+---+----------------+-----+
| id| text|label|
+---+----------------+-----+
| 0| a b c d e spark| 1.0|
| 1| b d| 0.0|
| 2| spark f g h| 1.0|
| 3|hadoop mapreduce| 0.0|
| 4| b spark who| 1.0|
| 5| g d a y| 0.0|
| 6| spark fly| 1.0|
| 7| was mapreduce| 0.0|
| 8| e spark program| 1.0|
| 9| a e c l| 0.0|
| 10| spark compile| 1.0|
| 11| hadoop software| 0.0|
+---+----------------+-----+
```

代码 7‑46

配置ML管道，有三个阶段组成：tokenizer、hashingTF和lr：

```scala
scala>val tokenizer = new
Tokenizer().setInputCol("text").setOutputCol("words")
tokenizer: org.apache.spark.ml.feature.Tokenizer = tok_2d74c623b391
scala>val hashingTF = new
HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features")
hashingTF: org.apache.spark.ml.feature.HashingTF =
hashingTF_9d7dce2a61ed
scala>val lr = new LogisticRegression().setMaxIter(10)
lr: org.apache.spark.ml.classification.LogisticRegression =
logreg_ea85c25b7a9e
scala>val pipeline = new Pipeline().setStages(Array(tokenizer,
hashingTF, lr))
pipeline: org.apache.spark.ml.Pipeline = pipeline_51032200df93
```

代码 7‑47

使用ParamGridBuilder构建一个参数网格来保存和检索参数：

```scala
scala>val paramGrid = new
ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(10, 100,
1000)).addGrid(lr.regParam,Array(0.1, 0.01)).build()
paramGrid: Array[org.apache.spark.ml.param.ParamMap] =
Array({
hashingTF_9d7dce2a61ed-numFeatures: 10,
logreg_ea85c25b7a9e-regParam: 0.1
}, {
hashingTF_9d7dce2a61ed-numFeatures: 100,
logreg_ea85c25b7a9e-regParam: 0.1
}, {
hashingTF_9d7dce2a61ed-numFeatures: 1000,
logreg_ea85c25b7a9e-regParam: 0.1
}, {
hashingTF_9d7dce2a61ed-numFeatures: 10,
logreg_ea85c25b7a9e-regParam: 0.01
}, {
hashingTF_9d7dce2a61ed-numFeatures: 100,
logreg_ea85c25b7a9e-regParam: 0.01
}, {
hashingTF_9d7dce2a61ed-numFeatures: 1000,
logreg_ea85c25b7a9e-regParam: 0.01
})
```

代码 7‑48

hashingTF.numFeatures有3个值，lr.regParam有2个值，这个网格有3 x 2 =
6种参数合组合。现在，我们将管道视为估算器，将其包装在CrossValidator实例中。这将使我们能够为所有Pipeline阶段共同选择参数。CrossValidator需要一个估算器、参数网格和评估器，此处的评估器是BinaryClassificationEvaluator及其默认指标是areaUnderROC。

```scala
scala> val cv = new
CrossValidator().setEstimator(pipeline).setEvaluator(new
BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)
cv: org.apache.spark.ml.tuning.CrossValidator = cv_16c0f7c44720
```

代码 7‑49

运行交叉验证，选择最好的参数组合：

```scala
scala> val cvModel = cv.fit(training)
cvModel: org.apache.spark.ml.tuning.CrossValidatorModel =
cv_16c0f7c44720
```

代码 7‑50

准备测试文档：

```scala
scala> val test = spark.createDataFrame(Seq(
| (4L, "spark i j k"),
| (5L, "l m n"),
| (6L, "mapreduce spark"),
| (7L, "apache hadoop")
| )).toDF("id", "text")
test: org.apache.spark.sql.DataFrame = [id: bigint, text: string]
```

代码 7‑51

cvModel使用最好的模型在测试文档上进行预测：

```scala
scala>cvModel.transform(test).select("id", "text", "probability",
"prediction").collect().foreach { case Row(id: Long,
| text: String, prob: Vector, prediction: Double) =>
| println(s"($id, $text) --> prob=$prob, prediction=$prediction")
| }
(4, spark i j k) --> prob=[0.25803432137769916,0.7419656786223008],
prediction=1.0
(5, l m n) --> prob=[0.9187600482920034,0.08123995170799662],
prediction=0.0
(6, mapreduce spark) -->
prob=[0.43181531442975374,0.5681846855702464], prediction=1.0
(7, apache hadoop) --> prob=[0.6766544523285499,0.32334554767145013],
prediction=0.0
```

代码 7‑52

#### 7.8.3.2 TrainValidationSplit

除CrossValidator外，Spark还提供TrainValidationSplit用于超参数调整。
TrainValidationSplit仅对每个参数组合进行一次评估，而对于CrossValidator而言需要进行K次评估，因此计算成本低，但是当训练数据集不够大时，不会产生可靠的结果。与CrossValidator不同，TrainValidationSplit创建单个(training,
test) 数据对，使用trainRatio参数将数据集分为这两部分，例如在trainRatio =
0.75的情况下，TrainValidationSplit将生成训练和测试数据集对，其中75％的数据用于训练，而25％的数据用于验证。像CrossValidator一样，TrainValidationSplit最终使用最佳的ParamMap和整个数据集来拟合估算器。

```scala
scala> import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
scala> import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegression
scala> import org.apache.spark.ml.tuning.{ParamGridBuilder,
TrainValidationSplit}
import org.apache.spark.ml.tuning.{ParamGridBuilder,
TrainValidationSplit}
```

代码 7‑53

准备训练和测试数据：

```scala
scala> val data =
spark.read.format("libsvm").load("/root/data/example/mllib/sample_linear_regression_data.txt")
data: org.apache.spark.sql.DataFrame = [label: double, features:
vector]
scala> data.show
+-------------------+--------------------+
| label| features|
+-------------------+--------------------+
| -9.490009878824548|(10,[0,1,2,3,4,5,...|
| 0.2577820163584905|(10,[0,1,2,3,4,5,...|
| -4.438869807456516|(10,[0,1,2,3,4,5,...|
|-19.782762789614537|(10,[0,1,2,3,4,5,...|
| -7.966593841555266|(10,[0,1,2,3,4,5,...|
| -7.896274316726144|(10,[0,1,2,3,4,5,...|
| -8.464803554195287|(10,[0,1,2,3,4,5,...|
| 2.1214592666251364|(10,[0,1,2,3,4,5,...|
| 1.0720117616524107|(10,[0,1,2,3,4,5,...|
|-13.772441561702871|(10,[0,1,2,3,4,5,...|
| -5.082010756207233|(10,[0,1,2,3,4,5,...|
| 7.887786536531237|(10,[0,1,2,3,4,5,...|
| 14.323146365332388|(10,[0,1,2,3,4,5,...|
|-20.057482615789212|(10,[0,1,2,3,4,5,...|
|-0.8995693247765151|(10,[0,1,2,3,4,5,...|
| -19.16829262296376|(10,[0,1,2,3,4,5,...|
| 5.601801561245534|(10,[0,1,2,3,4,5,...|
|-3.2256352187273354|(10,[0,1,2,3,4,5,...|
| 1.5299675726687754|(10,[0,1,2,3,4,5,...|
| -0.250102447941961|(10,[0,1,2,3,4,5,...|
+-------------------+--------------------+
only showing top 20 rows
scala>val Array(training, test) = data.randomSplit(Array(0.9, 0.1),
seed = 12345)
training: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] =
[label: double, features: vector]
test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] =
[label: double, features: vector]
scala>val lr = new LinearRegression().setMaxIter(10)
lr: org.apache.spark.ml.regression.LinearRegression =
linReg_4653e1bfeb16
```

代码 7‑54

使用ParamGridBuilder构建参数网格：

```scala
scala>val paramGrid = new ParamGridBuilder().addGrid(lr.regParam,
Array(0.1, 0.01)).addGrid(lr.fitIntercept).addGrid(lr
| .elasticNetParam, Array(0.0, 0.5, 1.0)).build()
paramGrid: Array[org.apache.spark.ml.param.ParamMap] =
Array({
linReg_4653e1bfeb16-elasticNetParam: 0.0,
linReg_4653e1bfeb16-fitIntercept: true,
linReg_4653e1bfeb16-regParam: 0.1
}, {
linReg_4653e1bfeb16-elasticNetParam: 0.0,
linReg_4653e1bfeb16-fitIntercept: true,
linReg_4653e1bfeb16-regParam: 0.01
}, {
linReg_4653e1bfeb16-elasticNetParam: 0.0,
linReg_4653e1bfeb16-fitIntercept: false,
linReg_4653e1bfeb16-regParam: 0.1
}, {
linReg_4653e1bfeb16-elasticNetParam: 0.0,
linReg_4653e1bfeb16-fitIntercept: false,
linReg_4653e1bfeb16-regParam: 0.01
}, {
linReg_4653e1bfeb16-elasticNetParam: 0.5,
linReg_4653e1bfeb16-fitIntercept: true,
linReg_4653e1bfeb16-regParam: 0.1
}, {
linReg_4653e1bfeb16-elasticNetParam: 0.5,
linReg_4653e1bfeb16-fitIntercept: true,
linReg_4653e1bfeb16-regPa...
scala>
```

代码 7‑55

TrainValidationSplit会调用所有的参数组合，使用评估器确定最好的模型。其包括线性回归评估器，评估器参数集合和估算器。80%的数据被用来作为训练数据，20%作为测试数据。

```scala
scala>val trainValidationSplit = new
TrainValidationSplit().setEstimator(lr).setEvaluator(new
RegressionEvaluator).setEstimatorParamMaps(paramGrid).setTrainRatio(0.8)
trainValidationSplit: org.apache.spark.ml.tuning.TrainValidationSplit =
tvs_ec6640a05517
```

代码 7‑56

运行TrainValidationSplit，选择最好的参数组合：

```scala
scala> val model = trainValidationSplit.fit(training)
model: org.apache.spark.ml.tuning.TrainValidationSplitModel =
tvs_ec6640a05517
```

代码 7‑57

在测试数据上进行预测：

```scala
scala>model.transform(test).select("features", "label",
"prediction").show()
+--------------------+--------------------+--------------------+
| features| label| prediction|
+--------------------+--------------------+--------------------+
|(10,[0,1,2,3,4,5,...| -23.51088409032297| -1.6659388625179559|
|(10,[0,1,2,3,4,5,...| -21.432387764165806| 0.3400877302576284|
|(10,[0,1,2,3,4,5,...| -12.977848725392104|-0.02335359093652395|
|(10,[0,1,2,3,4,5,...| -11.827072996392571| 2.5642684021108417|
|(10,[0,1,2,3,4,5,...| -10.945919657782932| -0.1631314487734783|
|(10,[0,1,2,3,4,5,...| -10.58331129986813| 2.517790654691453|
|(10,[0,1,2,3,4,5,...| -10.288657252388708| -0.9443474180536754|
|(10,[0,1,2,3,4,5,...| -8.822357870425154| 0.6872889429113783|
|(10,[0,1,2,3,4,5,...| -8.772667465932606| -1.485408580416465|
|(10,[0,1,2,3,4,5,...| -8.605713514762092| 1.110272909026478|
|(10,[0,1,2,3,4,5,...| -6.544633229269576| 3.0454559778611285|
|(10,[0,1,2,3,4,5,...| -5.055293333055445| 0.6441174575094268|
|(10,[0,1,2,3,4,5,...| -5.039628433467326| 0.9572366607107066|
|(10,[0,1,2,3,4,5,...| -4.937258492902948| 0.2292114538379546|
|(10,[0,1,2,3,4,5,...| -3.741044592262687| 3.343205816009816|
|(10,[0,1,2,3,4,5,...| -3.731112242951253| -2.6826413698701064|
|(10,[0,1,2,3,4,5,...| -2.109441044710089| -2.1930034039595445|
|(10,[0,1,2,3,4,5,...| -1.8722161156986976| 0.49547270330052423|
|(10,[0,1,2,3,4,5,...| -1.1009750789589774| -0.9441633113006601|
|(10,[0,1,2,3,4,5,...|-0.48115211266405217| -0.6756196573079968|
+--------------------+--------------------+--------------------+
only showing top 20 rows
```

代码 7‑58

## 7.9 实例分析

本节保留两个案例：一个围绕推荐系统，一个围绕分类问题。需要注意，案例中的部分代码仍使用 `spark.mllib` / RDD 风格 API，目的是帮助读者理解历史写法和存量系统；如果在 Spark 4.x 中新建项目，应优先选择 `spark.ml`、DataFrame 和 Pipeline 的等价实现。

### 7.9.1 预测用户偏好

> 兼容性说明：下面的 ALS 示例使用 RDD 风格写法，适合说明矩阵分解与旧项目代码结构。Spark 4.x 新项目建议优先参考 `org.apache.spark.ml.recommendation.ALS` 的 DataFrame API。

如果今天重新实现这一类推荐任务，通常会先把评分数据整理成包含 `user`、`item`、`rating` 三列的 DataFrame，再把特征清洗、训练与评估放进更一致的工程流程中。保留下面的写法，是为了帮助读者识别历史推荐系统代码最常见的组织方式。

下面继续用电影推荐说明协同过滤。假设 Ted 喜欢电影 A、B、C，Carol 喜欢电影 B、C，那么系统就会尝试根据这些交互记录推断 Bob 可能喜欢什么。Spark 里的 ALS（Alternating Least Squares，交替最小二乘）正是围绕这类“用户 - 物品评分矩阵”构建的经典矩阵分解方法。

ALS 的核心思路，是把原始的稀疏评分矩阵分解成两个更低维的稠密矩阵：一个表示用户在若干潜在因子上的偏好强度，另一个表示项目在同一组潜在因子上的表现。用户和项目在这些潜在因子上的匹配程度，决定了模型给出的预测评分。图例 7‑10 展示了这种“把用户兴趣和项目属性投影到同一潜在空间”的直观理解。

![](media/07_machine_learning/media/image10.jpeg)

图例 7‑10交替最小二乘法

ALS 是一种迭代优化算法。训练时，它会先固定用户因子，更新项目因子；再固定项目因子，更新用户因子，如此交替进行，直到损失函数收敛或达到迭代上限。对于推荐问题，这种方法的优点在于能够较好地处理评分矩阵的稀疏性，并且天然适合并行化，因此长期都是分布式推荐系统里的经典方案。阅读下面的历史示例时，重点可以放在三件事上：评分数据如何组织、训练集与测试集如何拆分，以及模型如何从用户 - 物品交互中学习出潜在偏好结构。

一个典型的机器学习工作流程如图例 7‑15所示。为了进行预测，将执行以下步骤：加载样本数据，并且将数据解析成用于交替最小二乘算法的输入格式；拆分数据分为两个部分，一个用于构建模型和一个用于测试模型；然后运行交替最小二乘算法建立和训练用户的产品矩阵模型；使用训练数据做出预测，并观察结果；然后使用测试数据试验模型。在下面的示例中，从ratings.dat数据集中加载评价数据，每一行包括用户ID、电影ID、评价（从1到5）和时间戳。

![](media/07_machine_learning/media/image11.png)

图例 7‑11机器学习的工作流程

```scala
scala> import org.apache.spark.mllib.recommendation.{ ALS,
MatrixFactorizationModel, Rating }
import org.apache.spark.mllib.recommendation.{ALS,
MatrixFactorizationModel, Rating}
```

在第一步中，加载评价数据到ratingText，加载数据为RDD

```scala
scala> val ratingText = sc.textFile("/data/ratings.dat")
ratingText: org.apache.spark.rdd.RDD[String] = /root/data/ratings.dat
MapPartitionsRDD[1] at textFile at <console>:25
scala> ratingText.take(2)
res3: Array[String] = Array(1::1193::5::978300760,
1::661::3::978302109)
```

代码 7‑59

转换ratingText为RDD，将parseRating函数适用于ratingText的每个元素，并返回一个新的评价对象ratingsRDD，因为将利用这些数据来构建矩阵模型，所以需要缓存，。

```scala
scala> def parseRating(str: String): Rating = {
| val fields = str.split("::")
| Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
| }
parseRating: (str: String)org.apache.spark.mllib.recommendation.Rating
scala> val ratingsRDD = ratingText.map(parseRating).cache()
ratingsRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= MapPartitionsRDD[2] at map at <console>:29
```

该parseRating函数解析评价数据文件的每一行，将其转换为MLlib的Rating类，将以此作为ALS.run方法的输入。下一步，将数据分为两个部分，一个用于训练模型和一个用于测试模型。在这里显示的代码使用了Hold-Out分割数据，80%的数据用来训练，20%的数据用来测试，然后运行交替最小二乘算法建立和训练用户和产品矩阵模型。

```scala
scala> val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)
splits:
Array[org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]]
= Array(MapPartitionsRDD[3] at randomSplit at <console>:31,
MapPartitionsRDD[4] at randomSplit at <console>:31)
scala> val trainingRatingsRDD = splits(0).cache()
trainingRatingsRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= MapPartitionsRDD[3] at randomSplit at <console>:31
scala> val testRatingsRDD = splits(1).cache()
testRatingsRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= MapPartitionsRDD[4] at randomSplit at <console>:31
```

代码 7‑60

使用ALS.train()调用交替最小二乘算法构建一个新的用户和产品矩阵模型，使用的参数为(rank=20,
iterations=10)，交替最小二乘中最重要的超参数为：

maxIter：要运行的最大迭代次数（默认为10）

rank：模型中潜在因子的数量（默认为10）

regParam：交替最小二乘中的正则化参数（默认为1.0）

```scala
scala> val model = ALS.train(trainingRatingsRDD, 10, 20)
model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel =
<org.apache.spark.mllib.recommendation.MatrixFactorizationModel@6f2f9ba4>
```

代码 7‑61

已经训练了一个模型model，想要得到测试数据的电影预测评价。首先用testRatingsRDD创建新的RDD，其中包括测试用户ID和影片ID，没有任何评价。

```scala
scala> val testUserProductRDD = testRatingsRDD.map {
| case Rating(user, product, rating) => (user, product)
| }
testUserProductRDD: org.apache.spark.rdd.RDD[(Int, Int)] =
MapPartitionsRDD[392] at map at <console>:35
```

代码 7‑62

然后，调用model.predict()方法，输入新的testUserProductRDD，以获取每个测试用户ID和影片ID对应的预测评级。

```scala
scala> val predictionsForTestRDD = model.predict(testUserProductRDD)
prdictionsForTestRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= MapPartitionsRDD[401] at map at MatrixFactorizationModel.scala:140
```

代码 7‑63

接下来对比测试评级的预测结果。在这里，创建用户ID，ID电影收视率键值对，这样就可以比较测试评级的预测评级。

```scala
scala>val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map
{
| case Rating(user, product, rating) => ((user, product), rating)
|}
predictionsKeyedByUserProductRDD: org.apache.spark.rdd.RDD[((Int, Int),
Double)] = MapPartitionsRDD[402] at map at <console>:43
```

为比较准备测试数据：

```scala
scala>val testKeyedByUserProductRDD = testRatingsRDD.map {
| case Rating(user, product, rating) => ((user, product), rating)
|}
testKeyedByUserProductRDD: org.apache.spark.rdd.RDD[((Int, Int),
Double)] = MapPartitionsRDD[403] at map at <console>:35
```

将预测结果与测试数据结合：

```scala
scala>val testAndPredictionsJoinedRDD =
testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)
testAndPredictionsJoinedRDD: org.apache.spark.rdd.RDD[((Int, Int),
(Double, Double))] = MapPartitionsRDD[406] at join at <console>:47
scala> testAndPredictionsJoinedRDD.take(10).mkString("\n")
res5: String =
((233,1265),(4.0,4.460843005222394))
((1308,1042),(4.0,3.1014835510132865))
((5686,2967),(1.0,2.103888739955093))
((4447,1100),(1.0,1.4102976702457886))
((2131,512),(2.0,3.111590795245745))
((3093,955),(5.0,4.443509525728583))
((2109,3928),(2.0,4.038725293203363))
((3242,1690),(1.0,3.2002316015918826))
((4270,3616),(3.0,3.55205292499131))
((3650,2701),(3.0,1.9368462777917386))
```

代码 7‑64

通过比较评分的预测，将预测评级为高，而实际评分较低作为误报。下面代码中测试评级\<= 1，而预测的评级是\> = 4为误报。

```scala
scala> val falsePositives = testAndPredictionsJoinedRDD.filter { case
((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >=
4) }
falsePositives: org.apache.spark.rdd.RDD[((Int, Int), (Double,
Double))] = MapPartitionsRDD[409] at filter at <console>:49
scala> falsePositives.take(2)
res6: Array[((Int, Int), (Double, Double))] =
Array(((1038,3545),(1.0,4.64155564571005)),
((5878,2875),(1.0,4.1482372423348295)))
```

代码 7‑65

该模型也可以通过平均绝对误差计算实际测试评价和预测评价之间的绝对误差的平均值来判断模型的训练效果。

```scala
scala> val meanAbsoluteError = testAndPredictionsJoinedRDD.map {
| case ((user, product), (testRating, predRating)) =>
| val err = (testRating - predRating)
| Math.abs(err)
| }.mean()
meanAbsoluteError: Double = 0.6895645970856591
```

代码 7‑66

在下面的代码中，创建一个ID为0的新用户电影评价newRatingsRDD，然后与ratingsRDD合并成unionRatingsRDD，然后输出到ALS返回一个新的推荐模型model。现在，可以通过调用model.recommendProducts()来获得建议，输入参数用户ID=
0和建议项目的数量=5。

```scala
scala> val
newRatingsRDD=sc.parallelize(Array(Rating(0,260,4),Rating(0,1,3)))
newRatingsRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= ParallelCollectionRDD[413] at parallelize at <console>:25
scala> val unionRatingsRDD = ratingsRDD.union(newRatingsRDD)
unionRatingsRDD:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]
= UnionRDD[414] at union at <console>:33
scala> val model = new
ALS().setRank(20).setIterations(10).run(unionRatingsRDD)
model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel =
org.apache.spark.mllib.recommendation.MatrixFactorizationModel@5859f307
scala> val topRecsForUser = model.recommendProducts(0, 5)
topRecsForUser: Array[org.apache.spark.mllib.recommendation.Rating] =
Array(Rating(0,1651,4.026343140072196),
Rating(0,260,3.9826456201257963), Rating(0,2323,3.955009095763199),
Rating(0,1196,3.860915147369469), Rating(0,1198,3.6932753705094252))
```

代码 7‑67

### 7.9.2 分析飞行延误

> 兼容性说明：下面的决策树示例使用 `spark.mllib` 的 `LabeledPoint` 与 RDD 风格 API，用于解释历史代码和算法参数。新项目建议优先使用 `spark.ml.classification.DecisionTreeClassifier` 配合 DataFrame/Pipeline。

如果按现代主线重做这个案例，通常会先把 CSV 读成 DataFrame，再用 `StringIndexer`、`VectorAssembler`、`DecisionTreeClassifier` 与 `BinaryClassificationEvaluator` 组织成一条 Pipeline。下面保留旧写法，主要是为了帮助你在存量项目中读懂特征准备、训练/测试拆分和模型评估这些基本步骤。

这实例的数据来自与航班信息，对于每次航班都有以下信息：

| 字段名                     | 字段描述        | 例子     |
| ----------------------- | ----------- | ------ |
| dOfM(String)            | 一个月中的某天     | 1      |
| dOfW (String)           | 星期几         | 4      |
| carrier (String)        | 运营商代码       | AA     |
| tailNum (String)        | 飞机的唯一标识符-尾号 | N787AA |
| flnum(Int)              | 航班号         | 21     |
| org\_id(String)         | 始发机场编号      | 12478  |
| origin(String)          | 始发机场代码      | JFK    |
| dest\_id (String)       | 目的地机场编号     | 12892  |
| dest (String)           | 目的地机场代码     | LAX    |
| crsdeptime(Double)      | 预定出发时间      | 900    |
| deptime (Double)        | 实际出发时间      | 855    |
| depdelaymins (Double)   | 出发延迟分钟      | 0      |
| crsarrtime (Double)     | 预定到达时间      | 1230   |
| arrtime (Double)        | 实际到达时间      | 1237   |
| arrdelaymins (Double)   | 到达延迟分钟      | 7      |
| crselapsedtime (Double) | 经过时间        | 390    |
| dist (Int)              | 距离          | 2475   |

表格 7‑3数据描述

这个任务是通过构建决策树预测飞机是否晚点，如果延迟40分钟则delayed为Yes，否则为No。训练决策树用到的特征字段包括dofM、dofW、crsDepTime、crsArrTime、carrier、crselapsedtime、origin、dest，标记字段为delayed。首先，从csv文件加载和解析数据。

导入需要的软件包：

```scala
scala> import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LabeledPoint
scala> import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
scala> import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.DecisionTree
scala> import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel
scala> import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.util.MLUtils
```

代码 7‑68

示例中，每个航班是一个项目，使用case class定义与csv数据文件中的一行相对应的Flight模式：

```scala
scala> case class Flight(dofM: String, dofW: String, carrier: String,
tailnum: String, flnum: Int, org_id: String, origin: String, dest_id:
String, dest: String, crsdeptime: Double, deptime: Double, depdelaymins:
Double, crsarrtime: Double, arrtime: Double, arrdelay: Double,
crselapsedtime: Double, dist: Int)
defined class Flight
```

代码 7‑69

定义函数将数据文件的一行解析到Flight类：

// function to parse input into Flight class

```scala
scala> def parseFlight(str: String): Flight = {
| val line = str.split(",")
| Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5),
line(6), line(7), line(8), line(9).toDouble, line(10).toDouble,
line(11).toDouble, line(12).toDouble, line(13).toDouble,
line(14).toDouble, line(15).toDouble, line(16).toInt)
| }
parseFlight: (str: String)Flight
```

代码 7‑70

从CSV文件加载数据然后进行转换和缓存，调用first()返回RDD中的第一个元素。

```scala
scala> val textRDD = sc.textFile("/root/data/rita2014jan.csv")
textRDD: org.apache.spark.rdd.RDD[String] = /root/data/rita2014jan.csv
MapPartitionsRDD[1] at textFile at <console>:29
scala> val flightsRDD = textRDD.map(parseFlight).cache()
flightsRDD: org.apache.spark.rdd.RDD[Flight] = MapPartitionsRDD[2]
at map at <console>:35
scala> flightsRDD.first()
res0: Flight =
Flight(1,3,AA,N338AA,1,12478,JFK,12892,LAX,900.0,914.0,14.0,1225.0,1238.0,13.0,385.0,2475)
```

代码 7‑71

要建立分类器模型，首先提取最有助于分类的特征，定义二元类别标签：Yes为延迟和No为不延迟。如果延迟超过40分钟，飞行被认为是延迟的。每个项目的特征和标签包括dofM、dofW、crsdeptime、crsarrtime、carrier、crselapsedtime、origin、dest、delayed。下面将非数字特征转换为数值，例如运营商AA是数字6，始发机场ATL是273。

创建运营商、始发地和目的地：

```scala
scala> var carrierMap: Map[String, Int] = Map()
carrierMap: Map[String,Int] = Map()
scala> var index: Int = 0
index: Int = 0
scala> flightsRDD.map(flight =>
flight.carrier).distinct.collect.foreach(x => { carrierMap += (x ->
index); index += 1 })
scala> carrierMap.toString
res2: String = Map(DL -> 5, F9 -> 10, US -> 9, OO -> 2, B6 -> 0, AA
-> 6, EV -> 12, FL -> 1, UA -> 4, MQ -> 8, WN -> 13, AS -> 3, VX
-> 7, HA -> 11)
scala> var originMap: Map[String, Int] = Map()
originMap: Map[String,Int] = Map()
scala> var index1: Int = 0
index1: Int = 0
scala> flightsRDD.map(flight =>
flight.origin).distinct.collect.foreach(x => { originMap += (x ->
index1); index1 += 1 })
scala> originMap.toString
res4: String = Map(ROW -> 23, OAJ -> 144, GCC -> 232, SYR -> 80, TYR
-> 162, TUL -> 180, STL -> 203, IDA -> 61, ICT -> 62, MQT -> 37,
SWF -> 118, EKO -> 148, JFK -> 216, LGB -> 241, ISP -> 101, ART ->
288, ORD -> 234, STX -> 170, EGE -> 159, LWS -> 132, TWF -> 229,
LAS -> 44, BET -> 286, GSP -> 117, DAY -> 123, KOA -> 252, BUR ->
292, DRO -> 276, PVD -> 31, BRD -> 77, SPS -> 1, CLD -> 184, SGF
-> 86, CDV -> 222, STT -> 214, OTZ -> 279, AVL -> 199, BOI -> 12,
PSP -> 150, SAF -> 40, FWA -> 146, MHT -> 186, SBN -> 206, RDM ->
182, PSG -> 59, LAX -> 294, BQN -> 293, HSV -> 257, RIC -> 6, BTM
-> 217, LSE -> 33, FCA -> 55, JAC -> 110, ATL -> 273, CHA -> 112,
BQK -> 96, MIA -> 176, GUC -> 282, SBP -> 163, BFL -> 74, DHN ->
51, FLG -> 155, BRO -> 274, LAN -> 192, FSM -> 15, RAP -> 285, EAU
-> 1...
scala> var destMap: Map[String, Int] = Map()
destMap: Map[String,Int] = Map()
scala> var index2: Int = 0
index2: Int = 0
scala> flightsRDD.map(flight =>
flight.dest).distinct.collect.foreach(x => { destMap += (x -> index2);
index2 += 1 })
scala> destMap.toString
res13: String = Map(ROW -> 23, OAJ -> 144, GCC -> 232, SYR -> 80,
TYR -> 162, TUL -> 180, STL -> 203, IDA -> 61, ICT -> 62, MQT ->
37, SWF -> 118, EKO -> 148, JFK -> 216, LGB -> 241, ISP -> 101, ART
-> 288, ORD -> 234, STX -> 170, EGE -> 159, LWS -> 132, TWF ->
229, LAS -> 44, BET -> 286, GSP -> 117, DAY -> 123, KOA -> 252, BUR
-> 292, DRO -> 276, PVD -> 31, BRD -> 77, SPS -> 1, CLD -> 184,
SGF -> 86, CDV -> 222, STT -> 214, OTZ -> 279, AVL -> 199, BOI ->
12, PSP -> 150, SAF -> 40, FWA -> 146, MHT -> 186, SBN -> 206, RDM
-> 182, PSG -> 59, LAX -> 294, BQN -> 293, HSV -> 257, RIC -> 6,
BTM -> 217, LSE -> 33, FCA -> 55, JAC -> 110, ATL -> 273, CHA ->
112, BQK -> 96, MIA -> 176, GUC -> 282, SBP -> 163, BFL -> 74, DHN
-> 51, FLG -> 155, BRO -> 274, LAN -> 192, FSM -> 15, RAP -> 285,
EAU -> ...
```

代码 7‑72

定义特征向量：

```scala
scala> val mlprep = flightsRDD.map(flight => {
| val monthday = flight.dofM.toInt - 1 // category
| val weekday = flight.dofW.toInt - 1 // category
| val crsdeptime1 = flight.crsdeptime.toInt
| val crsarrtime1 = flight.crsarrtime.toInt
| val carrier1 = carrierMap(flight.carrier) // category
| val crselapsedtime1 = flight.crselapsedtime.toDouble
| val origin1 = originMap(flight.origin) // category
| val dest1 = destMap(flight.dest) // category
| val delayed = if (flight.depdelaymins.toDouble > 40) 1.0 else 0.0
| Array(delayed.toDouble, monthday.toDouble, weekday.toDouble,
crsdeptime1.toDouble, crsarrtime1.toDouble, carrier1.toDouble,
crselapsedtime1.toDouble, origin1.toDouble, dest1.toDouble)
| })
mlprep: org.apache.spark.rdd.RDD[Array[Double]] =
MapPartitionsRDD[28] at map at <console>:43
scala> mlprep.take(1)
res14: Array[Array[Double]] = Array(Array(0.0, 0.0, 2.0, 900.0,
1225.0, 6.0, 385.0, 216.0, 294.0))
```

代码 7‑73

从包含RDD的特征数组中，创建包含LabeledPoints数组的[RDD](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/mllib/regression/LabeledPoint.html)，其中定义了数据点的特征向量和标签：

//Making LabeledPoint of features - this is the training data for the
model

```scala
scala> val mldata = mlprep.map(x => LabeledPoint(x(0),
Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))
mldata:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= MapPartitionsRDD[29] at map at <console>:45
scala> mldata.take(1)
res15: Array[org.apache.spark.mllib.regression.LabeledPoint] =
Array((0.0,[0.0,2.0,900.0,1225.0,6.0,385.0,216.0,294.0]))
```

代码 7‑74

接下来数据被拆分，以获得延迟和不延迟航班的合适百分比。然后将其分为训练数据集和测试数据集。mldata0是85%的非延迟，mldata1是100%的延迟，将mldata0与mldata1合并为mldata2：

```scala
scala> val mldata0 = mldata.filter(x => x.label ==
0).randomSplit(Array(0.85, 0.15))(1)
mldata0:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= MapPartitionsRDD[32] at randomSplit at <console>:47
scala> val mldata1 = mldata.filter(x => x.label != 0)
mldata1:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= MapPartitionsRDD[33] at filter at <console>:47
scala> val mldata2 = mldata0 ++ mldata1
mldata2:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= UnionRDD[34] at $plus$plus at <console>:51
```

分割mldata2为训练和测试数据集：

```scala
scala> val splits = mldata2.randomSplit(Array(0.7, 0.3))
splits:
Array[org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]]
= Array(MapPartitionsRDD[35] at randomSplit at <console>:53,
MapPartitionsRDD[36] at randomSplit at <console>:53)
scala> val (trainingData, testData) = (splits(0), splits(1))
trainingData:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= MapPartitionsRDD[35] at randomSplit at <console>:53
testData:
org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
= MapPartitionsRDD[36] at randomSplit at <console>:53
scala> testData.take(1)
res16: Array[org.apache.spark.mllib.regression.LabeledPoint] =
Array((0.0,[23.0,4.0,900.0,1225.0,6.0,385.0,216.0,294.0]))
```

代码 7‑75

接下来，准备[决策树所需参数](https://spark.apache.org/docs/latest/mllib-decision-tree.html)的值：

categoricalFeaturesInfo：指定哪些特征是分类的，以及每个特征可以采用多少种分类值。这是从特征索引到该特征的类别数量的映射。第一个分类特征categoricalFeaturesInfo
= (0 -\> 31)代表月中的日期，具有31个类别（值从0到31）。第二个categoricalFeaturesInfo = (1 -\>
7)表示星期几，并指定特征索引1到7个类别。运营商分类特征是索引4，值可以从0到不同运营商的数量；

maxDepth：是指树的最大深度；

maxBins：是指离散化连续特征时使用的数据块数；

impurity：是指在节点处的标签均匀性的不纯度测量。

通过将输入特征与这些特征相关联的标记输出之间进行关联来训练该模型。使用DecisionTree.trainClassifier()方法训练模型，该方法返回DecisionTreeModel。

为dofM、dofW、carrier、origin、dest设置范围：

```scala
scala> var categoricalFeaturesInfo = Map[Int, Int]()
categoricalFeaturesInfo: scala.collection.immutable.Map[Int,Int] =
Map()
scala> categoricalFeaturesInfo += (0 -> 31)
scala> categoricalFeaturesInfo += (1 -> 7)
scala> categoricalFeaturesInfo += (4 -> carrierMap.size)
scala> categoricalFeaturesInfo += (6 -> originMap.size)
scala> categoricalFeaturesInfo += (7 -> destMap.size)
scala> val numClasses = 2
numClasses: Int = 2
```

代码 7‑76

定义其他参数：

```scala
scala> val impurity = "gini"
impurity: String = gini
scala> val maxDepth = 9
maxDepth: Int = 9
scala> val maxBins = 7000
maxBins: Int = 7000
scala> val model = DecisionTree.trainClassifier(trainingData,
numClasses, categoricalFeaturesInfo,
| impurity, maxDepth, maxBins)
model: org.apache.spark.mllib.tree.model.DecisionTreeModel =
DecisionTreeModel classifier of depth 9 with 581 nodes
scala> model.toDebugString
res22: String =
"DecisionTreeModel classifier of depth 9 with 581 nodes
If (feature 0 in
{10.0,24.0,25.0,14.0,20.0,21.0,13.0,17.0,22.0,27.0,12.0,18.0,16.0,11.0,26.0,23.0,30.0,19.0,15.0})
If (feature 4 in
{0.0,5.0,10.0,1.0,6.0,9.0,13.0,2.0,7.0,3.0,11.0,8.0,4.0})
If (feature 2 <= 1151.0)
If (feature 0 in
{24.0,25.0,14.0,13.0,17.0,12.0,18.0,16.0,11.0,19.0,15.0})
If (feature 6 in
{88.0,247.0,288.0,196.0,46.0,152.0,228.0,29.0,179.0,211.0,106.0,238.0,121.0,61.0,132.0,133.0,1.0,248.0,201.0,102.0,260.0,38.0,297.0,165.0,252.0,197.0,156.0,109.0,256.0,212.0,129.0,237.0,2.0,266.0,148.0,264.0,279.0,118.0,281.0,54.0,181.0,219.0,76.0,7.0,245.0,39.0,98.0,208.0,103.0,66.0,251.0,241.0,162.0,112.0,194.0,50.0,67.0,199.0,182.0,154.0,143.0,87.0,158.0,186.0,55.0,119.0,246.0,190.0,19.0,239....
```

代码 7‑77

Model.toDebugString打印出决策树。接下来，使用测试数据来获得预测，然后将航班延迟的预测与实际航班延迟进行比较。错误的预测率是错误预测除以测试数据值的总数，约为31％。

在测试数据上进行评估，并且计算误差：

```scala
scala> val labelAndPreds = testData.map { point =>
| val prediction = model.predict(point.features)
| (point.label, prediction)
| }
labelAndPreds: org.apache.spark.rdd.RDD[(Double, Double)] =
MapPartitionsRDD[75] at map at <console>:43
scala> labelAndPreds.take(3)
res24: Array[(Double, Double)] = Array((0.0,0.0), (0.0,0.0),
(0.0,1.0))
scala> val wrongPrediction =(labelAndPreds.filter{
| case (label, prediction) => ( label !=prediction)
| })
wrongPrediction: org.apache.spark.rdd.RDD[(Double, Double)] =
MapPartitionsRDD[76] at filter at <console>:45
scala> wrongPrediction.count()
res25: Long = 11109
scala> val ratioWrong=wrongPrediction.count().toDouble/testData.count()
ratioWrong: Double = 0.31526520418877885
```

代码 7‑78

## 7.10 小结

在 Spark 4.x 中，机器学习章的主线应当理解为：以 DataFrame 组织数据，以 `spark.ml` 构建特征工程和模型，以 Pipeline 保证训练、验证与推理过程一致。本章同时保留了部分 `spark.mllib` / RDD 风格内容，用于帮助读者读懂旧代码、理解 API 演进以及掌握若干底层数据类型。学完本章后，读者应能区分“现代主线”和“历史兼容”两套写法，并据此选择合适的工程方案。

