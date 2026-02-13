# 算法汇总

## 决策树和集成

决策树和集成树是分类和回归的机器学习任务流行的方法。决策树广泛使用，因为它很容易解释，处理类别特征，延伸到多类分类设置，不需要的功能扩展，并能够捕捉到非线性和特征的交互。集成树算法，如随机森林和提高是表现最佳的分类和回归任务之一。

### 决策树

在spark.ml实现支持决策树的二进制和多分类和回归，同时使用连续和分类功能。按行实现分区的数据，允许使用数百万甚至数十亿的实例进行分布式训练。这个API和MLlib决策树之间的主要区别是：

（1）对于ML管道支持

（2）决策树被分离用于分类与回归

（3）使用DataFrame的元数据来区分连续和分类特征

决策树管道API提供了比原来的API更多的功能。特别地对于分类，用户可以获取每个类（又名类条件概率）的预测概率；对于回归，用户可以得到预测的偏置样本方差。Spark在这里列出输入和输出（预测）列类型。所有输出列都是可选的；排除一个输出列，设置其相应参数为空字符串。

  - 输入列

| 参数名称        | 类型     | 默认         | 描述   |
| ----------- | ------ | ---------- | ---- |
| labelCol    | Double | "label"    | 标签预测 |
| featuresCol | Vector | "features" | 特征向量 |

表格 4‑1输入列

  - 输出列

| 参数名称             | 类型     | 默认              | 描述                                  | 注意   |
| ---------------- | ------ | --------------- | ----------------------------------- | ---- |
| predictionCol    | Double | "prediction"    | 预测标签                                |      |
| rawPredictionCol | Vector | "rawPrediction" | 长度为＃个类的向量，在进行预测的树节点上有训练实例标签的数量      | 只有分类 |
| probabilityCol   | Vector | "probability"   | 长度为＃个类的向量，等于归一化为多项式分布的rawPrediction | 只有分类 |
| varianceCol      | Double |                 | 预测的偏差的样本方差                          | 只有回归 |

表格 4‑2输出列

#### 基于RDD

决策树是一种贪婪算法，执行特征空间的递归二进制分区。该树为每个最底部（叶子）的分区预测相同的标签,通过从一组可能的分割中选择最佳分割来贪婪地选择每个分区，以使树节点的信息增益最大化。换句话说，在每个树节点处选择的拆分是从集合$$\underset{s}{\text{argmax}}IG(D,s)$$中选择的,其中$$IG(D,s)$$是将分割s应用于数据集D时的信息增益。

| 不纯度  | 任务 | 公式                                                 | 描述                                                                                        |
| ---- | -- | -------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| 基尼指数 | 分类 | $$\sum_{i = 1}^{C}{}f_{i}(1 - f_{i})$$             | $$f_{i}$$是标签$$i$$ 在一个节点的频率，$$\text{C}$$是唯一标签的个数。                                        |
| 信息熵  | 分类 | $$\sum_{i = 1}^{C}{} - f_{i}\log(f_{i})$$          | $$f_{i}$$是标签$$i$$ 在一个节点的频率，$$\text{C}$$是唯一标签的个数。                                        |
| 方差   | 回归 | $$\frac{1}{N}\sum_{i = 1}^{N}{}(y_{i} - \mu)^{2}$$ | $$y_{i}$$是一个实例的标签, $$N$$ 是实例的个数， $$\mu$$ 是由 $$\frac{1}{N}\sum_{i = 1}^{N}{}y_{i}$$给出的平均值。 |

节点的不纯度是节点上标记均质性的量度，当前的实现提供了两种用于分类的不纯度度量：基尼指数和信息熵；和一种用于回归的不纯度度量：方差。

信息增益是父节点杂质与两个子节点杂质的加权和之间的差。
假设一个分割$$s$$将大小为$$N$$的数据集$$D$$分为两个大小分别为$$N_{\text{left}}$$和$$N_{\text{right}}$$的数据集$$D_{\text{left}}$$和$$D_{\text{right}}$$，则信息增益为：

$$
IG(D,s) = Impurity(D) - \frac{N_{\text{left}}}{N}Impurity(D_{\text{left}}) - \frac{N_{\text{right}}}{N}Impurity(D_{\text{right}})
$$

对于单机实施中的小型数据集，每个连续特征的分割候选通常是特征的唯一值。一些实现是对特征值进行排序，然后将排序后的唯一值用作拆分候选，以便更快地进行树计算。对于大型分布式数据集，对特征值进行排序非常昂贵。此实现通过对数据的采样部分执行分位数计算来获取一组近似的拆分候选集。有序拆分将创建箱，可以使用maxBins参数指定此类箱的最大数量。请注意，箱的数量不能大于实例的数量，这种情况很少见，因为maxBins默认值为32。如果不满足条件，则树算法会自动减少箱的数量。

对于具有$$M$$个可能值（类别）的分类特征，可以提出$$2^{M - 1} - 1$$分割候选。对于二进制（0/1）分类和回归，我们可以通过按平均标签对分类特征值进行排序来减少拆分候选的数量。例如，对于具有一个分类特征为(A,B,C)的二元分类问题，标签1的相应比例为(0.2,0.6,0.4)，则分类特征按(A,C,B)排序，两个拆分的候选对象为(A|C,B)和(A,C|B)，“|”表示分割。

在多类分类中，将尽可能使用所有$$2^{M - 1} - 1$$可能的拆分，当$$2^{M - 1} - 1$$大于参数maxBins时，我们使用（启发式）方法类似于用于二元分类和回归的方法，该$$M$$分类的特征值由不纯度排序，并将得到$$M - 1$$分割候选项。

当满足以下条件之一时，递归树构造将在节点处停止：

（1）节点深度等于maxDepth训练参数。

（2）没有分割的候选项会导致信息增益大于minInfoGain。

（3）没有分割的候选项产生每个都至少具有minInstancesPerNode训练实例的子节点。

通过讨论各种参数，我们包括一些使用决策树的准则。以下按重要性从高到低的顺序列出了这些参数。新用户应主要考虑“问题指定参数”部分和maxDepth参数。

  - 问题指定参数

这些参数描述了我们要解决的问题和数据集，应该指定它们而不需要调整。

algo：决策树的类型，分类或回归。

numClasses：类别数，仅适用分类。

categoricalFeaturesInfo：指定哪些是类别特征，以及每个特征可以采用多少类别值。这是从特征索引到特征类别数的映射，该映射中未包含的所有特征均视为连续特征，例如Map(0
-\> 2, 4 -\> 10)指定特征0为二进制（采用值0或1），并且特征4具有10个类别{0, 1, ...,
9}。请注意，特征索引是从0开始，特征0和4分别是实例的特征向量中第1和第5个元素。请注意，我们不必指定categoricalFeaturesInfo。该算法仍将运行，并且可以获得合理的结果，但是如果正确指定分类特征，则性能应该会更好。

  - 停止条件

这些参数确定树何时停止构建，添加新节点。调整这些参数时，请小心验证保留的测试数据，以免过度拟合。

maxDepth：树的最大深度，较深的树更具表现力，可能允许更高的准确性，但它们的训练成本也更高，并且更可能过拟合。

minInstancesPerNode：要进一步拆分节点，其每个子节点必须至少收到此数量的训练实例，这通常与RandomForest一起使用，因为它们通常比单独的树更深入地训练。

minInfoGain：对于要进一步拆分的节点，设置拆分后信息增益必须至少改善的值。

  - 可调参数

这些参数可以调整，调整时请小心验证保留的测试数据，以避免过度拟合。

maxBins：离散化连续特征时使用的桶数。增加maxBins允许算法考虑更多拆分候选并做出细粒度的拆分决策，但是也增加了计算和通信。请注意，该maxBins参数必须至少是任何分类特征的最大分类数M。

maxMemoryInMB：用于收集足够统计信息的内存量。保守地将默认值选择为256
MB，以使决策算法可以在大多数情况下使用。增加maxMemoryInMB（如果有可用的内存）可以通过较少的数据传递来加快训练速度，但是由于每次迭代的通信量可以与maxMemoryInMB成正比，因此可能随着maxMemoryInMB增长而产生的加快效果递减。为了更快地进行处理，决策树算法收集有关要拆分的节点组的统计信息（而不是一次分配1个节点）。一组中可以处理的节点数由内存需求（取决于每个特征）决定，maxMemoryInMB参数以兆字节为单位指定每个工作节点可用于这些统计信息的内存限制。

subsamplingRate：用于学习决策树的训练数据的比例，此参数与训练集成树最相关，例如使用RandomForest和GradientBoostedTrees，在此情况下可以对原始数据进行二次采样。对于训练单个决策树，此参数的用处不大，因为训练实例的数量通常不是主要约束。

impurity：用于在候选分割之间进行选择的不纯度测量，此测量必须与algo参数匹配。

  - 缓存和检查点

MLlib
1.2添加了一些参数，可以扩展到更大（更深）的树和树组合，当maxDepth被设定为大值时，这些参数对于开启节点ID缓存和检查点是有用的，当numTrees设置为大值时，这些参数对于RandomForest也很有用。

useNodeIdCache：如果将其设置为true，则算法将避免在每次迭代时将当前模型（一个或多个树）传递给执行程序，这对于深树（加快工作节点的计算速度）和大型随机森林（减少每次迭代的通信量）很有用。默认情况下，该算法将当前模型传达给执行者，以便执行者可以将训练实例与树节点进行匹配，启用此设置后算法将改为缓存此信息。节点ID缓存会生成一系列RDD（每个迭代1个），这种较长的谱系可能会导致性能问题，但是通过检查点中间的RDD可以缓解这些问题。请注意，检查点仅在useNodeIdCache设置为true
时适用。

checkpointDir：用于检查点节点ID缓存RDD的目录。

checkpointInterval：用于检查点节点ID缓存RDD的频率。设置得太低将导致写入HDFS的额外开销；如果执行程序失败并且需要重新计算RDD，则将其设置得过高会导致问题。

计算的成本与训练实例的数量、特征数量和maxBins参数具有近似线性的比例，通信量与特征数量和maxBins具有近似线性的比例。实现的算法读取稀疏和密集数据，但是它并未针对稀疏输入进行优化。

下面的示例演示如何加载
LIBSVM数据文件，将其解析为RDD，LabeledPoint然后使用决策树（以Gini杂质作为杂质度量且最大树深度为5）进行分类。计算测试误差以测量算法准确性。

scala\> import org.apache.spark.mllib.tree.DecisionTree

import org.apache.spark.mllib.tree.DecisionTree

scala\> import org.apache.spark.mllib.tree.model.DecisionTreeModel

import org.apache.spark.mllib.tree.model.DecisionTreeModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[6\] at map at MLUtils.scala:86

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[7\] at randomSplit at \<console\>:28,
MapPartitionsRDD\[8\] at randomSplit at \<console\>:28)

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[7\] at randomSplit at \<console\>:28

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[8\] at randomSplit at \<console\>:28

scala\> val numClasses = 2

numClasses: Int = 2

scala\> val categoricalFeaturesInfo = Map\[Int, Int\]()

categoricalFeaturesInfo: scala.collection.immutable.Map\[Int,Int\] =
Map()

scala\> val impurity = "gini"

impurity: String = gini

scala\> val maxDepth = 5

maxDepth: Int = 5

scala\> val maxBins = 32

maxBins: Int = 32

scala\> val model = DecisionTree.trainClassifier(trainingData,
numClasses, categoricalFeaturesInfo,

| impurity, maxDepth, maxBins)

model: org.apache.spark.mllib.tree.model.DecisionTreeModel =
DecisionTreeModel classifier of depth 2 with 5 nodes

scala\> val labelAndPreds = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelAndPreds: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[24\] at map at \<console\>:30

scala\> val testErr = labelAndPreds.filter(r =\> r.\_1 \!=
r.\_2).count().toDouble / testData.count()

testErr: Double = 0.0

scala\> println(s"Test Error = $testErr")

Test Error = 0.0

scala\> println(s"Learned classification tree model:\\n
${model.toDebugString}")

Learned classification tree model:

DecisionTreeModel classifier of depth 2 with 5 nodes

If (feature 434 \<= 70.5)

If (feature 100 \<= 193.5)

Predict: 0.0

Else (feature 100 \> 193.5)

Predict: 1.0

Else (feature 434 \> 70.5)

Predict: 1.0

scala\> model.save(sc, "/tmp/myDecisionTreeClassificationModel")

scala\> val sameModel = DecisionTreeModel.load(sc,
"/tmp/myDecisionTreeClassificationModel")

sameModel: org.apache.spark.mllib.tree.model.DecisionTreeModel =
DecisionTreeModel classifier of depth 2 with 5 nodes

下面的示例演示如何加载
LIBSVM数据文件，将其解析为RDD，LabeledPoint然后使用决策树执行回归，并以方差作为杂质度量，最大树深度为5。计算均方误差（MSE）的方法是最后评估
拟合优度。

scala\> import org.apache.spark.mllib.tree.DecisionTree

import org.apache.spark.mllib.tree.DecisionTree

scala\> import org.apache.spark.mllib.tree.model.DecisionTreeModel

import org.apache.spark.mllib.tree.model.DecisionTreeModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[53\] at map at MLUtils.scala:86

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[54\] at randomSplit at \<console\>:32,
MapPartitionsRDD\[55\] at randomSplit at \<console\>:32)

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[54\] at randomSplit at \<console\>:32

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[55\] at randomSplit at \<console\>:32

scala\> val categoricalFeaturesInfo = Map\[Int, Int\]()

categoricalFeaturesInfo: scala.collection.immutable.Map\[Int,Int\] =
Map()

scala\> val impurity = "variance"

impurity: String = variance

scala\> val maxDepth = 5

maxDepth: Int = 5

scala\> val maxBins = 32

maxBins: Int = 32

scala\> val model = DecisionTree.trainRegressor(trainingData,
categoricalFeaturesInfo, impurity,

| maxDepth, maxBins)

model: org.apache.spark.mllib.tree.model.DecisionTreeModel =
DecisionTreeModel regressor of depth 1 with 3 nodes

scala\> val labelsAndPredictions = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelsAndPredictions: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[68\] at map at \<console\>:34

scala\> val testMSE = labelsAndPredictions.map{ case (v, p) =\>
math.pow(v - p, 2) }.mean()

testMSE: Double = 0.03846153846153847

scala\> println(s"Test Mean Squared Error = $testMSE")

Test Mean Squared Error = 0.03846153846153847

scala\> println(s"Learned regression tree model:\\n
${model.toDebugString}")

Learned regression tree model:

DecisionTreeModel regressor of depth 1 with 3 nodes

If (feature 434 \<= 70.5)

Predict: 0.0

Else (feature 434 \> 70.5)

Predict: 1.0

scala\> model.save(sc, "/tmp/myDecisionTreeRegressionModel")

scala\> val sameModel = DecisionTreeModel.load(sc,
"/tmp/myDecisionTreeRegressionModel")

sameModel: org.apache.spark.mllib.tree.model.DecisionTreeModel =
DecisionTreeModel regressor of depth 1 with 3 nodes

### 集成树

DataFrame API支持两个主要的树集成算法：随机森林（Random Forests）和梯度提升树（Gradient-Boosted
Trees，GBTs），两者都采用spark.ml决策树作为其基本模型。DataFrame API和原来MLlib集成API之间的主要区别是：

（1）对于DataFrame和ML管道支持

（2）分类与回归的分离

（3）使用DataFrame的元数据来区分连续和分类特征

（4）随机森林具有更多的功能：特征重要性的估计，以及分类中的每一个类的预测概率，也称为类条件概率。

#### 随机森林

随机森林是集成决策树，其结合大量的决策树可以减少过度拟合的风险。spark.ml实现支持二元分类和多元分类和回归随机森林，同时使用连续和类别特征，关于算法本身的更多信息可以参阅spark.mllib随机森林文档。Spark在这里列出输入和输出（预测）列类型。所有输出列都是可选的；排除一个输出列，设置其相应参数为空字符串。

  - 输入列

| 参数名称        | 类型     | 默认              | 描述   |
| ----------- | ------ | --------------- | ---- |
| labelCol    | Double | "prediction"    | 预测标签 |
| featuresCol | Vector | "rawPrediction" | 特征向量 |

表格 4‑3输入列

  - 输出列（预测）

| 参数名称             | 类型     | 默认              | 描述                                  | 注意   |
| ---------------- | ------ | --------------- | ----------------------------------- | ---- |
| predictionCol    | Double | "prediction"    | 预测标签                                |      |
| rawPredictionCol | Vector | "rawPrediction" | 长度为＃个类的向量，在进行预测的树节点上有训练实例标签的数量      | 只有分类 |
| probabilityCol   | Vector | "probability"   | 长度为＃个类的向量，等于归一化为多项式分布的rawPrediction | 只有分类 |

表格 ‑4输出列

#### 梯度提升树

梯度提升树是集成决策树，迭代的训练决策树，以尽量减少损失函数。在spark.ml中实现支持了二元分类和回归梯度提升树，同时使用连续和类别特征，关于算法本身的更多信息可以参见spark.mllib上文档。Spark在这里列出输入和输出（预测）列类型，所有输出列都是可选的；排除一个输出列，设置其相应参数为空字符串。

  - 输入列

| 参数名称        | 类型     | 默认              | 描述   |
| ----------- | ------ | --------------- | ---- |
| labelCol    | Double | "prediction"    | 预测标签 |
| featuresCol | Vector | "rawPrediction" | 特征向量 |

请注意，GBTClassifier目前只支持二进制标签。

表格 4‑5输入列

  - 输出列（预测）

| 参数名称          | 类型     | 默认           | 描述   |
| ------------- | ------ | ------------ | ---- |
| predictionCol | Double | "prediction" | 预测标签 |

表格 4‑6输出列

在将来，GBTClassifier也将输出列rawPrediction和probability，就像RandomForestClassifier做的一样。

#### 基于RDD

集成方法是一种学习算法，该算法创建一组由其他基模型构成的模型。spark.mllib支持两种主要的集成算法：GradientBoostedTrees和RandomForest。两者都使用决策树作为其基础模型。梯度提升树和随机森林都是树集成学习算法，但训练过程是不同的，需要进行一些实际的权衡：

（1）梯度提升树一次训练一棵树，因此与随机森林相比，它们的训练时间更长。随机森林可以并行训练多棵树。另一方面，与随机森林相比，梯度提升树使用较小（较浅）的树通常是合理的，并且训练较小的树所需的时间更少。

（2）随机森林可能不太容易过度拟合，在随机森林中训练更多的树可以减少过度拟合的可能性，但是使用梯度提升树训练更多的树则可以增加过度拟合的可能性。从统计语言的角度来说，随机森林通过使用更多的树来减少方差，而GBT通过使用更多的树来减少方差。

（3）随机森林可能更容易调整，因为性能随树的数量单调提高，而如果树的数量太大，梯度提升树的性能可能会开始下降。

简而言之，两种算法都是有效的，并且选择应基于特定的数据集。随机森林是决策树的集成，是用于分类和回归的最成功的机器学习模型之一。他们结合了许多决策树，以减少过度拟合的风险。像决策树一样，随机森林处理类别特征，扩展到多类分类设置，不需要特征缩放，并且能够捕获非线性和特征之间的关系。spark.mllib支持使用连续和类别特征对二元和多类分类以及进行回归的随机森林，使用现有的决策树实现来实现随机森林。随机森林分别训练一组决策树，因此可以并行进行训练。该算法将随机性注入训练过程中，因此每个决策树都略有不同，然后每棵树的预测进行合并，可以减少预测的误差从而提高测试数据的性能。注入训练过程的随机性包括：

（1）在每次迭代中对原始数据集进行二次采样以获得不同的训练集（也称为自举）。

（2）在每个树节点上，考虑要分割特征的不同随机子集。

除了这些随机化之外，决策树训练的方式与单个决策树的训练方式相同。

为了对新实例进行预测，随机森林必须聚合其决策树集中的预测。对于分类和回归，此聚合的执行方式有所不同。分类采用投票的方法，每棵树的预测被计为一种分类的投票，预测的标签将是获得最多选票的类别。回归采用求平均的方法，每棵树都预测一个真实的数值，标签被预测为每个决策树预测的平均值。

通过讨论各种参数，我们包括一些使用随机森林的准则。由于决策树指南中介绍了这些参数，因此我们省略了一些决策树参数。我们提到的前两个参数是最重要的，对其进行调整通常可以提高性能：

numTrees：森林中的树木数量。增加树的数量将减少预测的方差，从而提高测试模型阶段的准确性，训练时间在树木数量上大致呈线性增加。

maxDepth：森林中每棵树的最大深度。深度的增加使模型更具表现力，但是深树需要更长的训练时间，也更容易过度拟合。通常对比单个决策树训练，随机森林使用更深的树是可以接受的，一棵树更可能过度拟合，但是由于对森林中的多棵树进行平均而减少了方差。

接下来的两个参数通常不需要调整，但是可以对其进行调整以加快训练速度。

subsamplingRate：此参数指定用于训练森林中每棵树的数据集的大小，是原始数据集大小的一部分。建议使用默认值（1.0），但降低此比例可以加快训练速度。

featureSubsetStrategy：用作在每个树节点处分割的候选特征的数量，指定为特征总数的分数或函数，减少此数字将加快训练速度，但是如果太低有时会影响性能。

下面的示例演示如何加载 LIBSVM数据文件，将其解析为LabeledPoint，然后使用随机森林进行分类，计算测试误差以测量算法准确性。

scala\> import org.apache.spark.mllib.tree.RandomForest

import org.apache.spark.mllib.tree.RandomForest

scala\> import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.tree.model.RandomForestModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

  - 加载数据

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[18\] at map at MLUtils.scala:86

  - 将数据按比例分成训练集（0.7）和测试集（0.3）

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[19\] at randomSplit at \<console\>:28,
MapPartitionsRDD\[20\] at randomSplit at \<console\>:28)

  - 训练模型，categoricalFeaturesInfo为空表明所有特征为连续值

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[19\] at randomSplit at \<console\>:28

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[20\] at randomSplit at \<console\>:28

scala\> val numClasses = 2

numClasses: Int = 2

scala\> val categoricalFeaturesInfo = Map\[Int, Int\]()

categoricalFeaturesInfo: scala.collection.immutable.Map\[Int,Int\] =
Map()

scala\> val numTrees = 3 // Use more in practice.

numTrees: Int = 3

scala\> val featureSubsetStrategy = "auto" // Let the algorithm choose.

featureSubsetStrategy: String = auto

scala\> val impurity = "gini"

impurity: String = gini

scala\> val maxDepth = 4

maxDepth: Int = 4

scala\> val maxBins = 32

maxBins: Int = 32

scala\> val model = RandomForest.trainClassifier(trainingData,
numClasses, categoricalFeaturesInfo,

| numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

model: org.apache.spark.mllib.tree.model.RandomForestModel =

TreeEnsembleModel classifier with 3 trees

  - 在测试实例上评估模型并计算测试错误

scala\> val labelAndPreds = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelAndPreds: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[39\] at map at \<console\>:30

scala\> val testErr = labelAndPreds.filter(r =\> r.\_1 \!=
r.\_2).count.toDouble / testData.count()

testErr: Double = 0.041666666666666664

scala\> println(s"Test Error = $testErr")

Test Error = 0.041666666666666664

scala\> println(s"Learned classification forest model:\\n
${model.toDebugString}")

Learned classification forest model:

TreeEnsembleModel classifier with 3 trees

Tree 0:

If (feature 386 \<= 15.5)

If (feature 303 \<= 4.5)

If (feature 434 \<= 79.5)

Predict: 0.0

Else (feature 434 \> 79.5)

Predict: 1.0

Else (feature 303 \> 4.5)

Predict: 0.0

Else (feature 386 \> 15.5)

Predict: 0.0

Tree 1:

If (feature 351 \<= 36.0)

Predict: 0.0

Else (feature 351 \> 36.0)

Predict: 1.0

Tree 2:

If (feature 405 \<= 21.0)

If (feature 298 \<= 253.5)

Predict: 0.0

Else (feature 298 \> 253.5)

Predict: 1.0

Else (feature 405 \> 21.0)

Predict: 1.0

  - 保存和加载模型

scala\> model.save(sc, "/tmp/myRandomForestClassificationModel")

scala\> val sameModel = RandomForestModel.load(sc,
"/tmp/myRandomForestClassificationModel")

sameModel: org.apache.spark.mllib.tree.model.RandomForestModel =

TreeEnsembleModel classifier with 3 trees

下面的示例演示如何加载LIBSVM数据文件，将其解析为LabeledPoint，然后使用随机森林执行回归，最后计算均方误差（MSE）以评估拟合度。

scala\> import org.apache.spark.mllib.tree.RandomForest

import org.apache.spark.mllib.tree.RandomForest

scala\> import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.tree.model.RandomForestModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

  - 加载数据

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[68\] at map at MLUtils.scala:86

  - 将数据按比例分成训练集（0.7）和测试集（0.3）

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[69\] at randomSplit at \<console\>:32,
MapPartitionsRDD\[70\] at randomSplit at \<console\>:32)

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[69\] at randomSplit at \<console\>:32

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[70\] at randomSplit at \<console\>:32

  - 训练模型，categoricalFeaturesInfo为空表明所有特征为连续值

scala\> val numClasses = 2

numClasses: Int = 2

scala\> val categoricalFeaturesInfo = Map\[Int, Int\]()

categoricalFeaturesInfo: scala.collection.immutable.Map\[Int,Int\] =
Map()

scala\> val numTrees = 3 // Use more in practice.

numTrees: Int = 3

scala\> val featureSubsetStrategy = "auto" // Let the algorithm choose.

featureSubsetStrategy: String = auto

scala\> val impurity = "variance"

impurity: String = variance

scala\> val maxDepth = 4

maxDepth: Int = 4

scala\> val maxBins = 32

maxBins: Int = 32

scala\> val model = RandomForest.trainRegressor(trainingData,
categoricalFeaturesInfo,

| numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

model: org.apache.spark.mllib.tree.model.RandomForestModel =

TreeEnsembleModel regressor with 3 trees

  - 在测试实例上评估模型并计算测试错误

scala\> val labelsAndPredictions = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelsAndPredictions: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[86\] at map at \<console\>:34

scala\> val testMSE = labelsAndPredictions.map{ case(v, p) =\>
math.pow((v - p), 2)}.mean()

20/04/27 07:18:00 WARN BLAS: Failed to load implementation from:
com.github.fommil.netlib.NativeSystemBLAS

20/04/27 07:18:00 WARN BLAS: Failed to load implementation from:
com.github.fommil.netlib.NativeRefBLAS

testMSE: Double = 0.0

scala\> println(s"Test Mean Squared Error = $testMSE")

Test Mean Squared Error = 0.0

scala\> println(s"Learned regression forest model:\\n
${model.toDebugString}")

Learned regression forest model:

TreeEnsembleModel regressor with 3 trees

Tree 0:

If (feature 490 \<= 43.0)

Predict: 0.0

Else (feature 490 \> 43.0)

Predict: 1.0

Tree 1:

If (feature 406 \<= 9.5)

Predict: 0.0

Else (feature 406 \> 9.5)

If (feature 327 \<= 81.0)

Predict: 1.0

Else (feature 327 \> 81.0)

Predict: 0.0

Tree 2:

If (feature 512 \<= 1.5)

If (feature 511 \<= 1.5)

Predict: 1.0

Else (feature 511 \> 1.5)

Predict: 0.0

Else (feature 512 \> 1.5)

Predict: 0.0

  - 保存和加载模型

scala\> model.save(sc, "/tmp/myRandomForestRegressionModel")

scala\> val sameModel = RandomForestModel.load(sc,
"/tmp/myRandomForestRegressionModel")

sameModel: org.apache.spark.mllib.tree.model.RandomForestModel =

TreeEnsembleModel regressor with 3 trees

梯度提升树是决策树的集成，是以最小化损失函数为代价迭代地训练决策树。像决策树一样，梯度提升树处理类别特征，扩展到多分类设置，不需要特征缩放，并且能够捕获非线性和特征之间相互关系。spark.mllib支持使用连续和类别特征进行二元分类和回归的梯度提升树，使用现有的决策树来实现。梯度提升树尚不支持多类分类，对于多类问题请使用决策树或随机森林，参阅决策树指南以获取有关树的更多信息。

梯度提升以迭代方式训练决策树序列，在每次迭代中该算法使用当前集成预测每个训练实例的标签，然后将该预测与真实标签进行比较。接下来，重新标记数据集，以将更多的重点放在预测效果较差的训练实例上，因此在下一次迭代中决策树将有助于纠正先前的错误。重新标记实例的特定机制由损失函数定义（如下所述），每次迭代梯度提升树都会进一步减少训练数据上的损失函数。

下表列出了梯度提升树在当前spark.mllib中支持的损失函数，请注意每种损失函数都适用于分类或回归，但不能同时适用于两者。

符号：$$N$$=实例数，$$y_{i}$$=实例$$i$$的标签，$$x_{i}$$=实例$$i$$的特征，$$F(x_{i})$$=模型对于实例$$i$$的预测标签。

| 损失函数 | 任务 | 公式                                                       | 描述                       |
| ---- | -- | -------------------------------------------------------- | ------------------------ |
| 对数   | 分类 | $$2\sum_{i = 1}^{N}{}\log(1 + \exp( - 2y_{i}F(x_{i})))$$ | 两次二项式负对数似然。              |
| 平方误差 | 回归 | $$\sum_{i = 1}^{N}{}(y_{i} - F(x_{i}))^{2}$$             | 也称为L2损失函数，回归任务的默认损失。     |
| 绝对误差 | 回归 | $$\sum_{i = 1}^{N}|y_{i} - F(x_{i})|$$                   | 也称为L1损失函数，对于离群值比平方误差更健壮。 |

表格 4‑7 损失函数

通过讨论各种参数，我们包括一些使用梯度提升树的准则，由于决策树指南中介绍了这些参数，因此我们省略了一些决策树参数。

loss：有关损失函数及其对任务的适用性的信息，请参见表格 4‑7。根据数据集的不同，不同的损失函数可能会产生明显不同的结果。

numIterations：设置集成中树木的数量，每次迭代都会生成一棵树。增加此数字可使模型更具表现力，从而提高训练数据的准确性，但是如果太大则可能会降低测试阶段的精度。

learningRate：不需要调整此参数。如果算法行为似乎不稳定，则减小该值可以提高稳定性。

algo：使用树\[Strategy\]参数设置算法或任务。

当训练更多树木时，梯度增强可能会过拟合，为了防止过度拟合在训练时进行验证很有用，提供了runWithValidation()方法来使用此选项。此方法以一对RDD作为参数，第一个是训练数据集，第二个是验证数据集，当验证错误的改善不超过某个公差时训练将停止，由BoostingStrategy中的validationTol参数提供。在实践中，验证误差最初会减小，随后会增大。在某些情况下，验证误差不会单调变化，建议用户设置足够大的负公差，并使用evaluateEachIteration
给出每次迭代的误差或损失，调整迭代次数来检查验证曲线。

下面的示例演示如何加载
LIBSVM数据文件，将其解析为LabeledPoint，然后使用带有对数损失函数的梯度增强树进行分类，计算测试误差以测量算法准确性。

scala\> import org.apache.spark.mllib.tree.GradientBoostedTrees

import org.apache.spark.mllib.tree.GradientBoostedTrees

scala\> import
org.apache.spark.mllib.tree.configuration.BoostingStrategy

import org.apache.spark.mllib.tree.configuration.BoostingStrategy

scala\> import
org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

  - 加载数据

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[6\] at map at MLUtils.scala:86

  - 将数据按比例分成训练集（0.7）和测试集（0.3）

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[7\] at randomSplit at \<console\>:29,
MapPartitionsRDD\[8\] at randomSplit at \<console\>:29)

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[7\] at randomSplit at \<console\>:29

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[8\] at randomSplit at \<console\>:29

  - 训练GradientBoostedTrees模型，分类任务缺省使用对数损失函数

scala\> val boostingStrategy =
BoostingStrategy.defaultParams("Classification")

boostingStrategy:
org.apache.spark.mllib.tree.configuration.BoostingStrategy =
BoostingStrategy(org.apache.spark.mllib.tree.configuration.Strategy@298eca94,org.apache.spark.mllib.tree.loss.LogLoss$@6a51a39d,100,0.1,0.001)

scala\> boostingStrategy.numIterations = 3 // Note: Use more iterations
in practice.

boostingStrategy.numIterations: Int = 3

scala\> boostingStrategy.treeStrategy.numClasses = 2

boostingStrategy.treeStrategy.numClasses: Int = 2

scala\> boostingStrategy.treeStrategy.maxDepth = 5

boostingStrategy.treeStrategy.maxDepth: Int = 5

  - 空的categoricalFeaturesInfo表示所有特征都是连续的。

scala\> boostingStrategy.treeStrategy.categoricalFeaturesInfo =
Map\[Int, Int\]()

boostingStrategy.treeStrategy.categoricalFeaturesInfo: Map\[Int,Int\] =
Map()

scala\> val model = GradientBoostedTrees.train(trainingData,
boostingStrategy)

model: org.apache.spark.mllib.tree.model.GradientBoostedTreesModel =

TreeEnsembleModel classifier with 3 trees

  - 在测试实例上评估模型并计算测试错误

scala\> val labelAndPreds = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelAndPreds: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[78\] at map at \<console\>:31

scala\> val testErr = labelAndPreds.filter(r =\> r.\_1 \!=
r.\_2).count.toDouble / testData.count()

20/04/27 15:40:48 WARN BLAS: Failed to load implementation from:
com.github.fommil.netlib.NativeSystemBLAS

20/04/27 15:40:48 WARN BLAS: Failed to load implementation from:
com.github.fommil.netlib.NativeRefBLAS

testErr: Double = 0.07692307692307693

scala\> println(s"Test Error = $testErr")

Test Error = 0.07692307692307693

scala\> println(s"Learned classification GBT model:\\n
${model.toDebugString}")

Learned classification GBT model:

TreeEnsembleModel classifier with 3 trees

Tree 0:

If (feature 405 \<= 21.0)

If (feature 100 \<= 193.5)

Predict: -1.0

Else (feature 100 \> 193.5)

Predict: 1.0

Else (feature 405 \> 21.0)

Predict: 1.0

Tree 1:

If (feature 490 \<= 43.0)

If (feature 435 \<= 32.5)

If (feature 155 \<= 230.5)

Predict: -0.4768116880884702

Else (feature 155 \> 230.5)

Predict: -0.47681168808847035

Else (feature 435 \> 32.5)

Predict: 0.4768116880884694

Else (feature 490 \> 43.0)

If (feature 241 \<= 169.5)

If (feature 124 \<= 49.5)

If (feature 155 \<= 59.0)

Predict: 0.4768116880884702

Else (feature 155 \> 59.0)

Predict: 0.4768116880884703

Else (feature 124 \> 49.5)

Predict: 0.4768116880884703

Else (feature 241 \> 169.5)

Predict: 0.47681168808847035

Tree 2:

If (feature 406 \<= 140.5)

If (feature 435 \<= 32.5)

If (feature 329 \<= 154.0)

Predict: -0.43819358104272055

Else (feature 329 \> 154.0)

Predict: -0.43819358104272066

Else (feature 435 \> 32.5)

Predict: 0.43819358104271977

Else (feature 406 \> 140.5)

If (feature 245 \<= 1.5)

If (feature 490 \<= 153.5)

Predict: 0.4381935810427206

Else (feature 490 \> 153.5)

Predict: 0.43819358104272066

Else (feature 245 \> 1.5)

Predict: 0.43819358104272066

  - 保存和加载模型

scala\> model.save(sc, "/tmp/myGradientBoostingClassificationModel")

scala\> val sameModel =
GradientBoostedTreesModel.load(sc,"/tmp/myGradientBoostingClassificationModel")

sameModel: org.apache.spark.mllib.tree.model.GradientBoostedTreesModel =

TreeEnsembleModel classifier with 3 trees

下面的示例演示如何加载LIBSVM数据文件，将其解析为LabeledPoint、，然后使用以平方误差为损失函数的梯度增强树执行回归，最后计算均方误差（MSE）以评估拟合度。

scala\> import org.apache.spark.mllib.tree.GradientBoostedTrees

import org.apache.spark.mllib.tree.GradientBoostedTrees

scala\> import
org.apache.spark.mllib.tree.configuration.BoostingStrategy

import org.apache.spark.mllib.tree.configuration.BoostingStrategy

scala\> import
org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

scala\> import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.util.MLUtils

  - 加载数据

scala\> val data = MLUtils.loadLibSVMFile(sc,
"/spark/data/mllib/sample\_libsvm\_data.txt")

data:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[115\] at map at MLUtils.scala:86

  - 将数据按比例分成训练集（0.7）和测试集（0.3）

scala\> val splits = data.randomSplit(Array(0.7, 0.3))

splits:
Array\[org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]\]
= Array(MapPartitionsRDD\[116\] at randomSplit at \<console\>:34,
MapPartitionsRDD\[117\] at randomSplit at \<console\>:34)

scala\> val (trainingData, testData) = (splits(0), splits(1))

trainingData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[116\] at randomSplit at \<console\>:34

testData:
org.apache.spark.rdd.RDD\[org.apache.spark.mllib.regression.LabeledPoint\]
= MapPartitionsRDD\[117\] at randomSplit at \<console\>:34

  - 训练GradientBoostedTrees模型，回归任务缺省使用平方误差损失函数

scala\> val boostingStrategy =
BoostingStrategy.defaultParams("Regression")

boostingStrategy:
org.apache.spark.mllib.tree.configuration.BoostingStrategy =
BoostingStrategy(org.apache.spark.mllib.tree.configuration.Strategy@25cc8a11,org.apache.spark.mllib.tree.loss.SquaredError$@29fccb14,100,0.1,0.001)

scala\> boostingStrategy.numIterations = 3 // Note: Use more iterations
in practice.

boostingStrategy.numIterations: Int = 3

scala\> boostingStrategy.treeStrategy.maxDepth = 5

boostingStrategy.treeStrategy.maxDepth: Int = 5

scala\> boostingStrategy.treeStrategy.categoricalFeaturesInfo =
Map\[Int, Int\]()

boostingStrategy.treeStrategy.categoricalFeaturesInfo: Map\[Int,Int\] =
Map()

scala\> val model = GradientBoostedTrees.train(trainingData,
boostingStrategy)

model: org.apache.spark.mllib.tree.model.GradientBoostedTreesModel =

TreeEnsembleModel regressor with 3 trees

  - 在测试实例上评估模型并计算测试错误

scala\> val labelsAndPredictions = testData.map { point =\>

| val prediction = model.predict(point.features)

| (point.label, prediction)

| }

labelsAndPredictions: org.apache.spark.rdd.RDD\[(Double, Double)\] =
MapPartitionsRDD\[165\] at map at \<console\>:36

scala\> val testMSE = labelsAndPredictions.map{ case(v, p) =\>
math.pow((v - p), 2)}.mean()

testMSE: Double = 0.058823529411764705

scala\> println(s"Test Mean Squared Error = $testMSE")

Test Mean Squared Error = 0.058823529411764705

scala\> println(s"Learned regression GBT model:\\n
${model.toDebugString}")

Learned regression GBT model:

TreeEnsembleModel regressor with 3 trees

Tree 0:

If (feature 406 \<= 22.0)

If (feature 99 \<= 35.0)

Predict: 0.0

Else (feature 99 \> 35.0)

Predict: 1.0

Else (feature 406 \> 22.0)

Predict: 1.0

Tree 1:

Predict: 0.0

Tree 2:

Predict: 0.0

  - 保存和加载模型

scala\> model.save(sc, "/tmp/myGradientBoostingRegressionModel")

scala\> val sameModel =
GradientBoostedTreesModel.load(sc,"/tmp/myGradientBoostingRegressionModel")

sameModel: org.apache.spark.mllib.tree.model.GradientBoostedTreesModel =

TreeEnsembleModel regressor with 3 trees

## 分类和回归

spark.mllib包支持二元分类、多类分类和回归分析的各种方法，下表列出了每类问题所支持的算法。

| 问题类型 | 支持方法                                                                                                                       |
| ---- | -------------------------------------------------------------------------------------------------------------------------- |
| 二元分类 | Linear SVMs, Logistic Regression, Decision Trees, Random Forests, Gradient-Boosted Trees, Naive Bayes                      |
| 多元分类 | Logistic Regression, Decision Trees, Random Forests, Naive Bayes                                                           |
| 回归   | Linear Least Squares, Lasso, Ridge Regression, Decision Trees, Random Forests, Gradient-Boosted Trees, Isotonic Regression |

表格 ‑8问题类型和支持方法

### 线性方法

Spark实现了流行的线性方法，如逻辑回归和$$L_{1}$$或$$L_{2}$$正则化的线性最小二乘法。Spark还包括一个弹性网络的DataFrame
API，是Zou等人提出的$$L_{1}$$和$$L_{2}$$正则化的混合体，正则化和通过弹性网络进行变量选择。在数学上，它被定义为$$L_{1}$$和$$L_{2}$$正则化项的凸组合：

$$
\alpha \lambda \|\mathbf{w}\|_{1}
 + (1-\alpha)\frac{\lambda}{2}\|\mathbf{w}\|_{2}^{2},
\quad \alpha \in [0,1],\ \lambda \ge 0
$$

公式 4‑1

通过恰当地设定$$\alpha$$，弹性网络既包含$$L_{1}$$又包含$$L_{2}$$正则化作为特例。例如，如果一个线性回归模型是用弹性网络参数α设置为$$1$$来训练的，则相当于一个Lasso模型。另一方面，如果$$\alpha$$被设置为$$0$$，则训练的模型变为岭回归模型。Spark实施管道API的线性回归和逻辑回归弹性网络正则化。

许多标准的机器学习方法可以被表述为一个凸优化问题，也就是寻找一个依赖于变量向量$$\mathbf{w}$$（称为代码中的权重）的凸函数$$f$$的最小化的任务，其具有$$d$$条目。在形式上，Spark可以把它写成优化问题$$\min_{\mathbf{w} \in \mathbb{R}^{d}}f(\mathbf{w})$$目标函数的形式为

$$
f(\mathbf{w}) = \lambda R(\mathbf{w}) + \frac{1}{n}\sum_{i=1}^{n} L(\mathbf{w};\mathbf{x}_{i}, y_{i})
$$

公式 4‑2

在这里，向量$$\mathbf{x}_{i} \in \mathbb{R}^{d}$$是训练数据的例子，对于$$1 \leq i \leq n$$和$$y_{i} \in \mathbb{R}$$是他对应的标签，预测。如果$$L(\mathbf{w};\mathbf{x},y)$$可以表示为$$\mathbf{w}^{T}x$$和$$y$$的函数，Spark称该方法为线性的。spark.mllib的几个分类和回归算法属于这个类别，在这里讨论。

目标函数$$f$$有两个部分：控制模型复杂度的正则化器和测量训练数据模型误差的损失。损失函数$$L(\mathbf{w};.)$$通常是$$\mathbf{w}$$一个凸函数。固定正则化参数$$\lambda \geq 0$$（在代码中为regParam）定义了最小化损失（即训练误差）和最小化模型复杂性（即避免过拟合）这两个目标之间的折衷。下表总结了spark.mllib支持的方法损失函数及其梯度或子梯度：

|      | 损失函数$$\ L(\mathbf{w};\mathbf{x},y)$$                               | 渐变或次渐变                                                                                                                          |
| ---- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| 铰链损失 | $$\max\{ 0,1 - y\mathbf{w}^{T}\mathbf{x}\},y \in \{ - 1, + 1\}$$   | $$\begin{cases} - y \cdot \mathbf{x}, & \text{if } y\mathbf{w}^{T}\mathbf{x} < 1 \\ 0, & \text{otherwise} \end{cases}$$ |
| 逻辑损失 | $$log(1 + exp( - y\mathbf{w}^{T}\mathbf{x})),y \in \{ - 1, + 1\}$$ | $$- y(1 - \frac{1}{1 + exp( - y\mathbf{w}^{T}\mathbf{x})}) \cdot \mathbf{x}$$                                                   |
| 平方损失 | $$\frac{1}{2}(\mathbf{w}^{T}\mathbf{x} - y)^{2},y \in \mathbb{R}$$ | $$(\mathbf{w}^{T}\mathbf{x} - y) \cdot \mathbf{x}$$                                                                             |

表格 4‑9spark.mllib支持的方法损失函数及其梯度或子梯度

请注意，在上面的数学公式中，二元标签$$y$$表示为$$+ 1$$（正数）或$$- 1$$（负数），便于公式化。但是在spark.mllib中负标签由$$0$$表示而不是$$- 1$$，以便与多类标签保持一致。正规化的目的是鼓励简单的模型，避免过度拟合。Spark在spark.mllib中支持以下正规化：

|                     | 正则化$$R(\mathbf{w})$$                                                          | 渐变或次渐变                                                |
| ------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------- |
| zero(unregularized) | $$\mathbf{0}$$                                                               | $$\mathbf{0}$$                                        |
| L2                  | $$\frac{1}{2}\|\mathbf{w}\|_{2}^{2}$$                                        | $$\mathbf{w}$$                                        |
| L1                  | $$\|\mathbf{w}\|_{1}$$                                                       | $$\text{sign}(\mathbf{w})$$                           |
| 弹性网络                | $$\alpha\|\mathbf{w}\|_{1} + (1 - \alpha)\frac{1}{2}\|\mathbf{w}\|_{2}^{2}$$ | $$\alpha\,\text{sign}(\mathbf{w}) + (1 - \alpha)\mathbf{w}$$ |

表格 4‑10spark.mllib中支持的正规化

这里$$\text{sign}(\mathbf{w})$$是向量，由$$\mathbf{w}$$**的**所有条目的signs
($$\pm 1$$)组成。L2正则化问题比L1正则化更容易解决，然而L1正则化可以帮助提高权重的稀疏性，从而导致更小和更易解释的模型，后者可以用于特征选择。弹性网络是L1和L2正则化的组合。不建议在没有正规化的情况下对模型进行训练，尤其是在训练样例数量较少的情况下。

### 分类

#### 逻辑回归 

逻辑回归是预测分类响应的流行方法。广义线性模型的一个特例是预测结果的概率。在spark.ml中，逻辑回归可以通过使用二项逻辑回归用来预测一个二元结果，或者它可以通过多分类逻辑回归用来预测一个多分类结果。使用family参数在这两个算法之间进行选择，或者将其保留，Spark将推断出正确的变体，通过将family参数设置为“multinomial”，多项逻辑回归可用于二元分类，会产生两套系数和两个截距。当在具有恒定非零列的数据集上，不带截距拟合LogisticRegressionModel时，Spark
MLlib为恒定的非零列输出零系数，这种行为与R语言中的glmnet相同，但与LIBSVM不同。

##### 二元逻辑回归 

逻辑回归广泛用于预测二元响应。它是如上面方程公式 4‑24中所描述的线性方法，在logistic损失给出的公式中具有损失函数：

$$L(\mathbf{w};\mathbf{x},y): = \log(1 + \exp( - y\mathbf{w}^{T}\mathbf{x})).$$

公式 4‑2

对于二元分类问题，算法输出一个二元逻辑回归模型。给定一个新的数据点，用xx表示，该模型通过应用逻辑函数做出预测

$$f(z) = \frac{1}{1 + e^{- z}}$$

公式 4‑3

其中$$z = \mathbf{w}^{T}\mathbf{x}$$。默认情况下，如果$$f(\mathbf{w}^{T}x) > 0.5$$，则结果为正，否则为负，尽管与线性SVM不同，逻辑回归模型的原始输出$$f(z)$$概率解释（即$$\mathbf{x}$$为正的概率）。

二元逻辑回归可以推广到多项逻辑回归来训练和预测多类分类问题。例如，对于$$K$$可能的结果，可以选择其中一个结果作为“支点”，其他$$K - 1$$结果可以分别与枢纽结果进行回归。在spark.mllib中，第一类$$0$$被选为“支点”类。对于多元分类问题，该算法将输出一个多项逻辑回归模型，其中包含对第一类进行回归的$$K - 1$$二元逻辑回归模型。给定一个新的数据点，$$\ K - 1$$模型将被运行，最大概率的类被选择作为预测类。Spark实现了两种算法来解决逻辑回归：小批量梯度下降和L-BFGS。Spark推荐使用L-BFGS而优于小批量梯度下降，以加速收敛。下面的例子显示了如何训练的二项模型和为二元分类使用弹性净正则化的多项逻辑回归模型。elasticNetParam对应于$$\alpha$$，regParam对应于$$\lambda$$。

scala\> import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.classification.LogisticRegression

代码 4‑1

  - 加载训练数据

scala\> val training =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

training: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

scala\> val lr = new
LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

lr: org.apache.spark.ml.classification.LogisticRegression =
logreg\_8f4d315ead25

scala\>

代码 4‑2

  - 拟合模型

scala\> val lrModel = lr.fit(training)

lrModel: org.apache.spark.ml.classification.LogisticRegressionModel =
logreg\_8f4d315ead25

代码 4‑3

  - 打印逻辑回归的系数和截距

scala\> println(s"Coefficients: ${lrModel.coefficients} Intercept:
${lrModel.intercept}")

Coefficients:
(692,\[244,263,272,300,301,328,350,351,378,379,405,406,407,428,433,434,455,456,461,462,483,484,489,490,496,511,512,517,539,540,568\],\[-7.353983524188197E-5,-9.102738505589466E-5,-1.9467430546904298E-4,-2.0300642473486668E-4,-3.1476183314863995E-5,-6.842977602660743E-5,1.5883626898239883E-5,1.4023497091372047E-5,3.5432047524968605E-4,1.1443272898171087E-4,1.0016712383666666E-4,6.014109303795481E-4,2.840248179122762E-4,-1.1541084736508837E-4,3.85996886312906E-4,6.35019557424107E-4,-1.1506412384575676E-4,-1.5271865864986808E-4,2.804933808994214E-4,6.070117471191634E-4,-2.008459663247437E-4,-1.421075579290126E-4,2.739010341160883E-4,2.7730456244968115E-4,-9.838027027269332E-5,-3.808522443517704E-4,-2.5315198008555033E-4,2.7747714770754307E-4,-2.443619763919199E-4,-0.0015394744687597765,-2.3073328411331293E-4\])
Intercept: 0.22456315961250325

代码 4‑4

  - 为二元分类设置family为multinomial

scala\> val mlr = new
LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")

mlr: org.apache.spark.ml.classification.LogisticRegression =
logreg\_2c2e16eac30e

scala\> val mlrModel = mlr.fit(training)

mlrModel: org.apache.spark.ml.classification.LogisticRegressionModel =
logreg\_2c2e16eac30e

代码 4‑5

  - 打印逻辑回归的系数和截距

scala\> println(s"Multinomial coefficients:
${mlrModel.coefficientMatrix}")

Multinomial coefficients: 2 x 692 CSCMatrix

(0,244) 4.290365458958277E-5

(1,244) -4.290365458958294E-5

(0,263) 6.488313287833108E-5

(1,263) -6.488313287833092E-5

(0,272) 1.2140666790834663E-4

(1,272) -1.2140666790834657E-4

(0,300) 1.3231861518665612E-4

(1,300) -1.3231861518665607E-4

(0,350) -6.775444746760509E-7

(1,350) 6.775444746761932E-7

(0,351) -4.899237909429297E-7

(1,351) 4.899237909430322E-7

(0,378) -3.5812102770679596E-5

(1,378) 3.581210277067968E-5

(0,379) -2.3539704331222065E-5

(1,379) 2.353970433122204E-5

(0,405) -1.90295199030314E-5

(1,405) 1.90295199030314E-5

……

scala\> println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

Multinomial intercepts: \[-0.12065879445860686,0.12065879445860686\]

代码 4‑6

逻辑回归的spark.ml实现也支持在训练集上提取模型的摘要。请注意，预测和度量被保存为在BinaryLogisticRegressionSummary中的DataFrame，被标注为@transient，因此仅在驱动程序上可用。

LogisticRegressionTrainingSummary提供了LogisticRegressionModel的摘要。目前，仅支持二元分类，并且必须将摘要明确地转换为BinaryLogisticRegressionTrainingSummary。当支持多类别分类时，这可能会改变，继续前面的例子：

scala\> import
org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary,
LogisticRegression}

import
org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary,
LogisticRegression}

代码 4‑7

  - 从上例训练的LogisticRegressionModel抽取摘要

scala\> val trainingSummary = lrModel.summary

trainingSummary:
org.apache.spark.ml.classification.LogisticRegressionTrainingSummary =
<org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary@305a362a>

代码 4‑8

  - 获得每个迭代的目标

scala\> val objectiveHistory = trainingSummary.objectiveHistory

objectiveHistory: Array\[Double\] = Array(0.6833149135741672,
0.6662875751473734, 0.6217068546034618, 0.6127265245887887,
0.6060347986802873, 0.6031750687571562, 0.5969621534836274,
0.5940743031983118, 0.5906089243339022, 0.5894724576491042,
0.5882187775729587)

scala\> println("objectiveHistory:")

objectiveHistory:

scala\> objectiveHistory.foreach(loss =\> println(loss))

0.6833149135741672

0.6662875751473734

0.6217068546034618

0.6127265245887887

0.6060347986802873

0.6031750687571562

0.5969621534836274

0.5940743031983118

0.5906089243339022

0.5894724576491042

0.5882187775729587

代码 ‑9

  - 获得度量用来在测试数据上判断性能，转换总结为BinaryLogisticRegressionSummary

scala\> val binarySummary =
trainingSummary.asInstanceOf\[BinaryLogisticRegressionSummary\]

binarySummary:
org.apache.spark.ml.classification.BinaryLogisticRegressionSummary =
org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary@305a362a

  - 获得ROCDataFrame和areaUnderROC.

scala\> val roc = binarySummary.roc

roc: org.apache.spark.sql.DataFrame = \[FPR: double, TPR: double\]

scala\> roc.show()

\+---+--------------------+

|FPR| TPR|

\+---+--------------------+

|0.0| 0.0|

|0.0|0.017543859649122806|

|0.0| 0.03508771929824561|

|0.0| 0.05263157894736842|

|0.0| 0.07017543859649122|

|0.0| 0.08771929824561403|

|0.0| 0.10526315789473684|

|0.0| 0.12280701754385964|

|0.0| 0.14035087719298245|

|0.0| 0.15789473684210525|

|0.0| 0.17543859649122806|

|0.0| 0.19298245614035087|

|0.0| 0.21052631578947367|

|0.0| 0.22807017543859648|

|0.0| 0.24561403508771928|

|0.0| 0.2631578947368421|

|0.0| 0.2807017543859649|

|0.0| 0.2982456140350877|

|0.0| 0.3157894736842105|

|0.0| 0.3333333333333333|

\+---+--------------------+

only showing top 20 rows

scala\> println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

areaUnderROC: 1.0

代码 4‑10

  - 设置模型阈值最大化F-Measure

scala\> val fMeasure = binarySummary.fMeasureByThreshold

fMeasure: org.apache.spark.sql.DataFrame = \[threshold: double,
F-Measure: double\]

scala\> val maxFMeasure =
fMeasure.select(max("F-Measure")).head().getDouble(0)

maxFMeasure: Double = 1.0

scala\> val bestThreshold = fMeasure.where($"F-Measure" ===
maxFMeasure).select("threshold").head().getDouble(0)

bestThreshold: Double = 0.5585022394278357

scala\> lrModel.setThreshold(bestThreshold)

res7: lrModel.type = logreg\_8f4d315ead25

代码 4‑11

##### 多项逻辑回归 

多类分类通过多项式逻辑（softmax）回归来支持。在多项逻辑回归中，算法产生$$K$$组系数，或维数$$K \times J$$的矩阵，其中$$K$$是结果类的数量，$$J$$是特征的数量。如果该算法适合于截距项，则截距的长度为$$K$$的向量是可用的。多项系数可用作coefficientMatrix矩阵，截距可用作interceptVector
。

在使用多元系列训练的逻辑回归模型上，coefficients和intercept方法不被支持。改用coefficientMatrix和interceptVector。输出结果类$$k \in 1,2,\ldots,K$$的条件概率使用softmax函数进行建模。

$$
P(Y=k\mid \mathbf{X},\mathbf{\beta}_{k},\beta_{0k})
= \frac{\exp(\mathbf{\beta}_{k}\cdot\mathbf{X}+\beta_{0k})}
{\sum_{j=0}^{K-1}\exp(\mathbf{\beta}_{j}\cdot\mathbf{X}+\beta_{0j})}
$$

公式 4‑4

Spark使用多项式响应模型将加权负对数似然最小化，并使用弹性网络（elastic-net）惩罚来控制过拟合。

$$
\min_{\beta,\beta_0}
\left[
-\sum_{i=1}^{L} w_i \log P(Y=y_i\mid x_i)
+ \lambda\left(\frac{1-\alpha}{2}\|\beta\|_2^2 + \alpha\|\beta\|_1\right)
\right]
$$

公式 4‑5

下面的例子展示了如何训练具有弹性网络正则化的多类逻辑回归模型。

scala\> import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.classification.LogisticRegression

代码 4‑12

  - 加载训练数据

scala\> val training =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_multiclass\_classification\_data.txt")

training: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

scala\> val lr = new
LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

lr: org.apache.spark.ml.classification.LogisticRegression =
logreg\_45c92230da22

代码 ‑13

  - 拟合模型

scala\> val lrModel = lr.fit(training)

lrModel: org.apache.spark.ml.classification.LogisticRegressionModel =
logreg\_45c92230da22

代码 ‑14

  - 打印多元逻辑回归系数和截距

scala\> println(s"Coefficients: \\n${lrModel.coefficientMatrix}")

Coefficients:

3 x 4 CSCMatrix

(1,2) -0.7803943459681859

(0,3) 0.3176483191238039

(1,3) -0.3769611423403096

scala\> println(s"Intercepts: ${lrModel.interceptVector}")

Intercepts:
\[0.05165231659832854,-0.12391224990853622,0.07225993331020768\]

代码 4‑15

#### 决策树分类器 

决策树是一种流行的分类和回归方法。以下示例以LibSVM格式加载数据集，将其分解为训练集和测试集，在第一个数据集上训练，然后在保留的测试集上进行评估。Spark使用两个特征转换器来准备数据；这些帮助索引标签和分类特征的类别，将元数据添加到决策树算法可以识别的DataFrame中。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import
org.apache.spark.ml.classification.DecisionTreeClassificationModel

import
org.apache.spark.ml.classification.DecisionTreeClassificationModel

scala\> import org.apache.spark.ml.classification.DecisionTreeClassifier

import org.apache.spark.ml.classification.DecisionTreeClassifier

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

scala\> import org.apache.spark.ml.feature.{IndexToString,
StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.{IndexToString, StringIndexer,
VectorIndexer}

代码 4‑16

  - 加载格式为LIBSVM的数据到DataFrame中

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 ‑17

  - 索引标签，增加元数据到标签列，拟合整个数据集在索引中包含所有标签

scala\> val labelIndexer = new
StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

labelIndexer: org.apache.spark.ml.feature.StringIndexerModel =
strIdx\_d251401baba6

代码 4‑18

  - 自动识别分类特征和索引，具有\>4个不同值的特征被作为连续值处理

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_0d7150e8751b

代码 ‑19

  - 分割数据为训练和测试（30%为测试数据）

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑20

  - 训练决策树模型

scala\> val dt = new
DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

dt: org.apache.spark.ml.classification.DecisionTreeClassifier =
dtc\_e50420acd179

代码 4‑21

  - 将索引标签转换成原始标签

scala\> val labelConverter = new
IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

labelConverter: org.apache.spark.ml.feature.IndexToString =
idxToStr\_c466772ec170

代码 ‑22

  - 链接索引和树到一个管道中

scala\> val pipeline = new Pipeline().setStages(Array(labelIndexer,
featureIndexer, dt, labelConverter))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_44d35fa1ad84

代码 4‑23

  - 训练模型

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_44d35fa1ad84

代码 4‑24

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 6 more fields\]

代码 4‑25

  - 选择示例行显示

scala\> predictions.select("predictedLabel", "label",
"features").show(5)

\+--------------+-----+--------------------+

|predictedLabel|label| features|

\+--------------+-----+--------------------+

| 0.0| 0.0|(692,\[122,123,148...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.0| 0.0|(692,\[124,125,126...|

| 0.0| 0.0|(692,\[124,125,126...|

| 0.0| 0.0|(692,\[124,125,126...|

\+--------------+-----+--------------------+

only showing top 5 rows

代码 4‑26

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_ec0435d1da2b

scala\> val accuracy = evaluator.evaluate(predictions)

accuracy: Double = 0.9583333333333334

scala\> println("Test Error = " + (1.0 - accuracy))

Test Error = 0.04166666666666663

scala\> val treeModel =
model.stages(2).asInstanceOf\[DecisionTreeClassificationModel\]

treeModel:
org.apache.spark.ml.classification.DecisionTreeClassificationModel =
DecisionTreeClassificationModel (uid=dtc\_e50420acd179) of depth 2 with
5 nodes

scala\> println("Learned classification tree model:\\n" +
treeModel.toDebugString)

Learned classification tree model:

DecisionTreeClassificationModel (uid=dtc\_e50420acd179) of depth 2 with
5 nodes

If (feature 406 \<= 20.0)

If (feature 99 in {2.0})

Predict: 0.0

Else (feature 99 not in {2.0})

Predict: 1.0

Else (feature 406 \> 20.0)

Predict: 0.0

代码 4‑27

#### 随机森林分类器 

随机森林是一种流行的分类和回归方法。以下示例以LibSVM格式加载数据集，将其分解为训练集和测试集，在第一个数据集上训练，然后在保留的测试集上进行评估。Spark使用两个特征转换器来准备数据；这些帮助索引标签和分类特征的类别，将元数据添加到决策树算法可以识别的DataFrame中。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import
org.apache.spark.ml.classification.{RandomForestClassificationModel,
RandomForestClassifier}

import
org.apache.spark.ml.classification.{RandomForestClassificationModel,
RandomForestClassifier}

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

scala\> import org.apache.spark.ml.feature.{IndexToString,
StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.{IndexToString, StringIndexer,
VectorIndexer}

代码 4‑28

  - 加载和解析数据文件，将其转换为DataFrame

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑29

  - 索引标签，增加元数据到标签列中，拟合所有数据集，在索引中包含所有标签

scala\> val labelIndexer = new
StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

labelIndexer: org.apache.spark.ml.feature.StringIndexerModel =
strIdx\_551d33dd5566

代码 4‑30

  - 自动识别分类特征和索引，设置maxCategories，具有\>4个不同值的特征被作为连续值处理

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_8e95643a494d

代码 ‑31

  - 分割数据为训练和测试（30%为测试数据）

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑32

  - 训练RandomForest模型.

scala\> val rf = new
RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)

rf: org.apache.spark.ml.classification.RandomForestClassifier =
rfc\_319e21d14e79

代码 4‑33

  - 将索引标签转换成原始标签

scala\> val labelConverter = new
IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

labelConverter: org.apache.spark.ml.feature.IndexToString =
idxToStr\_b8fdfba32f0e

代码 4‑34

  - 链接索引和树到一个管道中

scala\> val pipeline = new Pipeline().setStages(Array(labelIndexer,
featureIndexer, rf, labelConverter))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_745f2ee48c2b

代码 4‑35

  - 训练模型

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_745f2ee48c2b

代码 4‑36

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 6 more fields\]

代码 4‑37

  - 选择示例行显示

scala\> predictions.select("predictedLabel", "label",
"features").show(5)

\+--------------+-----+--------------------+

|predictedLabel|label| features|

\+--------------+-----+--------------------+

| 0.0| 0.0|(692,\[95,96,97,12...|

| 1.0| 0.0|(692,\[100,101,102...|

| 0.0| 0.0|(692,\[121,122,123...|

| 0.0| 0.0|(692,\[122,123,124...|

| 0.0| 0.0|(692,\[122,123,148...|

\+--------------+-----+--------------------+

only showing top 5 rows

代码 ‑38

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_3f0dda1404bf

scala\> val accuracy = evaluator.evaluate(predictions)

accuracy: Double = 0.9666666666666667

scala\> println("Test Error = " + (1.0 - accuracy))

Test Error = 0.033333333333333326

scala\> val rfModel =
model.stages(2).asInstanceOf\[RandomForestClassificationModel\]

rfModel:
org.apache.spark.ml.classification.RandomForestClassificationModel =
RandomForestClassificationModel (uid=rfc\_319e21d14e79) with 10 trees

scala\> println("Learned classification forest model:\\n" +
rfModel.toDebugString)

Learned classification forest model:

RandomForestClassificationModel (uid=rfc\_319e21d14e79) with 10 trees

Tree 0 (weight 1.0):

If (feature 552 \<= 0.0)

If (feature 550 \<= 43.0)

Predict: 0.0

Else (feature 550 \> 43.0)

Predict: 1.0

Else (feature 552 \> 0.0)

If (feature 605 \<= 0.0)

Predict: 0.0

Else (feature 605 \> 0.0)

Predict: 1.0

Tree 1 (weight 1.0):

If (feature 463 \<= 0.0)

If (feature 317 \<= 0.0)

If (feature 651 \<= 0.0)

Predict: 0.0

Else (feature 651 \> 0.0)

Predict: 1.0

Else (feature 317 \> 0.0)

Predict: 1.0

Else (feature 463 \> 0.0)

Predict: 0.0

……

代码 4‑39

#### 梯度提升树分类 

梯度提升树（GBTS）是利用决策树的合奏流行的分类和回归方法。有关更多信息，spark.ml实现可进一步在找到上GBTS部分。

以下示例以LibSVM格式加载数据集，将其分解为训练集和测试集，在第一个数据集上训练，然后在保留的测试集上进行评估。Spark使用两个特征转换器来准备数据；这些帮助索引标签和分类特征的类别，将元数据添加到决策树算法可以识别的DataFrame中。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import
org.apache.spark.ml.classification.{GBTClassificationModel,
GBTClassifier}

import org.apache.spark.ml.classification.{GBTClassificationModel,
GBTClassifier}

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

scala\> import org.apache.spark.ml.feature.{IndexToString,
StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.{IndexToString, StringIndexer,
VectorIndexer}

代码 ‑40

  - 加载和解析数据文件

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 ‑41

  - 索引标签，增加元数据到标签列，拟合整个数据集，在索引中包含所有的标签

scala\> val labelIndexer = new
StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

labelIndexer: org.apache.spark.ml.feature.StringIndexerModel =
strIdx\_4d85d1d41d81

代码 4‑42

  - 自动识别分类特征和索引，设置maxCategories，具有\>4个不同值的特征被作为连续值处理

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_31283371a256

代码 ‑43

  - 分割数据为训练和测试（30%为测试数据）

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑44

  - 训练GBT模型

scala\> val gbt = new
GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)

gbt: org.apache.spark.ml.classification.GBTClassifier =
gbtc\_928d4fc65752

代码 4‑45

  - 将索引标签转换成原始标签

scala\> val labelConverter = new
IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

labelConverter: org.apache.spark.ml.feature.IndexToString =
idxToStr\_0c8538c39ade

代码 ‑46

  - 连接索引和GBT到管道中

scala\> val pipeline = new Pipeline().setStages(Array(labelIndexer,
featureIndexer, gbt, labelConverter))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_014b373f021b

代码 4‑47

  - 训练模型

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_014b373f021b

代码 4‑48

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 6 more fields\]

代码 4‑49

  - 选择示例行显示

scala\> predictions.select("predictedLabel", "label",
"features").show(5)

\+--------------+-----+--------------------+

|predictedLabel|label| features|

\+--------------+-----+--------------------+

| 0.0| 0.0|(692,\[122,123,124...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.0| 0.0|(692,\[124,125,126...|

| 0.0| 0.0|(692,\[126,127,128...|

| 0.0| 0.0|(692,\[150,151,152...|

\+--------------+-----+--------------------+

only showing top 5 rows

代码 4‑50

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_c2066bd15b35

scala\> val accuracy = evaluator.evaluate(predictions)

accuracy: Double = 1.0

scala\> println("Test Error = " + (1.0 - accuracy))

Test Error = 0.0

scala\> val gbtModel =
model.stages(2).asInstanceOf\[GBTClassificationModel\]

gbtModel: org.apache.spark.ml.classification.GBTClassificationModel =
GBTClassificationModel (uid=gbtc\_928d4fc65752) with 10 trees

scala\> println("Learned classification GBT model:\\n" +
gbtModel.toDebugString)

Learned classification GBT model:

GBTClassificationModel (uid=gbtc\_928d4fc65752) with 10 trees

Tree 0 (weight 1.0):

If (feature 434 \<= 0.0)

If (feature 99 in {2.0})

Predict: -1.0

Else (feature 99 not in {2.0})

Predict: 1.0

Else (feature 434 \> 0.0)

Predict: -1.0

Tree 1 (weight 0.1):

If (feature 434 \<= 0.0)

If (feature 545 \<= 222.0)

Predict: 0.47681168808847

Else (feature 545 \> 222.0)

Predict: -0.4768116880884712

Else (feature 434 \> 0.0)

If (feature 459 \<= 151.0)

Predict: -0.4768116880884701

Else (feature 459 \> 151.0)

Predict: -0.4768116880884712

……

代码 4‑51

#### 多层感知分类 

多层感知器分类器（Multilayer Perceptron
Classifier，MLPC）是基于前馈神经网络上的分类器的。MLPC由节点的多个层组成，每个层被完全连接到网络中的下一层，在输入层节点表示的输入数据。所有其他节点通过带有节点的权重$$\mathbf{w}$$和偏置$$\mathbf{b}$$的输入线性组合映射输入到输出。这可以将$$K + 1$$层的MLPC写成矩阵形式，如下：

$$y(\mathbf{x}) = f_{K}(...f_{2}(\mathbf{w}_{2}^{T}f_{1}(\mathbf{w}_{1}^{T}\mathbf{x} + b_{1}) + b_{2})... + b_{K})$$

公式 4‑6

在中间层节点使用sigmoid(logistic)函数：

$$f(z_{i}) = \frac{1}{1 + e^{- z_{i}}}$$

公式 4‑7

节点在输出层使用softmax功能：

$$f(z_{i}) = \frac{e^{z_{i}}}{\sum_{k = 1}^{N}e^{z_{k}}}$$

公式 4‑8

$$N$$节点的数量在输出层中对应于类的数量。

MLPC采用反向传播学习模型。Spark使用逻辑损失函数进行优化和L-BFGS作为优化程序。

scala\> import
org.apache.spark.ml.classification.MultilayerPerceptronClassifier

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

代码 4‑52

  - 加载和解析数据文件

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_multiclass\_classification\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑53

  - 分割数据为训练和测试

scala\> val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)

splits:
Array\[org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\]\] =
Array(\[label: double, features: vector\], \[label: double, features:
vector\])

scala\> val train = splits(0)

train: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

scala\> val test = splits(1)

test: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑54

  - 指定神经网络层，大小为4的输入层，两个大小为5和4的中间层，大小为3的输出层

scala\> val layers = Array\[Int\](4, 5, 4, 3)

layers: Array\[Int\] = Array(4, 5, 4, 3)

代码 4‑55

  - 创建训练器，设置参数

scala\> val trainer = new
MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

trainer:
org.apache.spark.ml.classification.MultilayerPerceptronClassifier =
mlpc\_66690265bc2c

代码 4‑56

  - 训练模型

scala\> val model = trainer.fit(train)

model:
org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
= mlpc\_66690265bc2c

代码 4‑57

  - 在测试集上计算精度

scala\> val result = model.transform(test)

result: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 1 more field\]

scala\> val predictionAndLabels = result.select("prediction", "label")

predictionAndLabels: org.apache.spark.sql.DataFrame = \[prediction:
double, label: double\]

scala\> val evaluator = new
MulticlassClassificationEvaluator().setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_5e61181600fd

scala\> println("Test set accuracy = " +
evaluator.evaluate(predictionAndLabels))

Test set accuracy = 0.8627450980392157

代码 4‑58

#### 线性支持向量机

支持向量机在高维或无限维空间中构建一个超平面或超平面集合，该空间可用于分类、回归或其他任务。直观地，一个良好的分离是由超平面完成，其具有到任何类的最接近训练数据点的最大距离，所谓的功能余量。因为通常余量越大，分类器的泛化误差越低。LinearSVC在Spark
ML中支持具有线性支持向量机的二元分类，使用OWLQN优化器优化了铰链损耗。

scala\> import org.apache.spark.ml.classification.LinearSVC

import org.apache.spark.ml.classification.LinearSVC

代码 ‑59

  - 加载训练数据

scala\> val training =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

training: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

scala\> val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1)

lsvc: org.apache.spark.ml.classification.LinearSVC =
linearsvc\_3534f59606d8

代码 ‑60

  - 拟合模型

scala\> val lsvcModel = lsvc.fit(training)

lsvcModel: org.apache.spark.ml.classification.LinearSVCModel =
linearsvc\_3534f59606d8

代码 4‑61

  - 打印线性SVC的系数和截距

scala\> println(s"Coefficients: ${lsvcModel.coefficients} Intercept:
${lsvcModel.intercept}")

Coefficients:
\[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,-5.170630317473439E-4,-1.172288654973735E-4,-8.882754836918948E-5,8.522360710187464E-5,0.0,0.0,-1.3436361263314267E-5,3.729569801338091E-4,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,8.888949552633658E-4,2.9864059761812683E-4,3.793378816193159E-4,-1.762328898254081E-4,0.0,1.5028489269747836E-6,1.8056041144946687E-6,1.8028763260398597E-6,-3.3843713506473646E-6,-4.041580184807502E-6,2.0965017727015125E-6,8.536111642989494E-5,2.2064177429604464E-4,2.1677599940575452E-4,-5.472401396558763E-4,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,9.21415502407147E-4,3.1351066886882195E-4,2.481984318412822E-4,0.0,-4.147738197636148E-5,-3.6832150384497175E-5,0.0,-3.9652366184583814E-6,-5.1569169804965594E-5,-6.624697287084958E-5,-2.182148650424713E-5,1.163442969067449E-5,-1.1535211416971104E-6,3.8138960488857075E-5,1.5823711634321492E-6,-4.784013432336632E-5,-9.386493224111833E-5,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,4.3174897827077767E-4,1.7055492867397665E-4,0.0,-2.7978204136148868E-5,-5.88745220385208E-5,-4.1858794529775E-5,-3.740692964881002E-5,-3.9787939304887E-5,-5.545881895011037E-5,-4.505015598421474E-5,-3.214002494749943E-6,-1.6561868808274739E-6,-4.416063987619447E-6,-7.9986183315327E-6,-4.729962112535003E-5,-2.516595625914463E-5,-3……

代码 4‑62

#### One-vs-Rest分类（又名One-vs-All）

OneVsRest将一个给定的二分类算法有效地扩展到多分类问题应用中，也被称为“One-vs-All”。OneVsRest被实现为一个估算器，其获得分类器实例，并且为多分类的每个创建二元分类问题，第i类的分类器被训练来预测标签是否是i或不是，区分i类与所有其他。预测是通过评估每个二元分类进行，将置信度最高的分类索引作为标签输出。下面的例子演示如何加载鸢尾属植物数据集，解析为一个DataFrame，并使用执行多类分类OneVsRest。测试误差被计算，测量算法的精确度。

scala\> import org.apache.spark.ml.classification.{LogisticRegression,
OneVsRest}

import org.apache.spark.ml.classification.{LogisticRegression,
OneVsRest}

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

代码 4‑63

  - 加载数据文件

scala\> val inputData =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_multiclass\_classification\_data.txt")

inputData: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑64

  - 产生训练和测试数据

scala\> val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))

train: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

test: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑65

  - 初始化基本分类器

scala\> val classifier = new
LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true)

classifier: org.apache.spark.ml.classification.LogisticRegression =
logreg\_4ab3ec576ece

代码 4‑66

  - 初始化One Vs Rest分类器

scala\> val ovr = new OneVsRest().setClassifier(classifier)

ovr: org.apache.spark.ml.classification.OneVsRest =
oneVsRest\_18cdbd6163d0

代码 4‑67

  - 训练多分类模型

scala\> val ovrModel = ovr.fit(train)

ovrModel: org.apache.spark.ml.classification.OneVsRestModel =
oneVsRest\_18cdbd6163d0

  - 在测试数据上为模型打分

scala\> val predictions = ovrModel.transform(test)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 1 more field\]

代码 4‑68

  - 获得评估器

scala\> val evaluator = new
MulticlassClassificationEvaluator().setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_d0b6ab78ccdb

代码 4‑69

  - 在测试数据上计算分类错误

scala\> val accuracy = evaluator.evaluate(predictions)

accuracy: Double = 0.896551724137931

scala\> println(s"Test Error = ${1 - accuracy}")

Test Error = 0.10344827586206895

代码 4‑70

#### 朴素贝叶斯 

朴素贝叶斯分类器是一个概率分类器的家族，基于特征之间的强独立性假定应用贝叶斯定理，spark.ml实现目前支持多元朴素贝叶斯和伯努利朴素贝叶斯。

scala\> import org.apache.spark.ml.classification.NaiveBayes

import org.apache.spark.ml.classification.NaiveBayes

scala\> import
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

代码 4‑71

  - 加载格式为LIBSVM的数据到DataFrame中

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑72

  - 分割数据为训练和测试（30%为测试数据）

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3), seed = 1234L)

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑73

  - 训练NaiveBayes模型

scala\> val model = new NaiveBayes().fit(trainingData)

model: org.apache.spark.ml.classification.NaiveBayesModel =
NaiveBayesModel (uid=nb\_cce70f29bc75) with 2 classes

代码 4‑74

  - 选择示例行显示

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 3 more fields\]

scala\> predictions.show()

\+-----+--------------------+--------------------+-----------+----------+

|label| features| rawPrediction|probability|prediction|

\+-----+--------------------+--------------------+-----------+----------+

| 0.0|(692,\[95,96,97,12...|\[-173678.60946628...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[98,99,100,1...|\[-178107.24302988...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[100,101,102...|\[-100020.80519087...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[124,125,126...|\[-183521.85526462...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[127,128,129...|\[-183004.12461660...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[128,129,130...|\[-246722.96394714...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[152,153,154...|\[-208696.01108598...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[153,154,155...|\[-261509.59951302...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[154,155,156...|\[-217654.71748256...| \[1.0,0.0\]| 0.0|

| 0.0|(692,\[181,182,183...|\[-155287.07585335...| \[1.0,0.0\]| 0.0|

| 1.0|(692,\[99,100,101,...|\[-145981.83877498...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[100,101,102...|\[-147685.13694275...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[123,124,125...|\[-139521.98499849...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[124,125,126...|\[-129375.46702012...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[126,127,128...|\[-145809.08230799...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[127,128,129...|\[-132670.15737290...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[128,129,130...|\[-100206.72054749...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[129,130,131...|\[-129639.09694930...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[129,130,131...|\[-143628.65574273...| \[0.0,1.0\]| 1.0|

| 1.0|(692,\[129,130,131...|\[-129238.74023248...| \[0.0,1.0\]| 1.0|

\+-----+--------------------+--------------------+-----------+----------+

only showing top 20 rows

scala\> // Select (prediction, true label) and compute test error

scala\> val evaluator = new
MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

evaluator:
org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator =
mcEval\_0a46fb9d7c4b

scala\> val accuracy = evaluator.evaluate(predictions)

accuracy: Double = 1.0

scala\> println("Test set accuracy = " + accuracy)

Test set accuracy = 1.0

代码 4‑75

### 回归

#### 线性回归 

使用线性回归模型和模型总结工作的接口类似于逻辑回归的情况，下面的例子演示了训练弹性网络正规化线性回归模型，提取模型汇总统计。

scala\> import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.regression.LinearRegression

代码 4‑76

  - 加载训练数据

scala\> val training =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_linear\_regression\_data.txt")

training: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

scala\> val lr = new
LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

lr: org.apache.spark.ml.regression.LinearRegression =
linReg\_d95b427bfd2c

代码 4‑77

  - 拟合模型

scala\> val lrModel = lr.fit(training)

lrModel: org.apache.spark.ml.regression.LinearRegressionModel =
linReg\_d95b427bfd2c

代码 4‑78

  - 输出线性回归的系数和截距

scala\> println(s"Coefficients: ${lrModel.coefficients} Intercept:
${lrModel.intercept}")

Coefficients:
\[0.0,0.32292516677405936,-0.3438548034562218,1.9156017023458414,0.05288058680386263,0.765962720459771,0.0,-0.15105392669186682,-0.21587930360904642,0.22025369188813426\]
Intercept: 0.1598936844239736

代码 4‑79

  - 在训练集合上总结模型，输出一些度量

scala\> val trainingSummary = lrModel.summary

trainingSummary:
org.apache.spark.ml.regression.LinearRegressionTrainingSummary =
org.apache.spark.ml.regression.LinearRegressionTrainingSummary@75078835

scala\> println(s"numIterations: ${trainingSummary.totalIterations}")

numIterations: 7

scala\> println(s"objectiveHistory:
\[${trainingSummary.objectiveHistory.mkString(",")}\]")

objectiveHistory:
\[0.49999999999999994,0.4967620357443381,0.4936361664340463,0.4936351537897608,0.4936351214177871,0.49363512062528014,0.4936351206216114\]

scala\> trainingSummary.residuals.show()

\+--------------------+

| residuals|

\+--------------------+

| -9.889232683103197|

| 0.5533794340053554|

| -5.204019455758823|

| -20.566686715507508|

| -9.4497405180564|

| -6.909112502719486|

| -10.00431602969873|

| 2.062397807050484|

| 3.1117508432954772|

| -15.893608229419382|

| -5.036284254673026|

| 6.483215876994333|

| 12.429497299109002|

| -20.32003219007654|

| -2.0049838218725005|

| -17.867901734183793|

| 7.646455887420495|

| -2.2653482182417406|

|-0.10308920436195645|

| -1.380034070385301|

\+--------------------+

only showing top 20 rows

scala\> println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")

RMSE: 10.189077167598475

scala\> println(s"r2: ${trainingSummary.r2}")

r2: 0.022861466913958184

代码 4‑80

#### 广义线性回归 

相比较线性回归假设输出遵循高斯分布，广义线性模型是线性模型的特例，其中响应变量$$Y_{i}$$遵循的分布来自指数家族分布。Spark的GeneralizedLinearRegression接口允许灵活指定广义线性模型，可用于各种类型的预测问题，包括线性回归、泊松回归、逻辑回归等。目前在spark.ml中，只支持指数系列分布的一个子集。目前Spark最多只支持到4096特征，通过GeneralizedLinearRegression接口，如果超过这个限制会抛出异常。尽管如此，对于线性和逻辑回归，随着特征的增多模型可以用LinearRegression和LogisticRegression估计器训练。广义线性模型需要指数系列分布，能够以他的“经典”或“自然”的形式写成，又称为自然指数系列分布，自然指数系列分布的形式如下：

$$
f_{Y}(y|\theta,\tau) = h(y,\tau)\exp\!\left(\frac{\theta \cdot y - A(\theta)}{d(\tau)}\right)
$$

公式 4‑9

其中$$\theta$$是需要估算的参数，$$\tau$$是离散参数。在广义线性模型中，响应变量$$Y_{i}$$被假定为从自然指数族分布中得出：

$$
Y_i \sim f(\cdot \mid \theta_i,\tau)
$$

公式 4‑10

其中估算参数$$\theta_{i}$$与响应变量$$\mu_{i}$$的期望值相关

$$
\mu_i = A'(\theta_i)
$$

公式 4‑11

这里，$$A'(\theta_i)$$由所选分布的形式定义。广义线性模型还允许规定一个链接函数，该函数定义了响应变量$$\mu_i$$的期望值和所谓的线性预测器$$\eta_i$$之间的关系：

$$
g(\mu_i)=\eta_i=\mathbf{x}_i^T\boldsymbol{\beta}
$$

公式 4‑12

通常，链接函数被选择为使得$$A' = g^{-1}$$，其产生感兴趣参数$$\theta$$与线性预测器$$\eta$$之间的简化关系。在这种情况下，链接函数$$g(\mu)$$被认为是“规范”链接函数。

$$
\theta_i = A'^{-1}(\mu_i) = g\!\left(g^{-1}(\eta_i)\right)=\eta_i
$$

公式 4‑13

广义线性模型找到回归系数$$\overset{\rightarrow}{\beta}$$最大似然函数。

$$
\max_{\boldsymbol{\beta}} \mathcal{L}(\boldsymbol{\theta}\mid\mathbf{y},X)
= \prod_{i=1}^{N} h(y_i,\tau)\exp\!\left(\frac{y_i\theta_i - A(\theta_i)}{d(\tau)}\right)
$$

公式 4‑14

其中估算参数$$\theta_{i}$$与回归系数$$\overset{\rightarrow}{\beta}$$有关

$$
\theta_i = A'^{-1}\!\left(g^{-1}(\mathbf{x}_i \cdot \boldsymbol{\beta})\right)
$$

公式 4‑15

Spark的广义线性回归接口还提供了汇总统计诊断广义线性模型模型拟合，包括残差、p值、偏差、赤池信息量准则（Akaike Information
Criterion）和其他。

  - 可用系列

| 系列       | 响应方式                     | 被支持的连接                   |
| -------- | ------------------------ | ------------------------ |
| Gaussian | Continuous               | Identity\*, Log, Inverse |
| Binomial | Binary                   | Logit\*, Probit, CLogLog |
| Poisson  | Count                    | Log\*, Identity, Sqrt    |
| Gamma    | Continuous               | Inverse\*, Idenity, Log  |
| Tweedie  | Zero-inflated continuous | Power link function      |
|          |                          |                          |

表格 4‑9可用系列

下面的例子演示了训练广义线性模型与高斯响应，标识链接功能和提取模型汇总统计。

scala\> import
org.apache.spark.ml.regression.GeneralizedLinearRegression

import org.apache.spark.ml.regression.GeneralizedLinearRegression

代码 4‑81

  - 加载训练数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_linear\_regression\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

scala\> val glr = new
GeneralizedLinearRegression().setFamily("gaussian").setLink("identity").setMaxIter(10).setRegParam(0.3)

glr: org.apache.spark.ml.regression.GeneralizedLinearRegression =
glm\_57277c689abf

代码 4‑82

  - 拟合模型

scala\> val model = glr.fit(dataset)

model: org.apache.spark.ml.regression.GeneralizedLinearRegressionModel =
glm\_57277c689abf

代码 4‑83

  - 输出广义线性回归的系数和截距

scala\> println(s"Coefficients: ${model.coefficients}")

Coefficients:
\[0.010541828081257216,0.8003253100560949,-0.7845165541420371,2.3679887171421914,0.5010002089857577,1.1222351159753026,-0.2926824398623296,-0.49837174323213035,-0.6035797180675657,0.6725550067187461\]

scala\> println(s"Intercept: ${model.intercept}")

Intercept: 0.14592176145232041

代码 4‑84

  - 在训练集上总结模型和输出一些度量

scala\> val summary = model.summary

summary:
org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary
=
org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary@31143d5b

scala\> println(s"Coefficient Standard Errors:
${summary.coefficientStandardErrors.mkString(",")}")

Coefficient Standard Errors:
0.7950428434287478,0.8049713176546897,0.7975916824772489,0.8312649247659919,0.7945436200517938,0.8118992572197593,0.7919506385542777,0.7973378214726764,0.8300714999626418,0.7771333489686802,0.463930109648428

scala\> println(s"T Values: ${summary.tValues.mkString(",")}")

T Values:
0.013259446542269243,0.9942283563442594,-0.9836067393599172,2.848657084633759,0.6305509179635714,1.382234441029355,-0.3695715687490668,-0.6250446546128238,-0.7271418403049983,0.8654306337661122,0.31453393176593286

scala\> println(s"P Values: ${summary.pValues.mkString(",")}")

P Values:
0.989426199114056,0.32060241580811044,0.3257943227369877,0.004575078538306521,0.5286281628105467,0.16752945248679119,0.7118614002322872,0.5322327097421431,0.467486325282384,0.3872259825794293,0.753249430501097

scala\> println(s"Dispersion: ${summary.dispersion}")

Dispersion: 105.60988356821714

scala\> println(s"Null Deviance: ${summary.nullDeviance}")

Null Deviance: 53229.3654338832

scala\> println(s"Residual Degree Of Freedom Null:
${summary.residualDegreeOfFreedomNull}")

Residual Degree Of Freedom Null: 500

scala\> println(s"Deviance: ${summary.deviance}")

Deviance: 51748.8429484264

scala\> println(s"Residual Degree Of Freedom:
${summary.residualDegreeOfFreedom}")

Residual Degree Of Freedom: 490

scala\> println(s"AIC: ${summary.aic}")

AIC: 3769.1895871765314

scala\> println("Deviance Residuals: ")

Deviance Residuals:

scala\> summary.residuals().show()

\+-------------------+

| devianceResiduals|

\+-------------------+

|-10.974359174246889|

| 0.8872320138420559|

| -4.596541837478908|

|-20.411667435019638|

|-10.270419345342642|

|-6.0156058956799905|

|-10.663939415849267|

| 2.1153960525024713|

| 3.9807132379137675|

|-17.225218272069533|

| -4.611647633532147|

| 6.4176669407698546|

| 11.407137945300537|

| -20.70176540467664|

| -2.683748540510967|

|-16.755494794232536|

| 8.154668342638725|

|-1.4355057987358848|

|-0.6435058688185704|

| -1.13802589316832|

\+-------------------+

only showing top 20 rows

代码 4‑85

  - 赤池信息量准则

是衡量统计模型拟合优良性的一种标准，是由日本统计学家赤池弘次创立和发展的。赤池信息量准则建立在熵的概念基础上，可以权衡所估计模型的复杂度和此模型拟合数据的优良性。

#### 决策树回归 

决策树是受欢迎的分类和回归方法系列。有关更多信息，spark.ml实现可进一步在找到决策树节。下面例子中，加载LIBSVM格式的数据集，将其分成训练集和测试集，训练在第一数据集，然后在留存的测试集上评估。Spark使用特征转换器索引类别特征，添加元数据到该决策树算法可以识别的DataFrame。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.evaluation.RegressionEvaluator

scala\> import org.apache.spark.ml.feature.VectorIndexer

import org.apache.spark.ml.feature.VectorIndexer

scala\> import
org.apache.spark.ml.regression.DecisionTreeRegressionModel

import org.apache.spark.ml.regression.DecisionTreeRegressionModel

scala\> import org.apache.spark.ml.regression.DecisionTreeRegressor

import org.apache.spark.ml.regression.DecisionTreeRegressor

代码 4‑86

  - 加载格式为LIBSVM的数据到DataFrame中

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑87

  - 自动识别分类特征和索引

  - 将具有\>4不同值的特征作为连续的

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_22ea6e264a28

代码 4‑88

  - 分割数据为训练和测试（30%为测试数据）

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑89

  - 训练DecisionTree模型

scala\> val dt = new
DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")

dt: org.apache.spark.ml.regression.DecisionTreeRegressor =
dtr\_d8e21b9502e1

代码 4‑90

  - 链接索引和树到一个管道中

scala\> val pipeline = new Pipeline().setStages(Array(featureIndexer,
dt))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_c396dbb1f2f7

代码 4‑91

  - 训练模型，运行索引

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_c396dbb1f2f7

代码 4‑92

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 2 more fields\]

代码 4‑93

  - 选择示例行显示

scala\> predictions.select("prediction", "label", "features").show(5)

\+----------+-----+--------------------+

|prediction|label| features|

\+----------+-----+--------------------+

| 0.0| 0.0|(692,\[98,99,100,1...|

| 0.0| 0.0|(692,\[122,123,148...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.0| 0.0|(692,\[124,125,126...|

\+----------+-----+--------------------+

only showing top 5 rows

代码 4‑94

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

evaluator: org.apache.spark.ml.evaluation.RegressionEvaluator =
regEval\_5550a21b1674

scala\> val rmse = evaluator.evaluate(predictions)

rmse: Double = 0.19245008972987526

scala\> println("Root Mean Squared Error (RMSE) on test data = " + rmse)

Root Mean Squared Error (RMSE) on test data = 0.19245008972987526

scala\> val treeModel =
model.stages(1).asInstanceOf\[DecisionTreeRegressionModel\]

treeModel: org.apache.spark.ml.regression.DecisionTreeRegressionModel =
DecisionTreeRegressionModel (uid=dtr\_d8e21b9502e1) of depth 1 with 3
nodes

scala\> println("Learned regression tree model:\\n" +
treeModel.toDebugString)

Learned regression tree model:

DecisionTreeRegressionModel (uid=dtr\_d8e21b9502e1) of depth 1 with 3
nodes

If (feature 434 \<= 0.0)

Predict: 0.0

Else (feature 434 \> 0.0)

Predict: 1.0

代码 4‑95

#### 随机森林回归 

随机森林是受欢迎的分类和回归方法系列。下面例子中，加载LIBSVM格式的数据集，将其分成训练集和测试集，训练在第一数据集，然后在留存的测试集上评估。Spark使用特征转换器索引类别特征，添加元数据到该基于树算法可以识别的DataFrame。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.evaluation.RegressionEvaluator

scala\> import org.apache.spark.ml.feature.VectorIndexer

import org.apache.spark.ml.feature.VectorIndexer

scala\> import
org.apache.spark.ml.regression.{RandomForestRegressionModel,
RandomForestRegressor}

import org.apache.spark.ml.regression.{RandomForestRegressionModel,
RandomForestRegressor}

代码 4‑96

  - 加载和解析数据文件，将其转换为DataFrame

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑97

  - 自动识别分类特征，并且索引，设置maxCategories，将具有\>4不同值的特征作为连续的

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_8686fd13bc82

代码 4‑98

  - 分割数据到训练和测试集(30% 留存为测试)

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑99

  - 训练RandomForest模型

scala\> val rf = new
RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")

rf: org.apache.spark.ml.regression.RandomForestRegressor =
rfr\_8b3d97e58278

代码 4‑100

  - 链接索引和树到一个管道中

scala\> val pipeline = new Pipeline().setStages(Array(featureIndexer,
rf))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_a2a6e45d0f75

  - 训练模型，返回索引

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_a2a6e45d0f75

代码 4‑101

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 2 more fields\]

代码 4‑102

  - 选择示例行显示

scala\> predictions.select("prediction", "label", "features").show(5)

\+----------+-----+--------------------+

|prediction|label| features|

\+----------+-----+--------------------+

| 0.05| 0.0|(692,\[121,122,123...|

| 0.0| 0.0|(692,\[122,123,124...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.0| 0.0|(692,\[123,124,125...|

| 0.05| 0.0|(692,\[125,126,127...|

\+----------+-----+--------------------+

only showing top 5 rows

代码 4‑103

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

evaluator: org.apache.spark.ml.evaluation.RegressionEvaluator =
regEval\_ab1417ac176f

scala\> val rmse = evaluator.evaluate(predictions)

rmse: Double = 0.06565321642986129

scala\> println("Root Mean Squared Error (RMSE) on test data = " + rmse)

Root Mean Squared Error (RMSE) on test data = 0.06565321642986129

scala\> val rfModel =
model.stages(1).asInstanceOf\[RandomForestRegressionModel\]

rfModel: org.apache.spark.ml.regression.RandomForestRegressionModel =
RandomForestRegressionModel (uid=rfr\_8b3d97e58278) with 20 trees

scala\> println("Learned regression forest model:\\n" +
rfModel.toDebugString)

Learned regression forest model:

RandomForestRegressionModel (uid=rfr\_8b3d97e58278) with 20 trees

Tree 0 (weight 1.0):

If (feature 435 \<= 0.0)

If (feature 545 \<= 252.0)

Predict: 0.0

Else (feature 545 \> 252.0)

Predict: 1.0

Else (feature 435 \> 0.0)

Predict: 1.0

Tree 1 (weight 1.0):

If (feature 490 \<= 0.0)

Predict: 0.0

Else (feature 490 \> 0.0)

Predict: 1.0

Tree 2 (weight 1.0):

If (feature 290 \<= 0.0)

Predict: 1.0

Else (feature 290 \> 0.0)

……

代码 4‑104

#### 梯度推进树回归 

梯度提升树是流行的利用决策树集成的回归方法。在这个例子中数据集，GBTRegressor实际上只需要1次迭代，但一般不会。

scala\> import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.Pipeline

scala\> import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.evaluation.RegressionEvaluator

scala\> import org.apache.spark.ml.feature.VectorIndexer

import org.apache.spark.ml.feature.VectorIndexer

scala\> import org.apache.spark.ml.regression.{GBTRegressionModel,
GBTRegressor}

import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

代码 4‑105

  - 加载和解析数据文件，将其转换为DataFrame

scala\> val data =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

data: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑106

  - 自动识别分类特征，并且索引，设置maxCategories，将具有\>4不同值的特征作为连续的

scala\> val featureIndexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_2dc5f8c212c1

代码 4‑107

  - 分割数据到训练和测试集(30% 留存为测试).

scala\> val Array(trainingData, testData) = data.randomSplit(Array(0.7,
0.3))

trainingData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

testData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[label: double, features: vector\]

代码 4‑108

  - 训练梯度提升树模型

scala\> val gbt = new
GBTRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxIter(10)

gbt: org.apache.spark.ml.regression.GBTRegressor = gbtr\_307ad34e9fcd

代码 4‑109

  - 在管道中链接索引器和梯度提升树

scala\> val pipeline = new Pipeline().setStages(Array(featureIndexer,
gbt))

pipeline: org.apache.spark.ml.Pipeline = pipeline\_9462b17b4b09

代码 4‑110

  - 训练模型，运行索引器

scala\> val model = pipeline.fit(trainingData)

model: org.apache.spark.ml.PipelineModel = pipeline\_9462b17b4b09

代码 4‑111

  - 进行预测

scala\> val predictions = model.transform(testData)

predictions: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 2 more fields\]

代码 4‑112

  - 选择示例行显示

scala\> predictions.select("prediction", "label", "features").show(5)

\+----------+-----+--------------------+

|prediction|label| features|

\+----------+-----+--------------------+

| 0.0| 0.0|(692,\[95,96,97,12...|

| 0.0| 0.0|(692,\[122,123,148...|

| 0.0| 0.0|(692,\[124,125,126...|

| 0.0| 0.0|(692,\[124,125,126...|

| 0.0| 0.0|(692,\[126,127,128...|

\+----------+-----+--------------------+

only showing top 5 rows

代码 4‑113

  - 选择预测和真标签，计算测试错误

scala\> val evaluator = new
RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

evaluator: org.apache.spark.ml.evaluation.RegressionEvaluator =
regEval\_f3a7f3b952b3

scala\> val rmse = evaluator.evaluate(predictions)

rmse: Double = 0.0

scala\> println("Root Mean Squared Error (RMSE) on test data = " + rmse)

Root Mean Squared Error (RMSE) on test data = 0.0

scala\> val gbtModel =
model.stages(1).asInstanceOf\[GBTRegressionModel\]

gbtModel: org.apache.spark.ml.regression.GBTRegressionModel =
GBTRegressionModel (uid=gbtr\_307ad34e9fcd) with 10 trees

scala\> println("Learned regression GBT model:\\n" +
gbtModel.toDebugString)

Learned regression GBT model:

GBTRegressionModel (uid=gbtr\_307ad34e9fcd) with 10 trees

Tree 0 (weight 1.0):

If (feature 434 \<= 0.0)

If (feature 99 in {0.0,3.0})

Predict: 0.0

Else (feature 99 not in {0.0,3.0})

Predict: 1.0

Else (feature 434 \> 0.0)

Predict: 1.0

Tree 1 (weight 0.1):

Predict: 0.0

Tree 2 (weight 0.1):

Predict: 0.0

Tree 3 (weight 0.1):

Predict: 0.0

Tree 4 (weight 0.1):

Predict: 0.0

Tree 5 (weight 0.1):

Predict: 0.0

Tree 6 (weight 0.1):

Predict: 0.0

Tree 7 (weight 0.1):

Predict: 0.0

Tree 8 (weight 0.1):

Predict: 0.0

Tree 9 (weight 0.1):

Predict: 0.0

代码 4‑114

#### 生存回归 

在spark.ml中，我们实现了加速失败时间（Accelerated Failure
Time，AFT）模型，该模型是用于审查数据的参数生存回归模型。它描述了对数存活时间的模型，所以它通常被称为生存分析对数线性模型。不同与同一个目的的比例风险模型设计，AFT模型更容易并行化，因为每个实例独立地作为目标函数。对于受试者i
= 1，...，n的随机生命期$$t_i$$，假定给定协变量$$\mathbf{x}_i$$值带有可能的右截尾，AFT模型下的似然函数为：

$$
L(\beta,\sigma)=\prod_{i=1}^{n}
\left[\frac{1}{\sigma}f_0\!\left(\frac{\log t_i-\mathbf{x}_i^T\beta}{\sigma}\right)\right]^{\delta_i}
S_0\!\left(\frac{\log t_i-\mathbf{x}_i^T\beta}{\sigma}\right)^{1-\delta_i}
$$

公式 4‑16

其中$$\delta_i$$是发生事件的指标（未删失）。使用$$\epsilon_i=\frac{\log t_i-\mathbf{x}_i^T\beta}{\sigma}$$，对数似然函数为：

$$
\iota(\beta,\sigma)=\sum_{i=1}^{n}
\left[-\delta_i\log\sigma+\delta_i\log f_0(\epsilon_i)+(1-\delta_i)\log S_0(\epsilon_i)\right]
$$

公式 4‑17

其中$$S_{0}(\epsilon_{i})$$是基线幸存函数，$$f_{0}(\epsilon_{i})$$是相应的密度函数。

最常用的AFT模型是基于生存时间的威布尔分布，寿命威布尔分布对应于寿命对数的极值分布，$$S_{0}(\epsilon)$$函数为：

$$S_{0}(\epsilon_{i}) = \exp( - e^{\epsilon_{i}})$$

公式 4‑18

$$f_{0}(\epsilon_{i})$$函数是：

$$f_{0}(\epsilon_{i}) = e^{\epsilon_{i}}\exp( - e^{\epsilon_{i}})$$

公式 4‑19

具有威布尔寿命分布的AFT模型的对数似然函数为：

$$
\iota(\beta,\sigma)=- \sum_{i=1}^{n}\left[\delta_i\log\sigma - \delta_i\epsilon_i + e^{\epsilon_i}\right]
$$

公式 4‑20

由于最小化等价于最大后验概率的负对数似然度，Spark用来优化的损失函数是$$- \iota(\beta,\sigma)$$。$$\beta$$和$$\log\sigma$$的梯度函数分别为：

$$
\frac{\partial(-\iota)}{\partial\beta}
=\sum_{i=1}^{n}\left[\delta_i - e^{\epsilon_i}\right]\frac{\mathbf{x}_i}{\sigma}
$$

公式 4‑21

$$
\frac{\partial(-\iota)}{\partial(\log\sigma)}
=\sum_{i=1}^{n}\left[\delta_i + (\delta_i - e^{\epsilon_i})\epsilon_i\right]
$$

公式 4‑22

AFT模型可以被表述为凸优化问题，即根据系数向量$$\beta$$和尺度参数对数$$\log\sigma$$寻找凸函数$$- \iota(\beta,\sigma)$$最小值的任务。底层实现的优化算法是L-BFGS。该实现与R的生存函数survreg的结果相匹配。

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> import org.apache.spark.ml.regression.AFTSurvivalRegression

import org.apache.spark.ml.regression.AFTSurvivalRegression

scala\> val training = spark.createDataFrame(Seq(

| (1.218, 1.0, Vectors.dense(1.560, -0.605)),

| (2.949, 0.0, Vectors.dense(0.346, 2.158)),

| (3.627, 0.0, Vectors.dense(1.380, 0.231)),

| (0.273, 1.0, Vectors.dense(0.520, 1.151)),

| (4.199, 0.0, Vectors.dense(0.795, -0.226))

| )).toDF("label", "censor", "features")

training: org.apache.spark.sql.DataFrame = \[label: double, censor:
double ... 1 more field\]

scala\> val quantileProbabilities = Array(0.3, 0.6)

quantileProbabilities: Array\[Double\] = Array(0.3, 0.6)

scala\> val aft = new
AFTSurvivalRegression().setQuantileProbabilities(quantileProbabilities).setQuantilesCol("quantiles")

aft: org.apache.spark.ml.regression.AFTSurvivalRegression =
aftSurvReg\_e19697fe4c07

scala\> val model = aft.fit(training)

model: org.apache.spark.ml.regression.AFTSurvivalRegressionModel =
aftSurvReg\_e19697fe4c07

scala\> // Print the coefficients, intercept and scale parameter for AFT
survival regression

scala\> println(s"Coefficients: ${model.coefficients}")

Coefficients: \[-0.49630441105311934,0.19845217252922745\]

scala\> println(s"Intercept: ${model.intercept}")

Intercept: 2.638089896305637

scala\> println(s"Scale: ${model.scale}")

Scale: 1.5472363533632303

scala\> model.transform(training).show(false)

\+-----+------+--------------+------------------+---------------------------------------+

|label|censor|features |prediction |quantiles |

\+-----+------+--------------+------------------+---------------------------------------+

|1.218|1.0 |\[1.56,-0.605\] |5.718985621018948
|\[1.160322990805951,4.99546058340675\] |

|2.949|0.0 |\[0.346,2.158\] |18.07678210850554
|\[3.6675919944963185,15.789837303662035\]|

|3.627|0.0 |\[1.38,0.231\] |7.381908879359957
|\[1.4977129086101564,6.448002719505488\] |

|0.273|1.0 |\[0.52,1.151\]
|13.577717814884515|\[2.754778414791514,11.859962351993207\] |

|4.199|0.0 |\[0.795,-0.226\]|9.013087597344821
|\[1.82866218773319,7.8728164067854935\] |

\+-----+------+--------------+------------------+---------------------------------------+

代码 4‑115

#### 保序回归 

保序回归属于回归算法系列。正式保序回归是一个问题，其中给定的有限实数集合$$Y = y_{1},y_{2},...,y_{n}$$代表观察到的响应和$$X = x_{1},x_{2},...,x_{n}$$未知响应值被拟合发现最小化的函数：

$$
f(x)=\sum_{i=1}^{n} w_i (y_i-x_i)^2
$$

公式 4‑23

相对于完全顺序受试者$$x_{1} \leq x_{2} \leq ... \leq x_{n}$$，$$w_{i}$$是正权重。生成的函数被称为保序回归，它是独一无二的。它可以根据顺序限制被视为最小二乘问题。从本质上讲保序回归是一个单调函数，其最佳拟合原始数据点。

Spark实现了一个池相邻违法者算法，这是一种方法来并行保序回归。训练输入是包含标签、特征和权重三列的DataFrame。另外IsotonicRegression算法有一个可选的参数叫做$$\text{isotonic}$$，默认为true。这个参数指出保序回归是保序（单调增加）还是反向（单调递减）。

训练会返回一个IsotonicRegressionModel，可用于预测已知和未知的特征标签。保序回归的结果被处理为分段线性函数。因此用于预测的规则是：

（1）如果预测的输入准确匹配训练特征，那么相关的预测被返回。如果相同的特征具有多个预测，则返回他中的一个返回。哪一个是不确定的（同java.util.Arrays.binarySearch）。

（2）如果预测的输入比所有的训练特征低或高，则最低或最高特征的预测分别返回。如果有具有相同特征的多个预测，则最低或最高分别返回。

（3）如果预测的输入落在两个训练特征之间，则预测将被视为分段线性函数，并且内插的值是从最近的两个特征预测计算。在情况下，存在相同的特征具有多个值，那么与先前相同的规则被用。

scala\> import org.apache.spark.ml.regression.IsotonicRegression

import org.apache.spark.ml.regression.IsotonicRegression

代码 4‑116

  - 加载数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_isotonic\_regression\_libsvm\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑117

  - 训练保序回归模型

scala\> val ir = new IsotonicRegression()

ir: org.apache.spark.ml.regression.IsotonicRegression =
isoReg\_78794f99af75

scala\> val model = ir.fit(dataset)

model: org.apache.spark.ml.regression.IsotonicRegressionModel =
isoReg\_78794f99af75

scala\> println(s"Boundaries in increasing order:
${model.boundaries}\\n")

Boundaries in increasing order:
\[0.01,0.17,0.18,0.27,0.28,0.29,0.3,0.31,0.34,0.35,0.36,0.41,0.42,0.71,0.72,0.74,0.75,0.76,0.77,0.78,0.79,0.8,0.81,0.82,0.83,0.84,0.85,0.86,0.87,0.88,0.89,1.0\]

scala\> println(s"Predictions associated with the boundaries:
${model.predictions}\\n")

Predictions associated with the boundaries:
\[0.15715271294117644,0.15715271294117644,0.189138196,0.189138196,0.20040796,0.29576747,0.43396226,0.5081591025000001,0.5081591025000001,0.54156043,0.5504844466666667,0.5504844466666667,0.563929967,0.563929967,0.5660377366666667,0.5660377366666667,0.56603774,0.57929628,0.64762876,0.66241713,0.67210607,0.67210607,0.674655785,0.674655785,0.73890872,0.73992861,0.84242733,0.89673636,0.89673636,0.90719021,0.9272055075,0.9272055075\]

代码 4‑118

  - 进行预测

scala\> model.transform(dataset).show()

\+----------+--------------+-------------------+

| label| features| prediction|

\+----------+--------------+-------------------+

|0.24579296|(1,\[0\],\[0.01\])|0.15715271294117644|

|0.28505864|(1,\[0\],\[0.02\])|0.15715271294117644|

|0.31208567|(1,\[0\],\[0.03\])|0.15715271294117644|

|0.35900051|(1,\[0\],\[0.04\])|0.15715271294117644|

|0.35747068|(1,\[0\],\[0.05\])|0.15715271294117644|

|0.16675166|(1,\[0\],\[0.06\])|0.15715271294117644|

|0.17491076|(1,\[0\],\[0.07\])|0.15715271294117644|

| 0.0418154|(1,\[0\],\[0.08\])|0.15715271294117644|

|0.04793473|(1,\[0\],\[0.09\])|0.15715271294117644|

|0.03926568| (1,\[0\],\[0.1\])|0.15715271294117644|

|0.12952575|(1,\[0\],\[0.11\])|0.15715271294117644|

| 0.0|(1,\[0\],\[0.12\])|0.15715271294117644|

|0.01376849|(1,\[0\],\[0.13\])|0.15715271294117644|

|0.13105558|(1,\[0\],\[0.14\])|0.15715271294117644|

|0.08873024|(1,\[0\],\[0.15\])|0.15715271294117644|

|0.12595614|(1,\[0\],\[0.16\])|0.15715271294117644|

|0.15247323|(1,\[0\],\[0.17\])|0.15715271294117644|

|0.25956145|(1,\[0\],\[0.18\])| 0.189138196|

|0.20040796|(1,\[0\],\[0.19\])| 0.189138196|

|0.19581846| (1,\[0\],\[0.2\])| 0.189138196|

\+----------+--------------+-------------------+

only showing top 20 rows

代码 4‑119

## 聚集

### K均值

K均值（k-means）是最常用的聚类算法之一，它将数据点聚类成预定数量的聚类。MLlib实现包含一个名为kmeans||的k-means
++方法并行处理的变体。KMeans作为估算器实现，并生成KMeansModel作为基础模型。

#### 输入列

| 参数名称        | 类型     | 默认         | 描述   |
| ----------- | ------ | ---------- | ---- |
| featuresCol | Vector | "features" | 特这向量 |

表格 4‑10输入列

#### 输出列

| 参数名称          | 类型  | 默认           | 描述     |
| ------------- | --- | ------------ | ------ |
| predictionCol | Int | "prediction" | 预测聚集中心 |

表格 4‑11输出列

代码如下：

scala\> import org.apache.spark.ml.clustering.KMeans

import org.apache.spark.ml.clustering.KMeans

代码 4‑120

  - 加载数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_kmeans\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

  - 训练K均值模型

scala\> val kmeans = new KMeans().setK(2).setSeed(1L)

kmeans: org.apache.spark.ml.clustering.KMeans = kmeans\_1e6e4f712555

scala\> val model = kmeans.fit(dataset)

model: org.apache.spark.ml.clustering.KMeansModel = kmeans\_1e6e4f712555

代码 4‑121

  - 通过在平方误差的集合中计算评估聚类

scala\> val WSSSE = model.computeCost(dataset)

WSSSE: Double = 0.11999999999994547

scala\> println(s"Within Set Sum of Squared Errors = $WSSSE")

Within Set Sum of Squared Errors = 0.11999999999994547

代码 4‑122

  - 显示结果

scala\> println("Cluster Centers: ")

Cluster Centers:

scala\> model.clusterCenters.foreach(println)

\[0.1,0.1,0.1\]

\[9.1,9.1,9.1\]

代码 4‑123

### 潜在狄利克雷分配（LDA） 

LDA作为支持EMLDAOptimizer和OnlineLDAOptimizer的估算器实现，并生成LDAModel作为基础模型。如果需要，专家用户可以将由EMLDAOptimizer生成的LDAModel转换为DistributedLDAModel。

scala\> import org.apache.spark.ml.clustering.LDA

import org.apache.spark.ml.clustering.LDA

代码 4‑124

  - 加载数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_lda\_libsvm\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑125

  - 训练LDA模型

scala\> val lda = new LDA().setK(10).setMaxIter(10)

lda: org.apache.spark.ml.clustering.LDA = lda\_0c24c3ec3bab

scala\> val model = lda.fit(dataset)

model: org.apache.spark.ml.clustering.LDAModel = lda\_0c24c3ec3bab

scala\> val ll = model.logLikelihood(dataset)

ll: Double = -842.4862800491514

scala\> val lp = model.logPerplexity(dataset)

lp: Double = 3.2385774464305963

scala\> println(s"The lower bound on the log likelihood of the entire
corpus: $ll")

The lower bound on the log likelihood of the entire corpus:
-842.4862800491514

scala\> println(s"The upper bound on perplexity: $lp")

The upper bound on perplexity: 3.2385774464305963

代码 4‑126

  - 描述主题

scala\> val topics = model.describeTopics(3)

topics: org.apache.spark.sql.DataFrame = \[topic: int, termIndices:
array\<int\> ... 1 more field\]

scala\> println("The topics described by their top-weighted terms:")

The topics described by their top-weighted terms:

scala\> topics.show(false)

\+-----+-----------+---------------------------------------------------------------+

|topic|termIndices|termWeights |

\+-----+-----------+---------------------------------------------------------------+

|0 |\[2, 5, 7\] |\[0.10606441785146535, 0.10570106737280574,
0.1043039017910825\] |

|1 |\[1, 6, 2\] |\[0.10185078330694743, 0.09816924136544754,
0.09632455347714897\]|

|2 |\[1, 9, 4\] |\[0.10597705576734423, 0.09750947025544646,
0.09654669262128844\]|

|3 |\[0, 4, 8\] |\[0.102706983773961, 0.09842850171937594,
0.09815664111403606\] |

|4 |\[9, 6, 4\] |\[0.10452968226922606, 0.10414903762686416,
0.10103989135293562\]|

|5 |\[10, 6, 9\] |\[0.21878768117572409, 0.14074681597413502,
0.1276739943161934\] |

|6 |\[3, 7, 4\] |\[0.11638316021438284, 0.09901763897381445,
0.09795374111255549\]|

|7 |\[4, 0, 2\] |\[0.10855455833990776, 0.10334271299447938,
0.10034944281883779\]|

|8 |\[0, 7, 8\] |\[0.11008004098527444, 0.09919724236284332,
0.09810905351448598\]|

|9 |\[9, 6, 8\] |\[0.10106113658487842, 0.10013291564891967,
0.09769280655833748\]|

\+-----+-----------+---------------------------------------------------------------+

scala\>

代码 4‑127

  - 显示结果

scala\> val transformed = model.transform(dataset)

transformed: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 1 more field\]

scala\> transformed.show(false)

\+-----+---------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

|label|features |topicDistribution |

\+-----+---------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

|0.0 |(11,\[0,1,2,4,5,6,7,10\],\[1.0,2.0,6.0,2.0,3.0,1.0,1.0,3.0\])
|\[0.6990501464826979,0.004825989296507445,0.004825964734431469,0.004825959078413314,0.0048259276001261136,0.26234221206965436,0.004825955202628511,0.004826025945501356,0.004825893404537412,0.0048259261855020845\]
|

|1.0 |(11,\[0,1,3,4,7,10\],\[1.0,3.0,1.0,3.0,2.0,1.0\])
|\[0.008050728965819664,0.008050328998096084,0.008050336072632464,0.0080513660777465,0.008050221907908819,0.9275445084405548,0.008050734285468413,0.008050740924870691,0.008050657635177337,0.008050376691725378\]
|

……

代码 4‑128

  - LDA文档主题生成模型

LDA（Latent Dirichlet
Allocation）是一种文档主题生成模型，也称为一个三层贝叶斯概率模型，包含词、主题和文档三层结构。所谓生成模型，就是说，Spark认为一篇文章的每个词都是通过“以一定概率选择了某个主题，并从这个主题中以一定概率选择某个词语”这样一个过程得到。文档到主题服从多项式分布，主题到词服从多项式分布。\[1\]

LDA是一种非监督机器学习技术，可以用来识别大规模文档集（document
collection）或语料库（corpus）中潜藏的主题信息。它采用了词袋（bag
of
words）的方法，这种方法将每一篇文档视为一个词频向量，从而将文本信息转化为了易于建模的数字信息。但是词袋方法没有考虑词与词之间的顺序，这简化了问题的复杂性，同时也为模型的改进提供了契机。每一篇文档代表了一些主题所构成的一个概率分布，而每一个主题又代表了很多单词所构成的一个概率分布。

### 二分k均值

二分k均值是一种使用分裂（或“自上而下”）方法的分层聚类：所有观测都在一个聚类中开始，当一个分层向下移动时，分裂被递归地执行。二分k均值通常比常规K均值要快得多，但通常会产生不同的聚类。BisectingKMeans是作为估算器实现的，并生成一个BisectingKMeansModel作为基础模型。

scala\> import org.apache.spark.ml.clustering.BisectingKMeans

import org.apache.spark.ml.clustering.BisectingKMeans

代码 4‑129

  - 加载数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_kmeans\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑130

  - 训练二分k均值模型

scala\> val bkm = new BisectingKMeans().setK(2).setSeed(1)

bkm: org.apache.spark.ml.clustering.BisectingKMeans =
bisecting-kmeans\_bf550173fc8c

scala\> val model = bkm.fit(dataset)

model: org.apache.spark.ml.clustering.BisectingKMeansModel =
bisecting-kmeans\_bf550173fc8c

代码 4‑131

  - 评估聚集

scala\> val cost = model.computeCost(dataset)

cost: Double = 0.11999999999994547

scala\> println(s"Within Set Sum of Squared Errors = $cost")

Within Set Sum of Squared Errors = 0.11999999999994547

代码 4‑132

  - 显示结果

scala\> println("Cluster Centers: ")

Cluster Centers:

scala\> val centers = model.clusterCenters

centers: Array\[org.apache.spark.ml.linalg.Vector\] =
Array(\[0.1,0.1,0.1\], \[9.1,9.1,9.1\])

scala\> centers.foreach(println)

\[0.1,0.1,0.1\]

\[9.1,9.1,9.1\]

代码 4‑133

### 高斯混合模型

高斯混合模型表示复合分布，由此从k个高斯子分布中的一个绘制点，每个高斯子分布具有其自身的概率。spark.ml实现使用期望最大化算法在给定一组样本的情况下引发最大似然模型。GaussianMixture作为估算器实现，并生成一个GaussianMixtureModel作为基础模型。

#### 输入列 

| 参数名称        | 类型     | 默认         | 描述   |
| ----------- | ------ | ---------- | ---- |
| featuresCol | Vector | "features" | 特征向量 |

表格 4‑12输入列

#### 输出列 

| 参数名称           | 类型     | 默认            | **描述**  |
| -------------- | ------ | ------------- | ------- |
| predictionCol  | Int    | "prediction"  | 预测的聚集中心 |
| probabilityCol | Vector | "probability" | 每个群集的概率 |

表格 4‑13输出列

代码如下：

scala\> import org.apache.spark.ml.clustering.GaussianMixture

import org.apache.spark.ml.clustering.GaussianMixture

代码 4‑134

  - 加载数据

scala\> val dataset =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_kmeans\_data.txt")

dataset: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

代码 4‑135

  - 训练模型

scala\> val gmm = new GaussianMixture()

gmm: org.apache.spark.ml.clustering.GaussianMixture =
GaussianMixture\_2c319d8bb00b

scala\> .setK(2)

\<console\>:1: error: illegal start of definition

.setK(2)

^

scala\> val model = gmm.fit(dataset)

model: org.apache.spark.ml.clustering.GaussianMixtureModel =
GaussianMixture\_2c319d8bb00b

代码 4‑136

  - 混合模型的输出参数

scala\> for (i \<- 0 until model.getK) {

| println(s"Gaussian $$i:\\nweight=$${model.weights(i)}\\n" +

|
s"mu=$${model.gaussians(i).mean}\\nsigma=\\n$${model.gaussians(i).cov}\\n")

| }

Gaussian 0:

weight=0.5

mu=\[0.10000000000001552,0.10000000000001552,0.10000000000001552\]

sigma=

0.006666666666806454 0.006666666666806454 0.006666666666806454

0.006666666666806454 0.006666666666806454 0.006666666666806454

0.006666666666806454 0.006666666666806454 0.006666666666806454

Gaussian 1:

weight=0.5

mu=\[9.099999999999984,9.099999999999984,9.099999999999984\]

sigma=

0.006666666666812185 0.006666666666812185 0.006666666666812185

0.006666666666812185 0.006666666666812185 0.006666666666812185

0.006666666666812185 0.006666666666812185 0.006666666666812185

代码 4‑137

## 小结

Spark MLlib是Spark Core上的一个分布式机器学习框架，在很大程度上由于基于分布式内存的Spark架构，其速度是Apache
Mahout使用基于磁盘实现速度的9倍，根据基准测试由MLlib开发人员针对交替最小二乘（ALS）实现完成，在Mahout本身获得Spark接口之前。并且比Vowpal
Wabbit具有更好地扩展。许多常见的机器学习和统计算法已经实现并随MLlib一起提供，这简化了大规模机器学习的流水线，其中包括：支持向量机，逻辑回归，线性回归，决策树，朴素贝叶斯分类，等等
