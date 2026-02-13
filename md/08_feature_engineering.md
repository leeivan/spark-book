# 特征工程


## 本章先看懂什么
- 特征工程的三步：清洗、变换、选择。
- 文本、类别、数值特征分别怎么处理。
- 特征质量为什么常常比模型复杂度更重要。

## 一个最小例子
需求：做商品评论情感分类。
1. 分词并生成 TF-IDF。
2. 类别字段做 One-Hot 编码。
3. 数值字段做标准化。
4. 拼接特征后送入分类器。

实践中，稳定的特征流程往往比频繁换模型更有效。

> **版本基线（更新于 2026-02-13）**
> 本书默认适配 Apache Spark 4.1.1（稳定版），并兼容 4.0.2 维护分支。
> 推荐环境：JDK 17+（建议 JDK 21）、Scala 2.13、Python 3.10+。
特征是数据中抽取出来的对结果预测有用的信息，可以是文本或者数据。特征工程是使用专业背景知识和技巧处理数据，使得特征能在机器学习算法上发挥更好作用的过程。这个过程包含了特征提取、特征构建、特征选择等模块。特征工程的目的是筛选出更好的特征，获取更好的训练数据。因为好的特征具有更强的灵活性，可以用简单的模型做训练，更可以得到优秀的结果。“工欲善其事，必先利其器”，特征工程可以理解为利其器的过程。本节介绍Spark用于处理特征的算法，大致分为以下几组：

  - 特征提取：从原始数据中提取特征

  - 特征转换：缩放，转化或修改特征

  - 特征选择：从更多的特征中选择一个子集

  - 局部敏感散列：解决高维空间中近似或精确的近邻搜索

## 特征提取

在机器学习、模式识别和图像处理中，特征提取是从一组初始的测量数据开始，构建旨在提供有价值的和非冗余的派生值或特征，以利于后续机器学习和泛化过程，并在某些情况下更好地被人类解释。在图形处理中，特征提取一般也与降维有关。

### TF-IDF

TF-IDF（Term Frequency-Inverse Document
Fsrequency，TF表示词频，IDF表示是逆向文件频率）是一种自然语言学习的统计方法，用以评估字词在一个文件集或一个语料库中的重要程度。字词的重要性随着其在文件中出现的次数成正比而增加，但同时会随着其在语料库中出现的频率成反比而下降。基于TF-IDF加权的各种形式常被搜索引擎应用，作为返回信息与用户查询之间相关程度的度量或评级。

如果用\(t\)表示一个词条，用\(d\)表示一个文档，用\(D\)表示一个语料库，\(TF(t,d)\)表示词条\(t\)在文档\(d\)中出现的次数，而\(DF(t,D)\)表示词条\(t\)在语料库\(D\)中出现的次数。如果只使用\(TF(t,d)\)来衡量词条\(t\)重要性，那么很容易过度强调经常出现的词条，但其缺少有用的信息，例如英文中的冠词“a”、“the”和“of”，和中文的助词“的”、“地”和“得”等等。如果一个词条经常在整个语料库\(D\)中出现，这意味对于特定文档\(d\)其没有提供特定的信息，而\(DF(t,D)\)可以度量词条\(t\)提供多少信息量：

\[IDF(t,D) = \log\frac{|D| + 1}{DF(t,D) + 1}\]

公式 4‑1

其中\(|D|\)是语料库中文档的总数。由于公式
4‑1使用对数，所以当一个词条在所有文档中都出现，则\(DF(t,D)\)值将为0。如果词条不在语料库中，为了防止除数为零通常将分母调整为\(DF(t,D) + 1\)。最终，TF-IDF的值为TF和IDF的乘积：

\[TF - IDF(t,d,D) = TF(t,d) \cdot IDF(t,D)\]

公式 4‑2

如果在某一特定文档中，一个词条出现的频率比较高，而在整个语料库中出现的频率比较低，则可以产生出高权重的TF-IDF。因此，TF-IDF值倾向于过滤掉常见的词条，而保留重要的。

实际上，TF-IDF的定义有很多变种，这里用上面的公式定义举一个例子来理解其概念。假如在一个文档中而词条“大数据”出现了3次，那么“大数据”在该文档中的\(\text{TF}\left( t,d \right)\)就是3，如果“大数据”在99个文档出现过，而语料库中的文档总数是999，则\(\text{DF}\left( t,D \right)\)是\(\log\frac{\left( 999 + 1 \right)}{\left( 99 + 1 \right)} = 1\)，最后TF-IDF的值为\(3*1 = 3\)

在Spark中，HashingTF和CountVectorizer均可用于生成词条频率向量。HashingTF是一个转换器，它将多组项转换成固定长度的特征向量。在自然语言处理中，需要对文本中的单词或词组进行频率计算。HashingTF使用特征哈希（Featrue
Hashing或Hashing
Trick），即通过应用散列函数将原始特征（一系列单词或词组）映射到索引，然后根据映射的索引计算词频，其使用的散列函数是MurmurHash3，这种方法避免了计算全局词汇索引映射的需要，这对于大型语料库来说可以避免代价昂贵的计算，但是另外具有潜在的散列索引冲突，其中不同的原始特征可能在散列之后变成相同的词汇。为了减少散列索引发生碰撞的机会，可以增加目标特征维度，即散列表的桶数。由于使用简单的模来将散列函数转换为列索引，因此建议使用2的幂作为特征维度，否则特征将不会均匀地映射到向量索引。默认的特征尺寸是
\(2^{18}\)=
262,144。可选的二进制切换参数控制词汇频率计数，当设置为真时所有非零频率计数都被设置为1，这对于模拟二进制计数而不是整数计数的离散概率模型特别有用。使用IDF估算器对数据集进行拟合并生成IDFModel，IDFModel采用特征向量（通常由HashingTF或CountVectorizer创建）并按比例量化每一列，本质上是对语料库中经常出现的列添加权重。

在下面的代码段中，从一组句子开始，使用Tokenizer将每个句子分成单词。对于每个句子（单词袋），使用HashingTF将句子散列成一个特征向量，然后使用IDF来重新调整特征向量的权重，当使用文本作为特征时这通常会提高性能，经过处理的特征向量可以传递给机器学习算法。

scala\> import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

scala\> val sentenceData = spark.createDataFrame(Seq(

| (0.0, "Hi I heard about Spark"),

| (0.0, "I wish Java could use case classes"),

| (1.0, "Logistic regression models are neat")

| )).toDF("label", "sentence")

sentenceData: org.apache.spark.sql.DataFrame = \[label: double,
sentence: string\]

scala\> val tokenizer = new
Tokenizer().setInputCol("sentence").setOutputCol("words")

tokenizer: org.apache.spark.ml.feature.Tokenizer = tok\_1588061c5487

scala\> val wordsData = tokenizer.transform(sentenceData)

wordsData: org.apache.spark.sql.DataFrame = \[label: double, sentence:
string ... 1 more field\]

scala\> val hashingTF = new HashingTF()

hashingTF: org.apache.spark.ml.feature.HashingTF =
hashingTF\_3b9077cfb7fe

scala\>val hashingTF = new
HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

hashingTF: org.apache.spark.ml.feature.HashingTF =
hashingTF\_a7e6b47cbb50

scala\> val featurizedData = hashingTF.transform(wordsData)

featurizedData: org.apache.spark.sql.DataFrame = \[label: double,
sentence: string ... 2 more fields\]

scala\> val idf = new
IDF().setInputCol("rawFeatures").setOutputCol("features")

idf: org.apache.spark.ml.feature.IDF = idf\_d997c028b7bb

scala\> val idfModel = idf.fit(featurizedData)

idfModel: org.apache.spark.ml.feature.IDFModel = idf\_d997c028b7bb

scala\> val rescaledData = idfModel.transform(featurizedData)

rescaledData: org.apache.spark.sql.DataFrame = \[label: double,
sentence: string ... 3 more fields\]

scala\> featurizedData.take(1)

res12: Array\[org.apache.spark.sql.Row\] = Array(\[0.0,Hi I heard about
Spark,WrappedArray(hi, i, heard, about,
spark),(20,\[0,5,9,17\],\[1.0,1.0,1.0,2.0\])\])

scala\> rescaledData.take(1)

res15: Array\[org.apache.spark.sql.Row\] = Array(\[0.0,Hi I heard about
Spark,WrappedArray(hi, i, heard, about,
spark),(20,\[0,5,9,17\],\[1.0,1.0,1.0,2.0\]),(20,\[0,5,9,17\],\[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906\])\])

代码 4‑1

  - Murmurhash3

MurmurHash是一个非加密哈希函数，适用于一般的基于哈希的查找，是由Austin
Appleby在2008年创建的，目前在Github上与其测试套件SMHasher一起托管，其也存在一些变种，所有这些都已经被公开。其名称来自两个基本操作，乘法（MU）和旋转（R），在其内部循环中使用。与密码散列函数不同，MurmurHash不是专门设计成难以被反向破解的，所以不适用于密码学。目前的版本是MurmurHash3，产生一个32位或128位散列值。使用128位时，x86和x64版本不会生成相同的值，因为算法针对各自的平台进行了优化。

### Word2Vec

Word2Vec计算词汇的分布式矢量表示，分布式表示的主要优点在于向量空间中相似的词汇之间距离是靠近的，可以通过空间向量计算来分析词汇的语义，这使得对新模式的泛化更容易，模型估计的适应性更强。词汇的分布式向量表示在许多自然语言处理应用中被证明是有用的，例如命名实体识别、消歧、解析、标记和机器翻译。

在Spark的Word2Vec的实现中，使用了Skip-Gram模型。Skip-Gram的训练目标是学习单词向量表示，有利于在相同句子中更好地预测其上下文的。在数学上，给定训练词汇的序列\(w_{1},w_{2},\ldots,w_{T}\)，Skip-Gram的目标是最大化平均对数似然：

\[\frac{1}{T}\sum_{t = 1}^{T}{}\sum_{j = - k}^{j = k}{\log p(w_{t + j}|w_{t})}\]

公式 4‑3

其中\(k\)是训练窗口的大小。在Skip-Gram模型中，每个词\(w\)与两个向量\(u_{w}\)和\(v_{w}\)相关联，这两个向量分别是\(w\)的词向量表示。如果给定\(w_{j}\)，正确预测词\(w_{i}\)的概率是由Softmax模型决定的：

\[p(w_{i}|w_{j}) = \frac{\exp(u_{w_{i}}^{\top}v_{w_{j}})}{\sum_{l = 1}^{V}{\exp(u_{l}^{\top}v_{w_{j}})}}\]

公式 4‑4

其中\(V\)是词汇大小。基于Softmax的Skip-Gram模型计算成本是非常高的，因为\(\log p\left( w_{i} \middle| w_{j} \right)\)的计算成本与\(V\)成正比，可以很容易地达到数百万的数量级。为了加快Word2Vec的训练，使用分层Softmax，它将\(\log p\left( w_{i} \middle| w_{j} \right)\)的计算复杂度降低到\(O\left( \log\left( V \right) \right)\)
。

Word2Vec是一个估算器，接受文档的单词序列并训练Word2VecModel。模型将每个单词映射到一个唯一的固定大小的向量。Word2VecModel使用文档中所有单词的平均值将每个文档转换为一个向量，这个向量然后可以作为文档的特征用来预测和文档相似性计算等。在下面的代码段中，从一组文档开始，每个文档都被表示为一个单词序列。对于每个文档，把它转换成一个特征向量，这个特征向量可以被传递给一个机器学习算法。

scala\> import org.apache.spark.ml.feature.Word2Vec

import org.apache.spark.ml.feature.Word2Vec

scala\> import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.linalg.Vector

scala\> import org.apache.spark.sql.Row

import org.apache.spark.sql.Row

代码 4‑2

输入数据，每一行代表一句话或文档的词袋：

scala\> val documentDF = spark.createDataFrame(Seq(

| "Hi I heard about Spark".split(" "),

| "I wish Java could use case classes".split(" "),

| "Logistic regression models are neat".split(" ")

| ).map(Tuple1.apply)).toDF("text")

documentDF: org.apache.spark.sql.DataFrame = \[text: array\<string\>\]

scala\> documentDF.show

\+--------------------+

| text|

\+--------------------+

|\[Hi, I, heard, ab...|

|\[I, wish, Java, c...|

|\[Logistic, regres...|

\+--------------------+

代码 4‑3

学习从单词到向量的映射：

scala\> val word2Vec = new
Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)

word2Vec: org.apache.spark.ml.feature.Word2Vec = w2v\_58b619bf7883

scala\> val model = word2Vec.fit(documentDF)

model: org.apache.spark.ml.feature.Word2VecModel = w2v\_58b619bf7883

scala\> val result = model.transform(documentDF)

result: org.apache.spark.sql.DataFrame = \[text: array\<string\>,
result: vector\]

scala\> result.collect().foreach { case Row(text: Seq\[\_\], features:
Vector) =\>

| println(s"Text: \[${text.mkString(", ")}\] =\> \\nVector:
$features\\n")

| }

Text: \[Hi, I, heard, about, Spark\] =\>

Vector:
\[0.03173386193811894,0.009443491697311401,0.024377789348363876\]

Text: \[I, wish, Java, could, use, case, classes\] =\>

Vector: \[0.025682436302304268,0.0314303718706859,-0.01815584538105343\]

Text: \[Logistic, regression, models, are, neat\] =\>

Vector:
\[0.022586782276630402,-0.01601201295852661,0.05122732147574425\]

代码 4‑4

  - softmax函数

softmax函数用于多种分类方法，如多项逻辑回归（也称为softmax回归）、多类线性判别分析、朴素贝叶斯分类器和人工神经网络。具体来说，在多项逻辑回归和线性判别分析中，函数的输入是\(\mathbf{K}\)个不同的线性函数的结果，给定样本向量\(\mathbf{x}\)和加权向量\(\mathbf{w}\)的第\(\mathbf{j}\)个类的预测概率为：

\[P(y = j \mid \mathbf{x}) = \frac{e^{\mathbf{x}^{\mathsf{T}}\mathbf{w}_{j}}}{\sum_{k = 1}^{K}e^{\mathbf{x}^{\mathsf{T}}\mathbf{w}_{k}}}\]

公式 4‑5

这可以看作是K线性函数\(\mathbf{x \mapsto}\mathbf{x}^{\mathbf{T}}\mathbf{w}_{\mathbf{1}}\mathbf{,\ldots,x \mapsto}\mathbf{x}^{\mathbf{T}}\mathbf{w}_{\mathbf{K}}\)组成和softmax函数（其中\(\mathbf{x}^{\mathbf{T}}\mathbf{w}\)表示为\(\mathbf{x}\)和\(\mathbf{w}\)的内积）的组合。该操作相当于应用通过\(\mathbf{w}\mathbf{定}\)义的线性算子到向量\(\mathbf{x}\)，从而将原始的、可能是高维的输入向量转换成\(\mathbf{K}\)维空间\(\mathbf{R}^{\mathbf{K}}\)。

  - Skip-Gram

Word2Vec模型中，主要有Skip-Gram和CBOW两种模型，从直观上理解，Skip-Gram是给定输入词来预测上下文。而CBOW是给定上下文，来预测输入词。

![](media/08_feature_engineering/media/image1.tiff)

图例 4‑1Skip-Gram和CBOW两种模型

### CountVectorizer

CountVectorizer和CountVectorizerModel旨在帮助将文本文档集合转换为词汇计数向量。当预定义字典不可用时，CountVectorizer可用作估算器来提取词汇表，并生成一个CountVectorizerModel，该模型为基于这些词汇的文档生成稀疏矩阵表示，然后将其传递给其他算法。

在拟合过程中，CountVectorizer将在语料库中按照频率大小排序选择词汇，通过指定词汇必须出现的文档最小数量，一个可选参数minDF来影响拟合过程。另一个可选的二进制开关参数控制输出向量，如果设置为true，则所有非零计数都设置为1。这对于模拟二进制计数而不是整数计数的离散概率模型特别有用。假设有以下DateFrame，包括列id和texts：

id | texts

\----|----------

0 | Array("a", "b", "c")

1 | Array("a", "b", "b", "c", "a")

代码 4‑5

文本中的每一行都是一个Array \[String\]类型的文档，通过CountVectorizer的拟合产生一个带有词汇表(a, b,
c)的CountVectorizerModel，然后转换后的输出列vector包含：

id | texts | vector

\----|---------------------------------|---------------

0 | Array("a", "b", "c") | (3,\[0,1,2\],\[1.0,1.0,1.0\])

1 | Array("a", "b", "b", "c", "a") | (3,\[0,1,2\],\[2.0,2.0,1.0\])

代码 4‑6

每个向量表示文档中词汇的出现次数。

scala\> import org.apache.spark.ml.feature.{CountVectorizer,
CountVectorizerModel}

import org.apache.spark.ml.feature.{CountVectorizer,
CountVectorizerModel}

scala\> val df = spark.createDataFrame(Seq(

| (0, Array("a", "b", "c")),

| (1, Array("a", "b", "b", "c", "a"))

| )).toDF("id", "words")

df: org.apache.spark.sql.DataFrame = \[id: int, words: array\<string\>\]

scala\> val cvModel: CountVectorizerModel = new
CountVectorizer().setInputCol("words").setOutputCol("features").setVocabSize(3).setMinDF(2).fit(df)

cvModel: org.apache.spark.ml.feature.CountVectorizerModel =
cntVec\_5074dfb20769

scala\> val cvm = new CountVectorizerModel(Array("a", "b",
"c")).setInputCol("words").setOutputCol("features")

cvm: org.apache.spark.ml.feature.CountVectorizerModel =
cntVecModel\_8e78064be4b8

scala\> cvModel.transform(df).select("features").show()

\+--------------------+

| features|

\+--------------------+

|(3,\[0,1,2\],\[1.0,1...|

|(3,\[0,1,2\],\[2.0,2...|

\+--------------------+

代码 4‑7

## 特征转换

### Tokenizer

Tokenizer的处理过程是将文本分解成单个词汇的过程。下面的例子展示了如何将句子拆分成单词序列。RegexTokenizer允许基于正则表达式匹配更高级的处理，默认情况下pattern参数被用作分隔符来分割输入文本（正则表达式默认值为\\\\s
+），或者用户可以将gaps参数设置为false，指示正则表达式pattern指定了词汇而不是分割间隙，并查找所有匹配结果。

scala\> import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

scala\> import org.apache.spark.sql.functions.\_

import org.apache.spark.sql.functions.\_

scala\> val sentenceDataFrame = spark.createDataFrame(Seq(

| (0, "Hi I heard about Spark"),

| (1, "I wish Java could use case classes"),

| (2, "Logistic,regression,models,are,neat")

| )).toDF("id", "sentence")

sentenceDataFrame: org.apache.spark.sql.DataFrame = \[id: int, sentence:
string\]

scala\>

scala\> val tokenizer = new
Tokenizer().setInputCol("sentence").setOutputCol("words")

tokenizer: org.apache.spark.ml.feature.Tokenizer = tok\_c0f6a3c47f23

scala\> val regexTokenizer = new
RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\\\W")

regexTokenizer: org.apache.spark.ml.feature.RegexTokenizer =
regexTok\_0a7025a2ca35

scala\> val countTokens = udf { (words: Seq\[String\]) =\> words.length
}

countTokens: org.apache.spark.sql.expressions.UserDefinedFunction =
UserDefinedFunction(\<function1\>,IntegerType,Some(List(ArrayType(StringType,true))))

scala\> val tokenized = tokenizer.transform(sentenceDataFrame)

tokenized: org.apache.spark.sql.DataFrame = \[id: int, sentence: string
... 1 more field\]

scala\> tokenized.select("sentence", "words").withColumn("tokens",
countTokens(col("words"))).show(false)

\+-----------------------------------+------------------------------------------+------+

|sentence |words |tokens|

\+-----------------------------------+------------------------------------------+------+

|Hi I heard about Spark |\[hi, i, heard, about, spark\] |5 |

|I wish Java could use case classes |\[i, wish, java, could, use, case,
classes\]|7 |

|Logistic,regression,models,are,neat|\[logistic,regression,models,are,neat\]
|1 |

\+-----------------------------------+------------------------------------------+------+

scala\> val regexTokenized = regexTokenizer.transform(sentenceDataFrame)

regexTokenized: org.apache.spark.sql.DataFrame = \[id: int, sentence:
string ... 1 more field\]

scala\> regexTokenized.select("sentence", "words").withColumn("tokens",
countTokens(col("words"))).show(false)

\+-----------------------------------+------------------------------------------+------+

|sentence |words |tokens|

\+-----------------------------------+------------------------------------------+------+

|Hi I heard about Spark |\[hi, i, heard, about, spark\] |5 |

|I wish Java could use case classes |\[i, wish, java, could, use, case,
classes\]|7 |

|Logistic,regression,models,are,neat|\[logistic, regression, models,
are, neat\] |5 |

\+-----------------------------------+------------------------------------------+------+

代码 4‑8

### StopWordsRemover

停止词是需要从输入文本中排除的词，通常是因为词经常出现并且不具有具体的含义。StopWordsRemover将一系列字符串作为输入（例如Tokenizer处理后的输出），并从这些输入序列中删除所有停用词。停用词的列表由stopWords参数指定，可以通过调用StopWordsRemover.loadDefaultStopWords（language）来访问某些语言的默认停用词，其中可用的选项是“danish”，“dutch”，“english”，“finnish”，“french”，“german”，“hungarian”，“italian”，“norwegian”，“portuguese”，“russian”，“spanish”，“swedish”和“turkish”。布尔型参数caseSensitive指示匹配是否区分大小写，默认为false。假设有以下DataFrame，包含列id和raw：

id | raw

\----|----------

0 | \[I, saw, the, red, baloon\]

1 | \[Mary, had, a, little, lamb\]

代码 4‑9

将raw作为StopWordsRemover的输入列，filtered作为输出列，应该得到以下内容：

id | raw | filtered

\----|-----------------------------|--------------------

0 | \[I, saw, the, red, baloon\] | \[saw, red, baloon\]

1 | \[Mary, had, a, little, lamb\]|\[Mary, little, lamb\]

代码 4‑10

在filtered中，停用词“I”，“the”，“had”和“a”已被过滤掉。

scala\> import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.ml.feature.StopWordsRemover

scala\> val remover = new
StopWordsRemover().setInputCol("raw").setOutputCol("filtered")

remover: org.apache.spark.ml.feature.StopWordsRemover =
stopWords\_bcd87f61a74e

scala\> val dataSet = spark.createDataFrame(Seq(

| (0, Seq("I", "saw", "the", "red", "balloon")),

| (1, Seq("Mary", "had", "a", "little", "lamb"))

| )).toDF("id", "raw")

dataSet: org.apache.spark.sql.DataFrame = \[id: int, raw:
array\<string\>\]

scala\>

scala\> remover.transform(dataSet).show(false)

\+---+----------------------------+--------------------+

|id |raw |filtered |

\+---+----------------------------+--------------------+

|0 |\[I, saw, the, red, balloon\] |\[saw, red, balloon\] |

|1 |\[Mary, had, a, little, lamb\]|\[Mary, little, lamb\]|

\+---+----------------------------+--------------------+

代码 4‑11

### n-gram

\(n\)-gram是\(n\)个连续单词的序列，\(\text{\ n}\)是某个整数。NGram类可以用来将输入特征转换成\(n\)-gram。\(n\)-gram将字符串序列作为输入，参数\(n\)用于确定每个\(n\)-gram中连续单词的数量，输出将由\(n\)-gram序列组成，其中每个\(n\)-gram由一个以空格分隔的\(n\)个连续单词的字符串表示。如果输入序列包含少于\(n\)个字符串，则不会生成输出。

scala\> import org.apache.spark.ml.feature.NGram

import org.apache.spark.ml.feature.NGram

scala\> val wordDataFrame = spark.createDataFrame(Seq(

| (0, Array("Hi", "I", "heard", "about", "Spark")),

| (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),

| (2, Array("Logistic", "regression", "models", "are", "neat"))

| )).toDF("id", "words")

wordDataFrame: org.apache.spark.sql.DataFrame = \[id: int, words:
array\<string\>\]

scala\> val ngram = new
NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

ngram: org.apache.spark.ml.feature.NGram = ngram\_c2b7dcbc9a97

scala\> val ngramDataFrame = ngram.transform(wordDataFrame)

ngramDataFrame: org.apache.spark.sql.DataFrame = \[id: int, words:
array\<string\> ... 1 more field\]

scala\> ngramDataFrame.select("ngrams").show(false)

\+------------------------------------------------------------------+

|ngrams |

\+------------------------------------------------------------------+

|\[Hi I, I heard, heard about, about Spark\] |

|\[I wish, wish Java, Java could, could use, use case, case classes\]|

|\[Logistic regression, regression models, models are, are neat\] |

\+------------------------------------------------------------------+

代码 4‑12

### Binarizer

二值化是将数字特征根据阈值转换成二进制（0/1）特征的过程。Binarizer采用通用参数inputCol和outputCol
，以及阈值threshold。大于阈值的特征值被二进制化为1.0；等于或小于阈值的值被二值化为0.0。inputCol支持Vector和Double类型。

scala\> import org.apache.spark.ml.feature.Binarizer

import org.apache.spark.ml.feature.Binarizer

scala\> val data = Array((0, 0.1), (1, 0.8), (2, 0.2))

data: Array\[(Int, Double)\] = Array((0,0.1), (1,0.8), (2,0.2))

scala\> val dataFrame = spark.createDataFrame(data).toDF("id",
"feature")

dataFrame: org.apache.spark.sql.DataFrame = \[id: int, feature: double\]

scala\> val binarizer: Binarizer = new
Binarizer().setInputCol("feature").setOutputCol("binarized\_feature").setThreshold(0.5)

binarizer: org.apache.spark.ml.feature.Binarizer =
binarizer\_ecd8c65d1d28

scala\> val binarizedDataFrame = binarizer.transform(dataFrame)

binarizedDataFrame: org.apache.spark.sql.DataFrame = \[id: int, feature:
double ... 1 more field\]

scala\> println(s"Binarizer output with Threshold =
${binarizer.getThreshold}")

Binarizer output with Threshold = 0.5

scala\> binarizedDataFrame.show()

\+---+-------+-----------------+

| id|feature|binarized\_feature|

\+---+-------+-----------------+

| 0| 0.1| 0.0|

| 1| 0.8| 1.0|

| 2| 0.2| 0.0|

\+---+-------+-----------------+

代码 4‑13

### PCA（主成分分析）

PCA是一个统计的过程，使用正交变换将一组可能的相关变量观测值转换成一组线性不相关的变量值，称为主成分。一个PCA类使用主成分分析的方法训练一个模型来投影向量到低维空间。下面的例子说明了如何将5维的特征向量映射为3维的主成分。

scala\> import org.apache.spark.ml.feature.PCA

import org.apache.spark.ml.feature.PCA

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val data = Array(

| Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),

| Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),

| Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)

| )

data: Array\[org.apache.spark.ml.linalg.Vector\] =
Array((5,\[1,3\],\[1.0,7.0\]), \[2.0,0.0,3.0,4.0,5.0\],
\[4.0,0.0,0.0,6.0,7.0\])

scala\> val df =
spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

df: org.apache.spark.sql.DataFrame = \[features: vector\]

scala\> val pca = new
PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(3).fit(df)

pca: org.apache.spark.ml.feature.PCAModel = pca\_85a461ad106b

scala\> val result = pca.transform(df).select("pcaFeatures")

result: org.apache.spark.sql.DataFrame = \[pcaFeatures: vector\]

scala\> result.show(false)

\+-----------------------------------------------------------+

|pcaFeatures |

\+-----------------------------------------------------------+

|\[1.6485728230883807,-4.013282700516296,-5.524543751369388\] |

|\[-4.645104331781534,-1.1167972663619026,-5.524543751369387\]|

|\[-6.428880535676489,-5.337951427775355,-5.524543751369389\] |

\+-----------------------------------------------------------+

代码 4‑14

### PolynomialExpansion

PolynomialExpansion是一个将特征展开到多元空间的处理过程，运用于特征值进行一些多项式的转化，比如平方啊，三次方，通过设置n-degree参数结合原始的维度来定义，比如设置degree为2就可以将\((x,y)\)转化为\((x,x^{2},y,xy,y^{2})\)，下面的例子展示了如何将特征展开为一个3-degree多项式空间。

scala\> import org.apache.spark.ml.feature.PolynomialExpansion

import org.apache.spark.ml.feature.PolynomialExpansion

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val data = Array(

| Vectors.dense(2.0, 1.0),

| Vectors.dense(0.0, 0.0),

| Vectors.dense(3.0, -1.0)

| )

data: Array\[org.apache.spark.ml.linalg.Vector\] = Array(\[2.0,1.0\],
\[0.0,0.0\], \[3.0,-1.0\])

scala\> val df =
spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

df: org.apache.spark.sql.DataFrame = \[features: vector\]

scala\> val polyExpansion = new
PolynomialExpansion().setInputCol("features").setOutputCol("polyFeatures").setDegree(3)

polyExpansion: org.apache.spark.ml.feature.PolynomialExpansion =
poly\_a99f07202fbd

scala\> val polyDF = polyExpansion.transform(df)

polyDF: org.apache.spark.sql.DataFrame = \[features: vector,
polyFeatures: vector\]

scala\> polyDF.show(false)

\+----------+------------------------------------------+

|features |polyFeatures |

\+----------+------------------------------------------+

|\[2.0,1.0\] |\[2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0\] |

|\[0.0,0.0\] |\[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0\] |

|\[3.0,-1.0\]|\[3.0,9.0,27.0,-1.0,-3.0,-9.0,1.0,3.0,-1.0\]|

\+----------+------------------------------------------+

代码 4‑15

### Discrete Cosine Transform（DCT）

离散余弦变换（DCT）将在时域中的长度为\(N\)实数序列转换成另一个在频域中的长度为\(N\)实值序列。DCT类提供此功能，实现DCT-II方法并通过\(\frac{1}{\sqrt{2}}\)比例缩放结果，使得用于变换产生的表示矩阵为单一实体，无偏移被施加到所述变换的序列，例如变换序列的第0个元素是第0个DCT系数，而不是\(\frac{N}{2}\)）。

scala\> import org.apache.spark.ml.feature.DCT

import org.apache.spark.ml.feature.DCT

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val data = Seq(

| Vectors.dense(0.0, 1.0, -2.0, 3.0),

| Vectors.dense(-1.0, 2.0, 4.0, -7.0),

| Vectors.dense(14.0, -2.0, -5.0, 1.0))

data: Seq\[org.apache.spark.ml.linalg.Vector\] =
List(\[0.0,1.0,-2.0,3.0\], \[-1.0,2.0,4.0,-7.0\],
\[14.0,-2.0,-5.0,1.0\])

scala\> val df =
spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

df: org.apache.spark.sql.DataFrame = \[features: vector\]

scala\> val dct = new
DCT().setInputCol("features").setOutputCol("featuresDCT").setInverse(false)

dct: org.apache.spark.ml.feature.DCT = dct\_def77e623740

scala\> val dctDf = dct.transform(df)

dctDf: org.apache.spark.sql.DataFrame = \[features: vector, featuresDCT:
vector\]

scala\> dctDf.select("featuresDCT").show(false)

\+----------------------------------------------------------------+

|featuresDCT |

\+----------------------------------------------------------------+

|\[1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604\]|

|\[-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677\] |

|\[4.0,9.304453421915744,11.000000000000002,1.5579302036357163\] |

\+----------------------------------------------------------------+

代码 4‑16

  - DCT

离散傅里叶变换需要进行复数运算，尽管有FFT可以提高运算速度，但在图像编码、特别是在实时处理中非常不便。离散傅里叶变换在实际的图像通信系统中很少使用，但它具有理论的指导意义。根据离散傅里叶变换的性质，实偶函数的傅里叶变换只含实的余弦项，因此构造了一种实数域的变换——离散余弦变换(DCT)。通过研究发现，DCT除了具有一般的正交变换性质外，其变换阵的基向量很近似于Toeplitz矩阵的特征向量，后者体现了人类的语言、图像信号的相关特性。因此，在对语音、图像信号变换的确定的变换矩阵正交变换中，DCT变换被认为是一种准最佳变换。在近年颁布的一系列视频压缩编码的国际标准建议中，都把
DCT 作为其中的一个基本处理模块。

DCT除了上述介绍的几条特点，即：实数变换、确定的变换矩阵、准最佳变换性能外，二维DCT还是一种可分离的变换，可以用两次一维变换得到二维变换结果。最常用的一种离散余弦变换的类型是下面给出的第二种类型，通常所说的离散余弦变换指的就是这种。它的逆，也就是下面给出的第三种类型，通常相应的被称为"反离散余弦变换"，"逆离散余弦变换"或者"IDCT"。

有两个相关的变换，一个是离散正弦变换(DST for Discrete Sine
Transform),它相当于一个长度大概是它两倍的实奇函数的离散傅里叶变换；另一个是改进的离散余弦变换(MDCT
for Modified Discrete Cosine Transform),它相当于对交叠的数据进行离散余弦变换。

### StringIndexer

StringIndexer是将标签的字符串列转换成标签索引列，索引的取值范围为\[0,
numLabels\]，按照标签的出现频率进行排序，并且支持四种排序选项：

1.  frequencyDesc：按标签频率的降序（最频繁的标签分配为0）

2.  frequencyAsc：按标签频率的升序（最不频繁的标签分配为0）

3.  alphabetDesc：按字母顺序的降序

4.  alphabetAsc：按字母顺序的升序

默认选项为frequencyDesc。如果用户选择保留，则看不见的标签将放置在索引numLabels处，如果输入列为数字，则将其强制转换为字符串并为字符串值编制索引。当下游管道组件（例如估算器或转换器）使用此字符串索引标签时，必须将组件的输入列设置为此字符串索引列名称，在许多情况下可以使用setInputCol设置输入列。

假设有如下DataFrame包含id和category列：

id | category

\----|----------

0 | a

1 | b

2 | c

3 | a

4 | a

5 | c

代码 4‑17

category是具有三个标签字符串列：“a”，“b”和“c”的。应用StringIndexer并将category作为输入列，categoryIndex作为输出列，应该得到以下几点：

id | category | categoryIndex

\----|----------|---------------

0 | a | 0.0

1 | b | 2.0

2 | c | 1.0

3 | a | 0.0

4 | a | 0.0

5 | c | 1.0

代码 4‑18

“a”变为索引0，因为它是最常见的，其次是“c”索引1和“b”索引2。此外，当使用StringIndexer拟合一个数据集并且用它来进行其他转换时，还有三个策略关于如何处理看不见的标签：

  - 抛出一个异常（这是默认）

  - 跳过含有完全看不见的标签行

  - 将看不见的标签放在一个特殊的附加桶中，例如索引numLabels

回到前面的例子，但这次重新使用先前定义的StringIndexer到如下的数据集：

id | category

\----|----------

0 | a

1 | b

2 | c

3 | d

4 | e

代码 4‑19

如果还没有设置StringIndexer如何处理看不到的标签或将其设置为“error”，一个异常将被抛出，但是如果曾要求setHandleInvalid("skip")，以下数据集将产生：

id | category | categoryIndex

\----|----------|---------------

0 | a | 0.0

1 | b | 2.0

2 | c | 1.0

代码 4‑20

请注意，包含“d”或“e”的行不会出现，如果调用setHandleInvalid("keep")，以下数据集将产生：

id | category | categoryIndex

\----|----------|---------------

0 | a | 0.0

1 | b | 2.0

2 | c | 1.0

3 | d | 3.0

4 | e | 3.0

代码 4‑21

含有“d”或“e”的行被映射到索引“3.0”

scala\> import org.apache.spark.ml.feature.StringIndexer

import org.apache.spark.ml.feature.StringIndexer

scala\> val df = spark.createDataFrame(

| Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))

| ).toDF("id", "category")

df: org.apache.spark.sql.DataFrame = \[id: int, category: string\]

scala\> val indexer = new
StringIndexer().setInputCol("category").setOutputCol("categoryIndex")

indexer: org.apache.spark.ml.feature.StringIndexer =
strIdx\_13ff7dc1a41d

scala\> val indexed = indexer.fit(df).transform(df)

indexed: org.apache.spark.sql.DataFrame = \[id: int, category: string
... 1 more field\]

scala\> indexed.show()

\+---+--------+-------------+

| id|category|categoryIndex|

\+---+--------+-------------+

| 0| a| 0.0|

| 1| b| 2.0|

| 2| c| 1.0|

| 3| a| 0.0|

| 4| a| 0.0|

| 5| c| 1.0|

\+---+--------+-------------+

代码 4‑22

### IndexToString

与StringIndexer对称，IndexToString将标签索引列映射回包含原始字符串标签列。一个常见的使用情形是用StringIndexer从标签产生索引，使用索引训练模型，然后使用IndexToString从具有预测索引的列中返回原始标签，但是我们可以自由地提供自己的标签。在StringIndexer例子的基础上，假设有一个DataFrame包含id和categoryIndex：

id | categoryIndex

\----|---------------

0 | 0.0

1 | 2.0

2 | 1.0

3 | 0.0

4 | 0.0

5 | 1.0

代码 4‑23

将categoryIndex作为IndexToString输入列，originalCategory作为输出列，能找回原来的标签，将从列的元数据来推断：

id | categoryIndex | originalCategory

\----|---------------|-----------------

0 | 0.0 | a

1 | 2.0 | b

2 | 1.0 | c

3 | 0.0 | a

4 | 0.0 | a

5 | 1.0 | c

代码 4‑24

scala\> import org.apache.spark.ml.attribute.Attribute

import org.apache.spark.ml.attribute.Attribute

scala\> import org.apache.spark.ml.feature.{IndexToString,
StringIndexer}

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

scala\> val df = spark.createDataFrame(Seq(

| (0, "a"),

| (1, "b"),

| (2, "c"),

| (3, "a"),

| (4, "a"),

| (5, "c")

| )).toDF("id", "category")

df: org.apache.spark.sql.DataFrame = \[id: int, category: string\]

scala\> val indexer = new
StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)

indexer: org.apache.spark.ml.feature.StringIndexerModel =
strIdx\_a7b1ea964da7

scala\> val indexed = indexer.transform(df)

indexed: org.apache.spark.sql.DataFrame = \[id: int, category: string
... 1 more field\]

scala\> println(s"Transformed string column '${indexer.getInputCol}' " +

| s"to indexed column '${indexer.getOutputCol}'")

Transformed string column 'category' to indexed column 'categoryIndex'

scala\> indexed.show()

\+---+--------+-------------+

| id|category|categoryIndex|

\+---+--------+-------------+

| 0| a| 0.0|

| 1| b| 2.0|

| 2| c| 1.0|

| 3| a| 0.0|

| 4| a| 0.0|

| 5| c| 1.0|

\+---+--------+-------------+

scala\> val inputColSchema = indexed.schema(indexer.getOutputCol)

inputColSchema: org.apache.spark.sql.types.StructField =
StructField(categoryIndex,DoubleType,true)

scala\> println(s"StringIndexer will store labels in output column
metadata: " +

| s"${Attribute.fromStructField(inputColSchema).toString}\\n")

StringIndexer will store labels in output column metadata:
{"vals":\["a","c","b"\],"type":"nominal","name":"categoryIndex"}

scala\> val converter = new
IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")

converter: org.apache.spark.ml.feature.IndexToString =
idxToStr\_6c16029b021c

scala\> val converted = converter.transform(indexed)

converted: org.apache.spark.sql.DataFrame = \[id: int, category: string
... 2 more fields\]

scala\>

scala\> println(s"Transformed indexed column '${converter.getInputCol}'
back to original string " +

| s"column '${converter.getOutputCol}' using labels in metadata")

Transformed indexed column 'categoryIndex' back to original string
column 'originalCategory' using labels in metadata

scala\> converted.select("id", "categoryIndex",
"originalCategory").show()

\+---+-------------+----------------+

| id|categoryIndex|originalCategory|

\+---+-------------+----------------+

| 0| 0.0| a|

| 1| 2.0| b|

| 2| 1.0| c|

| 3| 0.0| a|

| 4| 0.0| a|

| 5| 1.0| c|

\+---+-------------+----------------+

代码 4‑25

### OneHotEncoder

OneHotEncoder将标签索引列映射到二进制向量列，其中只包含一个有效位，这种编码允许需要连续特征值的算法（如逻辑回归）使用类别特征。

scala\> import org.apache.spark.ml.feature.{OneHotEncoder,
StringIndexer}

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

scala\> val df = spark.createDataFrame(Seq(

| (0, "a"),

| (1, "b"),

| (2, "c"),

| (3, "a"),

| (4, "a"),

| (5, "c")

| )).toDF("id", "category")

df: org.apache.spark.sql.DataFrame = \[id: int, category: string\]

scala\> val indexer = new
StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)

indexer: org.apache.spark.ml.feature.StringIndexerModel =
strIdx\_960fe46d3016

scala\> val indexed = indexer.transform(df)

indexed: org.apache.spark.sql.DataFrame = \[id: int, category: string
... 1 more field\]

scala\> val encoder = new
OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")

encoder: org.apache.spark.ml.feature.OneHotEncoder =
oneHot\_e70bbac6206f

scala\> val encoded = encoder.transform(indexed)

encoded: org.apache.spark.sql.DataFrame = \[id: int, category: string
... 2 more fields\]

scala\> encoded.show()

\+---+--------+-------------+-------------+

| id|category|categoryIndex| categoryVec|

\+---+--------+-------------+-------------+

| 0| a| 0.0|(2,\[0\],\[1.0\])|

| 1| b| 2.0| (2,\[\],\[\])|

| 2| c| 1.0|(2,\[1\],\[1.0\])|

| 3| a| 0.0|(2,\[0\],\[1.0\])|

| 4| a| 0.0|(2,\[0\],\[1.0\])|

| 5| c| 1.0|(2,\[1\],\[1.0\])|

\+---+--------+-------------+-------------+

代码 4‑26

### VectorIndexer

VectorIndexer将分类特征索引为向量数据集，既可以自动决定哪些特征是分类的，也可以将原始值转换成分类索引，具体而言执行以下操作：

（1）获得一个向量类型的输入以及maxCategories参数。

（2）基于不同特征值的数量来识别哪些特征需要被类别化，其中最多maxCategories个特征需要被类别化。

（3）对于每一个类别特征从0开始计算类别索引。

（4）对类别特征进行索引然后将原始特征值转换为索引。

索引后的类别特征可以帮助决策树等算法恰当的处理类别型特征，并得到较好结果。在下面的例子中，读入一个数据集，然后使用VectorIndexer来决定哪些特征需要被作为类别特征，将类别特征转换为他的索引。

scala\> import org.apache.spark.ml.feature.VectorIndexer

import org.apache.spark.ml.feature.VectorIndexer

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val data = Seq(

| Vectors.sparse(3, Array(0, 1, 2), Array(2.0, 5.0, 7.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(3.0, 5.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(4.0, 7.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(2.0, 4.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(9.0, 5.0, 7.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(2.0, 5.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(2.0, 5.0, 7.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(3.0, 4.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(8.0, 4.0, 9.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(3.0, 6.0, 2.0)),

| Vectors.sparse(3, Array(0, 1, 2), Array(5.0, 9.0, 2.0)))

data: Seq\[org.apache.spark.ml.linalg.Vector\] =
List((3,\[0,1,2\],\[2.0,5.0,7.0\]), (3,\[0,1,2\],\[3.0,5.0,9.0\]),
(3,\[0,1,2\],\[4.0,7.0,9.0\]), (3,\[0,1,2\],\[2.0,4.0,9.0\]),
(3,\[0,1,2\],\[9.0,5.0,7.0\]), (3,\[0,1,2\],\[2.0,5.0,9.0\]),
(3,\[0,1,2\],\[2.0,5.0,7.0\]), (3,\[0,1,2\],\[3.0,4.0,9.0\]),
(3,\[0,1,2\],\[8.0,4.0,9.0\]), (3,\[0,1,2\],\[3.0,6.0,2.0\]),
(3,\[0,1,2\],\[5.0,9.0,2.0\]))

scala\> val df =
spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

df: org.apache.spark.sql.DataFrame = \[features: vector\]

scala\> val indexer = new
VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5)

indexer: org.apache.spark.ml.feature.VectorIndexer =
vecIdx\_695bb0c7d21c

scala\> val indexerModel = indexer.fit(df)

indexerModel: org.apache.spark.ml.feature.VectorIndexerModel =
vecIdx\_695bb0c7d21c

scala\> val categoricalFeatures: Set\[Int\] =
indexerModel.categoryMaps.keys.toSet

categoricalFeatures: Set\[Int\] = Set(1, 2)

scala\> println(s"Chose ${categoricalFeatures.size} categorical
features: " + categoricalFeatures.mkString(", "))

Chose 2 categorical features: 1, 2

scala\> val indexedData = indexerModel.transform(df)

indexedData: org.apache.spark.sql.DataFrame = \[features: vector,
indexedFeatures: vector\]

scala\> indexedData.show(false)

\+-------------------------+-------------------------+

|features |indexedFeatures |

\+-------------------------+-------------------------+

|(3,\[0,1,2\],\[2.0,5.0,7.0\])|(3,\[0,1,2\],\[2.0,1.0,1.0\])|

|(3,\[0,1,2\],\[3.0,5.0,9.0\])|(3,\[0,1,2\],\[3.0,1.0,2.0\])|

|(3,\[0,1,2\],\[4.0,7.0,9.0\])|(3,\[0,1,2\],\[4.0,3.0,2.0\])|

|(3,\[0,1,2\],\[2.0,4.0,9.0\])|(3,\[0,1,2\],\[2.0,0.0,2.0\])|

|(3,\[0,1,2\],\[9.0,5.0,7.0\])|(3,\[0,1,2\],\[9.0,1.0,1.0\])|

|(3,\[0,1,2\],\[2.0,5.0,9.0\])|(3,\[0,1,2\],\[2.0,1.0,2.0\])|

|(3,\[0,1,2\],\[2.0,5.0,7.0\])|(3,\[0,1,2\],\[2.0,1.0,1.0\])|

|(3,\[0,1,2\],\[3.0,4.0,9.0\])|(3,\[0,1,2\],\[3.0,0.0,2.0\])|

|(3,\[0,1,2\],\[8.0,4.0,9.0\])|(3,\[0,1,2\],\[8.0,0.0,2.0\])|

|(3,\[0,1,2\],\[3.0,6.0,2.0\])|(3,\[0,1,2\],\[3.0,2.0,0.0\])|

|(3,\[0,1,2\],\[5.0,9.0,2.0\])|(3,\[0,1,2\],\[5.0,4.0,0.0\])|

\+-------------------------+-------------------------+

代码 4‑27

在上面的实例中，特征向量包含3个特征，即特征0、特征1、特征2。如第一行对应的特征分别是2.0、5.0、7.0，VectorIndexer将其转换为2.0、1.0、1.0。只有特征1和特征2被转换了，特征0没有被转换。这是因为特征0有6中取值分别为2、3、4、5、8、9，多于前面的设置setMaxCategories(5)，因此被视为连续值了不会被转换。

特征1的转换方式：

（4、5、6、7、9）--\>(0、1、2、3、4、5)

代码 4‑28

特征2的转换方式：

(2、7、9)--\>(0、1、2)

代码 4‑29

对照最后DataFrame第一行的输出格式说明，

转换前的值：

(3,\[0,1,2\],\[2.0,5.0,7.0\])

代码 4‑30

转换后的值：

(3,\[0,1,2\],\[2.0,1.0,1.0\])

代码 4‑31

### Interaction

Interaction是一个转换器，用来加载向量或双值列，并生成一个单一向量，其包含从每个输入列中一个值的所有组合的乘积，例如有2个向量类型的列，其每一个具有3个维度的输入列，那么将得到一个9维向量作为输出列，假设有一个DataFrame包含列“id1”，“vec
1”和“vec 2”：

id1|vec1 |vec2

\---|--------------|--------------

1 |\[1.0,2.0,3.0\] |\[8.0,4.0,5.0\]

2 |\[4.0,3.0,8.0\] |\[7.0,9.0,8.0\]

3 |\[6.0,1.0,9.0\] |\[2.0,3.0,6.0\]

4 |\[10.0,8.0,6.0\]|\[9.0,4.0,5.0\]

5 |\[9.0,2.0,7.0\] |\[10.0,7.0,3.0\]

6 |\[1.0,1.0,4.0\] |\[2.0,8.0,4.0\]

代码 4‑32

应用Interaction与这些输入列上，然后interactedCol作为输出列包含：

id1|vec1 |vec2 |interactedCol

\---|--------------|--------------|------------------------------------------------------

1 |\[1.0,2.0,3.0\] |\[8.0,4.0,5.0\]
|\[8.0,4.0,5.0,16.0,8.0,10.0,24.0,12.0,15.0\]

2 |\[4.0,3.0,8.0\] |\[7.0,9.0,8.0\]
|\[56.0,72.0,64.0,42.0,54.0,48.0,112.0,144.0,128.0\]

3 |\[6.0,1.0,9.0\] |\[2.0,3.0,6.0\]
|\[36.0,54.0,108.0,6.0,9.0,18.0,54.0,81.0,162.0\]

4 |\[10.0,8.0,6.0\]|\[9.0,4.0,5.0\]
|\[360.0,160.0,200.0,288.0,128.0,160.0,216.0,96.0,120.0\]

5 |\[9.0,2.0,7.0\]
|\[10.0,7.0,3.0\]|\[450.0,315.0,135.0,100.0,70.0,30.0,350.0,245.0,105.0\]

6 |\[1.0,1.0,4.0\] |\[2.0,8.0,4.0\]
|\[12.0,48.0,24.0,12.0,48.0,24.0,48.0,192.0,96.0\]

代码 4‑33

代码如下：

scala\>

import org.apache.spark.ml.feature.Interaction

scala\> import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.feature.VectorAssembler

scala\> val df = spark.createDataFrame(Seq(

| (1, 1, 2, 3, 8, 4, 5),

| (2, 4, 3, 8, 7, 9, 8),

| (3, 6, 1, 9, 2, 3, 6),

| (4, 10, 8, 6, 9, 4, 5),

| (5, 9, 2, 7, 10, 7, 3),

| (6, 1, 1, 4, 2, 8, 4)

| )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")

df: org.apache.spark.sql.DataFrame = \[id1: int, id2: int ... 5 more
fields\]

scala\> val assembler1 = new VectorAssembler().setInputCols(Array("id2",
"id3", "id4")).setOutputCol("vec1")

assembler1: org.apache.spark.ml.feature.VectorAssembler =
vecAssembler\_4be42f312aab

scala\> val assembled1 = assembler1.transform(df)

assembled1: org.apache.spark.sql.DataFrame = \[id1: int, id2: int ... 6
more fields\]

scala\> val assembler2 = new VectorAssembler().setInputCols(Array("id5",
"id6", "id7")).setOutputCol("vec2")

assembler2: org.apache.spark.ml.feature.VectorAssembler =
vecAssembler\_fbad7aa97d71

scala\> val assembled2 = assembler2.transform(assembled1).select("id1",
"vec1", "vec2")

assembled2: org.apache.spark.sql.DataFrame = \[id1: int, vec1: vector
... 1 more field\]

scala\> val interaction = new Interaction().setInputCols(Array("id1",
"vec1", "vec2")).setOutputCol("interactedCol")

interaction: org.apache.spark.ml.feature.Interaction =
interaction\_f0818f401e2d

scala\> val interacted = interaction.transform(assembled2)

interacted: org.apache.spark.sql.DataFrame = \[id1: int, vec1: vector
... 2 more fields\]

scala\> interacted.show(truncate = false)

\+---+--------------+--------------+------------------------------------------------------+

|id1|vec1 |vec2 |interactedCol |

\+---+--------------+--------------+------------------------------------------------------+

|1 |\[1.0,2.0,3.0\] |\[8.0,4.0,5.0\]
|\[8.0,4.0,5.0,16.0,8.0,10.0,24.0,12.0,15.0\] |

|2 |\[4.0,3.0,8.0\] |\[7.0,9.0,8.0\]
|\[56.0,72.0,64.0,42.0,54.0,48.0,112.0,144.0,128.0\] |

|3 |\[6.0,1.0,9.0\] |\[2.0,3.0,6.0\]
|\[36.0,54.0,108.0,6.0,9.0,18.0,54.0,81.0,162.0\] |

|4 |\[10.0,8.0,6.0\]|\[9.0,4.0,5.0\]
|\[360.0,160.0,200.0,288.0,128.0,160.0,216.0,96.0,120.0\]|

|5 |\[9.0,2.0,7.0\]
|\[10.0,7.0,3.0\]|\[450.0,315.0,135.0,100.0,70.0,30.0,350.0,245.0,105.0\]
|

|6 |\[1.0,1.0,4.0\] |\[2.0,8.0,4.0\]
|\[12.0,48.0,24.0,12.0,48.0,24.0,48.0,192.0,96.0\] |

\+---+--------------+--------------+------------------------------------------------------+

代码 4‑34

### Normalizer

Normalizer是一个转换器，作用范围是每一行，使每一个行向量归一化为一个单位范数。这需要指定参数p，用来指定p-范数用于归一化（默认情况下\(p = 2\)）。这种归一化可以帮助标准化我们的输入数据，并提高学习算法的行为。下面的例子演示了如何加载一个libsvm格式的数据集，然后归一化每个行，使其具有单位\(L^{1}\)范数和单位\(L^{\infty}\)范数。

scala\> import org.apache.spark.ml.feature.Normalizer

import org.apache.spark.ml.feature.Normalizer

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val dataFrame = spark.createDataFrame(Seq(

| (0, Vectors.dense(1.0, 0.5, -1.0)),

| (1, Vectors.dense(2.0, 1.0, 1.0)),

| (2, Vectors.dense(4.0, 10.0, 2.0))

| )).toDF("id", "features")

dataFrame: org.apache.spark.sql.DataFrame = \[id: int, features:
vector\]

scala\> val normalizer = new
Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0)

normalizer: org.apache.spark.ml.feature.Normalizer =
normalizer\_5dd6c243055f

scala\> val l1NormData = normalizer.transform(dataFrame)

l1NormData: org.apache.spark.sql.DataFrame = \[id: int, features: vector
... 1 more field\]

scala\> println("Normalized using L^1 norm")

Normalized using L^1 norm

scala\> l1NormData.show()

\+---+--------------+------------------+

| id| features| normFeatures|

\+---+--------------+------------------+

| 0|\[1.0,0.5,-1.0\]| \[0.4,0.2,-0.4\]|

| 1| \[2.0,1.0,1.0\]| \[0.5,0.25,0.25\]|

| 2|\[4.0,10.0,2.0\]|\[0.25,0.625,0.125\]|

\+---+--------------+------------------+

scala\> val lInfNormData = normalizer.transform(dataFrame, normalizer.p
-\> Double.PositiveInfinity)

lInfNormData: org.apache.spark.sql.Dataset\[org.apache.spark.sql.Row\] =
\[id: int, features: vector ... 1 more field\]

scala\> println("Normalized using L^inf norm")

Normalized using L^inf norm

scala\> lInfNormData.show()

\+---+--------------+--------------+

| id| features| normFeatures|

\+---+--------------+--------------+

| 0|\[1.0,0.5,-1.0\]|\[1.0,0.5,-1.0\]|

| 1| \[2.0,1.0,1.0\]| \[1.0,0.5,0.5\]|

| 2|\[4.0,10.0,2.0\]| \[0.4,1.0,0.2\]|

\+---+--------------+--------------+

代码 4‑35

  - 范数

知道距离的定义是一个宽泛的概念，只要满足非负、自反、三角不等式就可以称之为距离。范数是一种强化了的距离概念，它在定义上比距离多了一条数乘的运算法则。有时候为了便于理解，可以把范数当作距离来理解。在数学上，范数包括向量范数和矩阵范数，向量范数表征向量空间中向量的大小，矩阵范数表征矩阵引起变化的大小。一种非严密的解释就是，对应向量范数，向量空间中的向量都是有大小的，这个大小如何度量，就是用范数来度量的，不同的范数都可以来度量这个大小，就好比米和尺都可以来度量远近一样；对于矩阵范数，学过线性代数通过运算\(\mathbf{AX = B}\)，可以将向量X变化为B，矩阵范数就是来度量这个变化大小的。

在n维实向量空间\(\mathbb{R}^{\mathbf{n}}\)中,向量\(\mathbf{x = (x}\mathbf{1,x}\mathbf{2,...,xn)}\)的长度通常由欧几里得范数给出，那么：

\[{\parallel x \parallel}_{2} = {({x_{1}}^{2} + {x_{2}}^{2} + \cdots + {x_{n}}^{2})}^{\frac{1}{2}}\]

公式 4‑6

上面的公式是两点x和y之间的欧几里得距离，是两点之间直线的长度\(\mathbf{\parallel x - y \parallel}_{\mathbf{2}}\)。在许多情况下，欧几里德距离不足以捕获给定空间中的实际距离。出租车司机在网格街道计划中提出了类似的建议，但是他应该测量的不是直线到目的地的长度，而是直角距离（rectilinear
distance），其考虑到了街道是正交的或相互平行。p-范数的这一类概括了这两个例子，并在数学、物理学和计算机科学的许多部分有着丰富的应用。对于实数\(\mathbf{p \geq 1}\)，x的\(\mathbf{p}\)-norm或\(\mathbf{L}^{\mathbf{p}}\)-norm由下式定义：

\[{\parallel x \parallel}_{p} = {(|x_{1}|^{p} + |x_{2}|^{p} + \cdots + |x_{n}|^{p})}^{\frac{1}{p}}\]

公式 4‑7

当p取1，2，\(\mathbf{\infty}\)的时候分别是以下几种最简单的情形：

1-范数是经常见到的一种范数，它的定义如下：

\[||x||_{1} = \sum_{i}^{}|x_{i}|\]

公式 4‑8

表示向量x中非零元素的绝对值之和。1-范数有很多的名字，例如熟悉的曼哈顿距离、最小绝对误差等。使用1-范数可以度量两个向量间的差异，如绝对误差和（Sum
of Absolute Difference）：

\[SAD(x_{1},x_{2}) = \sum_{i}^{}|x_{1i} - x_{2i}|\]

公式 4‑9

由于1-范数的天然性质，对1-范数优化是一个稀疏解，因此1-范数也被叫做稀疏规则算子。通过1-范数可以实现特征的稀疏，去掉一些没有信息的特征，例如在对用户的电影爱好做分类的时候，用户有100个特征，可能只有十几个特征是对分类有用的，大部分特征如身高体重等可能都是无用的，利用1-范数就可以过滤掉。

2-范数是最常用的范数了，用的最多的度量距离欧氏距离就是一种2-范数，它的定义如下：

\[||x||_{2} = \sqrt{\sum_{i}^{}x_{i}^{2}}\]

公式 4‑11

表示向量元素的平方和再开平方。像1-范数一样，2-范数也可以度量两个向量间的差异，如平方差和（Sum of Squared
Difference）:

\[SSD(x_{1},x_{2}) = \sum_{i}^{}{(x_{1i} - x_{2i})^{2}}\]

公式 4‑12

2-范数通常会被用来做优化目标函数的正则化项，防止模型为了迎合训练集而过于复杂造成过拟合的情况，从而提高模型的泛化能力。

当p=∞时，也就是∞-范数，它主要被用来度量向量元素的最大值。∞-范数的定义为：

\[||x||_{\infty} = \sqrt[\infty]{\sum_{1}^{n}x_{i}^{\infty}}，x = (x_{1},x_{2},\cdots,x_{n})\]

公式 4‑14

与0-范数一样，在通常情况下大家都用的是：

\[||x||_{\infty} = max(|x_{i}|)\]

公式 4‑15

### StandardScaler

对于同一个特征，不同的样本中的取值可能会相差非常大，一些异常小或异常大的数据会误导模型的正确训练；另外，如果数据的分布很分散也会影响训练结果。以上两种方式都体现在方差会非常大。此时，我们可以将特征中的值进行标准差标准化，即转换为均值为0，方差为1的正态分布。如果特征非常稀疏，并且有大量的0（现实应用中很多特征都具有这个特点），Z-score
标准化的过程几乎就是一个除0的过程，结果不可预料。所以在训练模型之前，一定要对特征的数据分布进行探索，并考虑是否有必要将数据进行标准化。基于特征值的均值（Mean）和标准差（Standard
Deviation）进行数据的标准化。它的计算公式为：标准化数据=(原数据-均值)/标准差。标准化后的变量值围绕0上下波动，大于0说明高于平均水平，小于0说明低于平均水平。因为在原始的资料中，各变数的范围大不相同。对于某些机器学习的算法，若没有做过标准化，目标函数会无法适当的运作。举例来说，多数的分类器利用两点间的距离计算两点的差异，
若其中一个特征具有非常广的范围，那两点间的差异就会被该特征左右，因此，所有的特征都该被标准化，这样才能大略的使各特征依比例影响距离。另外一个做特征缩放的理由是他能使加速梯度下降法的收敛。

StandardScaler作用范围是每一行，使每一个行向量标准化具有单位标准偏差和零均值，或其中之一，需要的参数：

withStd：默认为true，按比例缩放数据到单位标准偏差。

withMean：默认为false，在缩放前使用均值居中数据。这将构建一个稠密矩阵输出，所以当输入稀疏矩阵时要小心。

StandardScaler是一个估算器，其可以在数据集上拟合，以产生一个StandardScalerModel；这相当于计算汇总统计，然后该模型可以转换数据集中一个向量列，使其具有单位标准偏差和零平均值的特征，或其中之一。

需要注意的是，如果一个特征的标准偏差为零，它将在这个特征的向量中返回默认0.0值。下面的例子演示了如何加载libsvm格式的数据集，然后归一化每个特征，使其具有单位标准偏差。

scala\> import org.apache.spark.ml.feature.StandardScaler

import org.apache.spark.ml.feature.StandardScaler

scala\> val dataFrame =
spark.read.format("libsvm").load("/spark/data/example/mllib/sample\_libsvm\_data.txt")

dataFrame: org.apache.spark.sql.DataFrame = \[label: double, features:
vector\]

setWithMean是否减均值，setWithStd是否将数据除以标准差，这里就是没有减均值但有除以标准差：

scala\> val scaler = new
StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false)

scaler: org.apache.spark.ml.feature.StandardScaler =
stdScal\_3902362fa7e8

通过拟合StandardScaler计算汇总统计：

scala\> val scalerModel = scaler.fit(dataFrame)

scalerModel: org.apache.spark.ml.feature.StandardScalerModel =
stdScal\_3902362fa7e8

归一化每个特征到单位标准偏差：

scala\> val scaledData = scalerModel.transform(dataFrame)

scaledData: org.apache.spark.sql.DataFrame = \[label: double, features:
vector ... 1 more field\]

scala\> scaledData.show()

\+-----+--------------------+--------------------+

|label| features| scaledFeatures|

\+-----+--------------------+--------------------+

| 0.0|(692,\[127,128,129...|(692,\[127,128,129...|

| 1.0|(692,\[158,159,160...|(692,\[158,159,160...|

| 1.0|(692,\[124,125,126...|(692,\[124,125,126...|

| 1.0|(692,\[152,153,154...|(692,\[152,153,154...|

| 1.0|(692,\[151,152,153...|(692,\[151,152,153...|

| 0.0|(692,\[129,130,131...|(692,\[129,130,131...|

| 1.0|(692,\[158,159,160...|(692,\[158,159,160...|

| 1.0|(692,\[99,100,101,...|(692,\[99,100,101,...|

| 0.0|(692,\[154,155,156...|(692,\[154,155,156...|

| 0.0|(692,\[127,128,129...|(692,\[127,128,129...|

| 1.0|(692,\[154,155,156...|(692,\[154,155,156...|

| 0.0|(692,\[153,154,155...|(692,\[153,154,155...|

| 0.0|(692,\[151,152,153...|(692,\[151,152,153...|

| 1.0|(692,\[129,130,131...|(692,\[129,130,131...|

| 0.0|(692,\[154,155,156...|(692,\[154,155,156...|

| 1.0|(692,\[150,151,152...|(692,\[150,151,152...|

| 0.0|(692,\[124,125,126...|(692,\[124,125,126...|

| 0.0|(692,\[152,153,154...|(692,\[152,153,154...|

| 1.0|(692,\[97,98,99,12...|(692,\[97,98,99,12...|

| 1.0|(692,\[124,125,126...|(692,\[124,125,126...|

\+-----+--------------------+--------------------+

only showing top 20 rows

代码 4‑36

### MinMaxScaler 

MinMaxScaler作用范围是每一行，重新缩放每个特征到特定范围内，通常是在\[0，1\]，需要的参数为：

min：0.0为默认值，转换后的下限，由所有特征共享。

max：1.0为默认值，转换后的上限，由所有特征共享。

MinMaxScaler对数据集计算汇总统计并产生MinMaxScalerModel，然后该模型可以独立地转换每个特征，使得其在给定的范围内。

\[\begin{matrix}
Rescaled(e_{i}) = \frac{e_{i} - E_{\min}}{E_{\max} - E_{\min}}*(max - min) + min \\
\end{matrix}\]

公式 4‑17

对于\(E_{\max} = = E_{\min}\)情况下，\(Rescaled(e_{i}) = 0.5*(max + min)\)。请注意，由于零值可能会被转换为非零值，转换器的输出将是DenseVector，即使稀疏矩阵作为输入。下面的例子演示了如何加载libsvm格式的数据集，然后重新调整每个特征为\[0,1\]。

scala\> import org.apache.spark.ml.feature.MinMaxScaler

import org.apache.spark.ml.feature.MinMaxScaler

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val dataFrame = spark.createDataFrame(Seq(

| (0, Vectors.dense(1.0, 0.1, -1.0)),

| (1, Vectors.dense(2.0, 1.1, 1.0)),

| (2, Vectors.dense(3.0, 10.1, 3.0))

| )).toDF("id", "features")

dataFrame: org.apache.spark.sql.DataFrame = \[id: int, features:
vector\]

scala\> val scaler = new
MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")

scaler: org.apache.spark.ml.feature.MinMaxScaler =
minMaxScal\_4c96b5ef3f2b

计算汇总统计产生MinMaxScalerModel：

scala\> val scalerModel = scaler.fit(dataFrame)

scalerModel: org.apache.spark.ml.feature.MinMaxScalerModel =
minMaxScal\_4c96b5ef3f2b

重新按比例调整每个特征到范围\[min, max\]：

scala\> val scaledData = scalerModel.transform(dataFrame)

scaledData: org.apache.spark.sql.DataFrame = \[id: int, features: vector
... 1 more field\]

scala\> println(s"Features scaled to range: \[${scaler.getMin},
${scaler.getMax}\]")

Features scaled to range: \[0.0, 1.0\]

scala\> scaledData.select("features", "scaledFeatures").show()

\+--------------+--------------+

| features|scaledFeatures|

\+--------------+--------------+

|\[1.0,0.1,-1.0\]| \[0.0,0.0,0.0\]|

| \[2.0,1.1,1.0\]| \[0.5,0.1,0.5\]|

|\[3.0,10.1,3.0\]| \[1.0,1.0,1.0\]|

\+--------------+--------------+

代码 4‑37

### MaxAbsScaler

MaxAbsScaler转换向量行的数据集，重新缩放每个特征到\[-1,1\]的范围内，通过除以每个特征的最大绝对值，它不移位或居中数据，因此不破坏任何稀疏性。

MaxAbsScaler对数据集计算汇总统计，并产生MaxAbsScalerModel，然后该模型可以独立地转换每个特征为范围\[-1,1\]。下面的例子演示了如何加载LIBSVM格式的数据集，然后重新调整每个特征为\[-1,1\]。

scala\> import org.apache.spark.ml.feature.MaxAbsScaler

import org.apache.spark.ml.feature.MaxAbsScaler

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val dataFrame = spark.createDataFrame(Seq(

| (0, Vectors.dense(1.0, 0.1, -8.0)),

| (1, Vectors.dense(2.0, 1.0, -4.0)),

| (2, Vectors.dense(4.0, 10.0, 8.0))

| )).toDF("id", "features")

dataFrame: org.apache.spark.sql.DataFrame = \[id: int, features:
vector\]

scala\> val scaler = new
MaxAbsScaler().setInputCol("features").setOutputCol("scaledFeatures")

scaler: org.apache.spark.ml.feature.MaxAbsScaler =
maxAbsScal\_26cca37ab1f7

计算汇总统计并且产生MaxAbsScalerModel：

scala\> val scalerModel = scaler.fit(dataFrame)

scalerModel: org.apache.spark.ml.feature.MaxAbsScalerModel =
maxAbsScal\_26cca37ab1f7

重新按比例调整每个特征到范围\[-1, 1\]：

scala\> val scaledData = scalerModel.transform(dataFrame)

scaledData: org.apache.spark.sql.DataFrame = \[id: int, features: vector
... 1 more field\]

scala\> scaledData.select("features", "scaledFeatures").show()

\+--------------+----------------+

| features| scaledFeatures|

\+--------------+----------------+

|\[1.0,0.1,-8.0\]|\[0.25,0.01,-1.0\]|

|\[2.0,1.0,-4.0\]| \[0.5,0.1,-0.5\]|

|\[4.0,10.0,8.0\]| \[1.0,1.0,1.0\]|

\+--------------+----------------+

代码 4‑38

### Bucketizer 

Bucketizer将连续的特征列转换成特征桶列，这些桶由用户指定，拥有一个splits参数。
例如商城的人群，觉得把人分为50以上和50以下太不精准了，应该分为20岁以下，20-30岁，30-40岁，36-50岁，50以上，那么就得用到数值离散化的处理方法了。离散化就是把特征进行适当的离散处理，比如上面所说的年龄是个连续的特征，但是把它分为不同的年龄阶段就是离散化了，这样更利于我们分析用户行为进行精准推荐。Bucketizer能方便的将一堆数据分成不同的区间，它需要一个参数：

splits：如果有n+1个splits，那么将有n个桶。桶将由split x和split
y共同确定，它的值范围为\[x,y\]，如果是最后一个桶，范围将是\[x,y\]。splits应该严格递增。负无穷和正无穷必须明确的提供用来覆盖所有的双精度值，否则超出splits的值将会被认为是一个错误。splits的两个例子是Array(Double.NegativeInfinity,
0.0, 1.0, Double.PositiveInfinity) 和 Array(0.0, 1.0, 2.0)。

注意,如果你并不知道目标列的上界和下界，我们应该添加Double.NegativeInfinity和Double.PositiveInfinity作为边界从而防止潜在的超过边界的异常，以下示例演示如何将一列双精度数据转换为另一个索引列。

scala\> import org.apache.spark.ml.feature.Bucketizer

import org.apache.spark.ml.feature.Bucketizer

scala\> val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5,
Double.PositiveInfinity)

splits: Array\[Double\] = Array(-Infinity, -0.5, 0.0, 0.5, Infinity)

scala\> val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)

data: Array\[Double\] = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)

scala\> val dataFrame =
spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

dataFrame: org.apache.spark.sql.DataFrame = \[features: double\]

scala\> val bucketizer = new
Bucketizer().setInputCol("features").setOutputCol("bucketedFeatures").setSplits(splits)

bucketizer: org.apache.spark.ml.feature.Bucketizer =
bucketizer\_5c90c0ea99ee

转换原始数据为桶索引：

scala\> val bucketedData = bucketizer.transform(dataFrame)

bucketedData: org.apache.spark.sql.DataFrame = \[features: double,
bucketedFeatures: double\]

scala\> println(s"Bucketizer output with
${bucketizer.getSplits.length-1} buckets")

Bucketizer output with 4 buckets

scala\> bucketedData.show()

\+--------+----------------+

|features|bucketedFeatures|

\+--------+----------------+

| -999.9| 0.0|

| -0.5| 1.0|

| -0.3| 1.0|

| 0.0| 2.0|

| 0.2| 2.0|

| 999.9| 3.0|

\+--------+----------------+

代码 4‑39

### ElementwiseProduct 

ElementwiseProduct对每一个输入向量乘以一个给定的权重向量，换句话说就是通过一个乘子对数据集的每一列进行缩放，可以表示为在输入向量\(v\)和转换向量\(w\)之间的Hadamard乘积，以产生结果向量为：

\[\begin{pmatrix}
v_{1} \\
 \vdots \\
v_{N} \\
\end{pmatrix} \circ \begin{pmatrix}
w_{1} \\
 \vdots \\
w_{N} \\
\end{pmatrix} = \begin{pmatrix}
v_{1}w_{1} \\
 \vdots \\
v_{N}w_{N} \\
\end{pmatrix}\]

公式 4‑18

下面这个例子展示了如何使用一个变换向量值转化向量。

scala\> import org.apache.spark.ml.feature.ElementwiseProduct

import org.apache.spark.ml.feature.ElementwiseProduct

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val dataFrame = spark.createDataFrame(Seq(

| ("a", Vectors.dense(1.0, 2.0, 3.0)),

| ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

dataFrame: org.apache.spark.sql.DataFrame = \[id: string, vector:
vector\]

scala\> val transformingVector = Vectors.dense(0.0, 1.0, 2.0)

transformingVector: org.apache.spark.ml.linalg.Vector = \[0.0,1.0,2.0\]

scala\> val transformer = new
ElementwiseProduct().setScalingVec(transformingVector).setInputCol("vector").setOutputCol("transformedVector")

transformer: org.apache.spark.ml.feature.ElementwiseProduct =
elemProd\_1945a75a91ff

scala\> transformer.transform(dataFrame).show()

\+---+-------------+-----------------+

| id| vector|transformedVector|

\+---+-------------+-----------------+

| a|\[1.0,2.0,3.0\]| \[0.0,2.0,6.0\]|

| b|\[4.0,5.0,6.0\]| \[0.0,5.0,12.0\]|

\+---+-------------+-----------------+

代码 4‑40

  - Hadamard乘积

在数学中，Hadamard乘积（也称为Schur乘积或entrywise乘积）是一个二元运算，它采用两个相同维数的矩阵并且相乘，并生成另一个矩阵，其中元素i,j是原始的两个矩阵元素i,j乘积。它不应该与更常见的矩阵产品混淆。这是归因于法国数学家雅克·哈达玛（Jacques
Hadamard）或德国数学家伊萨·舒尔（Issai Schur）的名字。

### SQLTransformer 

SQLTransformer实现由SQL语句定义的转换。目前只支持SQL语法，例如"SELECT ... FROM \_\_THIS\_\_
..."，其中"\_\_THIS\_\_"代表输入数据集的基础表。select子句指定字段、常量和表达式的输出进行显示，并且可以是Spark
SQL支持任何select子句。用户还可以使用Spark
SQL内置函数和用户定义函数对这些选定的列进行操作。例如，SQLTransformer支持的语句：

SELECT a, a + b AS a\_b FROM \_\_THIS\_\_

SELECT a, SQRT(b) AS b\_sqrt FROM \_\_THIS\_\_ where a \> 5

SELECT a, b, SUM(c) AS c\_sum FROM \_\_THIS\_\_ GROUP BY a, b

代码 4‑41

假设有一个DataFrame包含的列为id、v1和v2：

id | v1 | v2

\----|-----|-----

0 | 1.0 | 3.0

2 | 2.0 | 5.0

那么SQLTransformerwith语句"SELECT \*, (v1 + v2) AS v3, (v1 \* v2) AS v4 FROM
\_\_THIS\_\_"：输出为：

id | v1 | v2 | v3 | v4

\----|-----|-----|-----|-----

0 | 1.0 | 3.0 | 4.0 | 3.0

2 | 2.0 | 5.0 | 7.0 |10.0

代码 4‑42

代码如下：

scala\> import org.apache.spark.ml.feature.SQLTransformer

import org.apache.spark.ml.feature.SQLTransformer

scala\> val df = spark.createDataFrame(

| Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

df: org.apache.spark.sql.DataFrame = \[id: int, v1: double ... 1 more
field\]

scala\> val sqlTrans = new SQLTransformer().setStatement(

| "SELECT \*, (v1 + v2) AS v3, (v1 \* v2) AS v4 FROM \_\_THIS\_\_")

sqlTrans: org.apache.spark.ml.feature.SQLTransformer = sql\_50ab9032d703

scala\> sqlTrans.transform(df).show()

\+---+---+---+---+----+

| id| v1| v2| v3| v4|

\+---+---+---+---+----+

| 0|1.0|3.0|4.0| 3.0|

| 2|2.0|5.0|7.0|10.0|

\+---+---+---+---+----+

代码 4‑43

### VectorAssembler 

VectorAssembler是一个转换器，其结合给定列的列表到单一向量列。其作用是结合原始特性和不同特征转换器产生的特征到一个特征向量，用了训练机器学习模型，如逻辑回归和决策树有用。VectorAssembler接受以下输入列类型：所有数值类型、布尔型和向量类型。在每一行中，输入列的值将被按指定的顺序连接成一个向量。假设有一个DataFrame，包含的列分别为id、hour、mobile、userFeatures和clicked：

id | hour | mobile | userFeatures | clicked

\----|------|--------|------------------|---------

0 | 18 | 1.0 | \[0.0, 10.0, 0.5\] | 1.0

代码 4‑44

userFeatures是包含三个用户特征的向量列。要结合hour、mobile和userFeatures特征到一个特征向量features中，并用它来预测clicked的结果。如果设置VectorAssembler的输入列为hour、mobile以及userFeatures，输出列为features，改造后应该得到以下DataFrame：

id | hour | mobile | userFeatures | clicked | features

\----|------|--------|------------------|---------|-----------------------------

0 | 18 | 1.0 | \[0.0, 10.0, 0.5\] | 1.0 | \[18.0, 1.0, 0.0, 10.0, 0.5\]

代码如下：

scala\> import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.feature.VectorAssembler

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> val dataset = spark.createDataFrame(

| Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))

| ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

dataset: org.apache.spark.sql.DataFrame = \[id: int, hour: int ... 3
more fields\]

scala\> val assembler = new VectorAssembler().setInputCols(Array("hour",
"mobile", "userFeatures")).setOutputCol("features")

assembler: org.apache.spark.ml.feature.VectorAssembler =
vecAssembler\_65ad0e36e9ee

scala\> val output = assembler.transform(dataset)

output: org.apache.spark.sql.DataFrame = \[id: int, hour: int ... 4 more
fields\]

scala\> println("Assembled columns 'hour', 'mobile', 'userFeatures' to
vector column 'features'")

Assembled columns 'hour', 'mobile', 'userFeatures' to vector column
'features'

scala\> output.select("features", "clicked").show(false)

\+-----------------------+-------+

|features |clicked|

\+-----------------------+-------+

|\[18.0,1.0,0.0,10.0,0.5\]|1.0 |

\+-----------------------+-------+

代码 4‑45

### QuantileDiscretizer 

QuantileDiscretizer将连续型特征转换为被分箱的类别特征，分箱的数量由numBuckets参数决定，分箱的范围由渐进算法决定，可能使用的实际分箱会比这个值小，例如不同的输入值个数不足以创造足够的不同分位数。

NaN值：在QuantileDiscretizer过滤中，NaN值会从列中被除去。这将产生一个Bucketizer模型进行预测。在转换过程中，Bucketizer当发现数据集中有NaN值会产生一个错误，但用户也可以选择保留或删除数据集内的NaN值，通过设置handleInvalid。如果用户选择保留NaN值，它将被特殊处理，并放入自己的桶中。例如使用4各桶，则非NaN的数据将被放入buckets\[0-3\]，但NaN的会计数在一个特殊的bucket\[4\]中。

算法：分箱范围使用近似算法。逼近的精度可以由relativeError参数控制。如果设为零，则确切地进行分位数计算，但计算确切位数是消耗更高的计算资源，下部和上部箱边界分别是-Infinity和+Infinity，其涵盖所有实际值。假设DataFrame包含的列有id和hour：

id | hour

\----|------

0 | 18.0

\----|------

1 | 19.0

\----|------

2 | 8.0

\----|------

3 | 5.0

\----|------

4 | 2.2

代码 4‑46

hour是具有双精度类型的连续特征，想将连续特征划分为类别之一。考虑到numBuckets = 3，应该得到以下DataFrame：

id | hour | result

\----|------|------

0 | 18.0 | 2.0

\----|------|------

1 | 19.0 | 2.0

\----|------|------

2 | 8.0 | 1.0

\----|------|------

3 | 5.0 | 1.0

\----|------|------

4 | 2.2 | 0.0

代码 4‑47

代码如下：

scala\> import org.apache.spark.ml.feature.QuantileDiscretizer

import org.apache.spark.ml.feature.QuantileDiscretizer

scala\> val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4,
2.2))

data: Array\[(Int, Double)\] = Array((0,18.0), (1,19.0), (2,8.0),
(3,5.0), (4,2.2))

scala\> val df = spark.createDataFrame(data).toDF("id", "hour")

df: org.apache.spark.sql.DataFrame = \[id: int, hour: double\]

scala\> val discretizer = new
QuantileDiscretizer().setInputCol("hour").setOutputCol("result").setNumBuckets(3)

discretizer: org.apache.spark.ml.feature.QuantileDiscretizer =
quantileDiscretizer\_90ae4654fb10

scala\> val result = discretizer.fit(df).transform(df)

result: org.apache.spark.sql.DataFrame = \[id: int, hour: double ... 1
more field\]

scala\> result.show()

\+---+----+------+

| id|hour|result|

\+---+----+------+

| 0|18.0| 2.0|

| 1|19.0| 2.0|

| 2| 8.0| 1.0|

| 3| 5.0| 1.0|

| 4| 2.2| 0.0|

\+---+----+------+

代码 4‑48

### Imputer

Imputer转换器添加数据集中的丢失值，或者使用丢失值所在列的平均值或中值。输入列应该是DoubleType或FloatType。目前Imputer不支持类别特征，并可能对包含类别特征的列产生不正确的值。注意所有输入列中的null值被视为丢失，所以也被估算。假设DataFrame包含的列为a和b：

a | b

\------------|-----------

1.0 | Double.NaN

2.0 | Double.NaN

Double.NaN | 3.0

4.0 | 4.0

5.0 | 5.0

代码 4‑49

在这个例子中，Imputer将替换所有出现的Double.NaN（默认为丢失的值），使用在相应列中其它值计算的平均值（默认插补策略）。在这个例子中，对于列a和b中的替代值分别是3.0和4.0。改造后的输出列中，丢失值将被对应列的平均值替换。

a | b | out\_a | out\_b

\------------|------------|-------|-------

1.0 | Double.NaN | 1.0 | 4.0

2.0 | Double.NaN | 2.0 | 4.0

Double.NaN | 3.0 | 3.0 | 3.0

4.0 | 4.0 | 4.0 | 4.0

5.0 | 5.0 | 5.0 | 5.0

代码 4‑50

代码如下：

scala\> import org.apache.spark.ml.feature.Imputer

import org.apache.spark.ml.feature.Imputer

scala\> val df = spark.createDataFrame(Seq(

| (1.0, Double.NaN),

| (2.0, Double.NaN),

| (Double.NaN, 3.0),

| (4.0, 4.0),

| (5.0, 5.0)

| )).toDF("a", "b")

df: org.apache.spark.sql.DataFrame = \[a: double, b: double\]

scala\> val imputer = new Imputer().setInputCols(Array("a",
"b")).setOutputCols(Array("out\_a", "out\_b"))

imputer: org.apache.spark.ml.feature.Imputer = imputer\_143727445c95

scala\> val model = imputer.fit(df)

model: org.apache.spark.ml.feature.ImputerModel = imputer\_143727445c95

scala\> model.transform(df).show()

\+---+---+-----+-----+

| a| b|out\_a|out\_b|

\+---+---+-----+-----+

|1.0|NaN| 1.0| 4.0|

|2.0|NaN| 2.0| 4.0|

|NaN|3.0| 3.0| 3.0|

|4.0|4.0| 4.0| 4.0|

|5.0|5.0| 5.0| 5.0|

\+---+---+-----+-----+

代码 4‑51

## 特征选择

### VectorSlicer

VectorSlicer是转换器，获取一个特征向量，并输出由原来特征向量的子数组构成的特征向量，其是用于从向量列中提取特征。VectorSlicer接受一个指定索引的向量列，然后通过这些索引选择值输出一个新的向量列。有两种类型的索引：

（1）整数索引表示在向量中的索引，setIndices()。

（2）字符串索引代表在向量中的特征，setNames()。这就要求向量列有一个AttributeGroup，其实现了Attribute字段名称的匹配。

由整数和字符串指定都是可以接受的。此外，可以同时使用整数索引和字符串名称。至少有一个特征必须被选中。重复的特征是不允许的，所以被选择的指数和名称之间不能有重叠。请注意，如果选择特征名称，则将会在遇到空的输入属性时抛出异常。输出向量首先使用选择的索引（按照给定的顺序）调用特征，然通过所选择的名称（按照给定的顺序）调用特性。假设有DataFrameuserFeatures：

userFeatures

\------------------

\[0.0, 10.0, 0.5\]

代码 4‑52

userFeatures是包含三个用户的特征向量列。假设userFeatures的第一列全为零，所以要删除它，然后选择只有最后两列。VectorSlicer使用setIndices(1,
2)选择最后两个元素，然后产生一个名为features新向量列：

userFeatures | features

\------------------|-----------------------------

\[0.0, 10.0, 0.5\] | \[10.0, 0.5\]

代码 4‑53

假设有userFeatures潜在的输入属性，即\["f1", "f2", "f3"\]，那么可以用setNames("f2",
"f3")选择它。

userFeatures | features

\------------------|-----------------------------

\[0.0, 10.0, 0.5\] | \[10.0, 0.5\]

\["f1", "f2", "f3"\] | \["f2", "f3"\]

代码 4‑54

代码如下：

scala\> import java.util.Arrays

import java.util.Arrays

scala\> import org.apache.spark.ml.attribute.{Attribute, AttributeGroup,
NumericAttribute}

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup,
NumericAttribute}

scala\> import org.apache.spark.ml.feature.VectorSlicer

import org.apache.spark.ml.feature.VectorSlicer

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> import org.apache.spark.sql.Row

import org.apache.spark.sql.Row

scala\> import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.types.StructType

scala\> val data = Arrays.asList(

| Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),

| Row(Vectors.dense(-2.0, 2.3, 0.0))

| )

data: java.util.List\[org.apache.spark.sql.Row\] =
\[\[(3,\[0,1\],\[-2.0,2.3\])\], \[\[-2.0,2.3,0.0\]\]\]

scala\> val defaultAttr = NumericAttribute.defaultAttr

defaultAttr: org.apache.spark.ml.attribute.NumericAttribute =
{"type":"numeric"}

scala\> val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)

attrs: Array\[org.apache.spark.ml.attribute.NumericAttribute\] =
Array({"type":"numeric","name":"f1"}, {"type":"numeric","name":"f2"},
{"type":"numeric","name":"f3"})

scala\> val attrGroup = new AttributeGroup("userFeatures",
attrs.asInstanceOf\[Array\[Attribute\]\])

attrGroup: org.apache.spark.ml.attribute.AttributeGroup =
{"ml\_attr":{"attrs":{"numeric":\[{"idx":0,"name":"f1"},{"idx":1,"name":"f2"},{"idx":2,"name":"f3"}\]},"num\_attrs":3}}

scala\> val dataset = spark.createDataFrame(data,
StructType(Array(attrGroup.toStructField())))

dataset: org.apache.spark.sql.DataFrame = \[userFeatures: vector\]

scala\> val slicer = new
VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

slicer: org.apache.spark.ml.feature.VectorSlicer =
vectorSlicer\_0a05aca9c4df

scala\> slicer.setIndices(Array(1)).setNames(Array("f3"))

res35: slicer.type = vectorSlicer\_0a05aca9c4df

scala\> // or slicer.setIndices(Array(1, 2)), or
slicer.setNames(Array("f2", "f3"))

scala\> val output = slicer.transform(dataset)

output: org.apache.spark.sql.DataFrame = \[userFeatures: vector,
features: vector\]

scala\> output.show(false)

\+--------------------+-------------+

|userFeatures |features |

\+--------------------+-------------+

|(3,\[0,1\],\[-2.0,2.3\])|(2,\[0\],\[2.3\])|

|\[-2.0,2.3,0.0\] |\[2.3,0.0\] |

\+--------------------+-------------+

代码 4‑55

### RFormula 

RFormula选择由R模型公式指定的列。目前，支持R运算符的有限子集，包括‘〜’、‘.’、‘:’、‘+’和‘-’。基本的操作符含义是：

  - \~表示分离目标和术语

  - \+表示连接术语，“+ 0”表示除去截距

  - \-表示删除一个术语，“- 1”表示除去截距

  - :表示相互作用（乘法数值，或二进制化分类值）

  - 表示除了目标的所有列

假设a和b是双列，用下面这个简单的例子来说明RFormula的效果：

y \~ a + b

代码 4‑56

表示模型：

y \~ w0 + w1 \* a + w2 \* b

代码 4‑57

其中w0是截距和w1和w2为系数。

y \~ a + b + a:b – 1

代码 4‑58

表示模型：

y \~ w1 \* a + w2 \* b + w3 \* a \* b

代码 4‑59

其中w1，w2和w3为系数。

RFormula产生一个特征矢量列和一个双精度或字符串的标签列。例如，当公式被用在R语言的线性回归时，字符串输入栏将是One-hot编码，并且数字列将转换为双精度。如果标签列是字符串类型，其会使用StringIndexer先转换成双精度。如果在DataFrame中标签列不存在，输出标签列将从公式中指定的响应变量被创建。假设DataFrame包含的列为：id、country、hour和clicked：

id | country | hour | clicked

\---|---------|------|---------

7 | "US" | 18 | 1.0

8 | "CA" | 12 | 0.0

9 | "NZ" | 15 | 0.0

代码 4‑60

如果用RFormula带有公式的字符串clicked \~ country +
hour，这表明要基于country和hour预测clicked，改造后应该得到以下DataFrame：

id | country | hour | clicked | features | label

\---|---------|------|---------|------------------|-------

7 | "US" | 18 | 1.0 | \[0.0, 0.0, 18.0\] | 1.0

8 | "CA" | 12 | 0.0 | \[0.0, 1.0, 12.0\] | 0.0

9 | "NZ" | 15 | 0.0 | \[1.0, 0.0, 15.0\] | 0.0

代码 4‑61

代码如下：

scala\> import org.apache.spark.ml.feature.RFormula

import org.apache.spark.ml.feature.RFormula

scala\> val dataset = spark.createDataFrame(Seq(

| (7, "US", 18, 1.0),

| (8, "CA", 12, 0.0),

| (9, "NZ", 15, 0.0)

| )).toDF("id", "country", "hour", "clicked")

dataset: org.apache.spark.sql.DataFrame = \[id: int, country: string ...
2 more fields\]

scala\> val formula = new RFormula().setFormula("clicked \~ country +
hour").setFeaturesCol("features").setLabelCol("label")

formula: org.apache.spark.ml.feature.RFormula = RFormula(clicked \~
country + hour) (uid=rFormula\_7340c58620d2)

scala\> val output = formula.fit(dataset).transform(dataset)

output: org.apache.spark.sql.DataFrame = \[id: int, country: string ...
4 more fields\]

scala\> output.select("features", "label").show()

\+--------------+-----+

| features|label|

\+--------------+-----+

|\[0.0,0.0,18.0\]| 1.0|

|\[1.0,0.0,12.0\]| 0.0|

|\[0.0,1.0,15.0\]| 0.0|

\+--------------+-----+

代码 4‑62

### ChiSqSelector 

ChiSqSelector代表卡方特征选择，使用具有分类特征的标签数据进行操作。ChiSqSelector使用卡方独立测试来决定选择哪些特征。它支持五种选择方法：numTopFeatures、percentile、fpr、fdr、fwe：

numTopFeatures：根据卡方检验选择固定数量的顶级特征。这类似于产生具有最大预测能力的特征。

Percentile：类似于numTopFeatures，但是选择所有特征的一部分而不是固定的数字。

Fpr：选择p值低于阈值的所有特征，从而控制选择的误报率。

Fdr：使用Benjamini-Hochberg过程来选择虚假发现率低于阈值的所有特征。

Few：选择p值低于阈值的所有特征。阈值由1 / numFeatures缩放，从而控制选择的总体误差（family-wise error
rate）。默认情况下，选择方法是numTopFeatures，顶级要素的默认数量设置为50。用户可以使用setSelectorType选择一个选择方法。假设有一个DataFrame含有id、features和clicked三列，其中clicked为需要预测的目标：

id | features | clicked

\---|-----------------------|---------

7 | \[0.0, 0.0, 18.0, 1.0\] | 1.0

8 | \[0.0, 1.0, 12.0, 0.0\] | 0.0

9 | \[1.0, 0.0, 15.0, 0.1\] | 0.0

代码 4‑63

如果使用numTopFeatures = 1的ChiSqSelector，然后根据标签clicked，特征向量的最后一列被选为最有用的特征：

id | features | clicked | selectedFeatures

\---|-----------------------|---------|------------------

7 | \[0.0, 0.0, 18.0, 1.0\] | 1.0 | \[1.0\]

8 | \[0.0, 1.0, 12.0, 0.0\] | 0.0 | \[0.0\]

9 | \[1.0, 0.0, 15.0, 0.1\] | 0.0 | \[0.1\]

代码 4‑64

代码如下：

scala\> import org.apache.spark.ml.feature.ChiSqSelector

import org.apache.spark.ml.feature.ChiSqSelector

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\>

scala\> val data = Seq(

| (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),

| (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),

| (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)

| )

data: Seq\[(Int, org.apache.spark.ml.linalg.Vector, Double)\] =
List((7,\[0.0,0.0,18.0,1.0\],1.0), (8,\[0.0,1.0,12.0,0.0\],0.0),
(9,\[1.0,0.0,15.0,0.1\],0.0))

scala\>

scala\> val df = spark.createDataset(data).toDF("id", "features",
"clicked")

df: org.apache.spark.sql.DataFrame = \[id: int, features: vector ... 1
more field\]

scala\> val selector = new
ChiSqSelector().setNumTopFeatures(1).setFeaturesCol("features").setLabelCol("clicked").setOutputCol("selectedFeatures")

selector: org.apache.spark.ml.feature.ChiSqSelector =
chiSqSelector\_060a4c0d78ed

scala\> val result = selector.fit(df).transform(df)

result: org.apache.spark.sql.DataFrame = \[id: int, features: vector ...
2 more fields\]

scala\> println(s"ChiSqSelector output with top
${selector.getNumTopFeatures} features selected")

ChiSqSelector output with top 1 features selected

scala\> result.show()

\+---+------------------+-------+----------------+

| id| features|clicked|selectedFeatures|

\+---+------------------+-------+----------------+

| 7|\[0.0,0.0,18.0,1.0\]| 1.0| \[18.0\]|

| 8|\[0.0,1.0,12.0,0.0\]| 0.0| \[12.0\]|

| 9|\[1.0,0.0,15.0,0.1\]| 0.0| \[15.0\]|

\+---+------------------+-------+----------------+

代码 4‑65

  - 卡方检验

卡方检验是用途非常广的一种假设检验方法，它在分类资料统计推断中的应用，包括：两个率或两个构成比比较的卡方检验；多个率或多个构成比比较的卡方检验以及分类资料的相关分析等。卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，卡方值越大，越不符合；卡方值越小，偏差越小，越趋于符合，若两个值完全相等时，卡方值就为0，表明理论值完全符合。卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，卡方值越大，越不符合；卡方值越小，偏差越小，越趋于符合，若两个值完全相等时，卡方值就为0，表明理论值完全符合。

## 局部敏感哈希

局部敏感哈希（Locality Sensitive
Hashing，LSH）是一类重要的哈希技术，这是常见用于聚集算法，近似大型数据集的最近邻搜索和异常检测。局部敏感哈希的总体思路是使用一个家族的函数将数据点哈希到桶中，使相互靠近的数据点具有很高概率在相同的桶中，而彼此远离数据点更可能都在不同的桶中。如下的局部敏感哈希系列的正式定义。

在度量空间(M, d)中，其中M是一个集合和d是M上的距离函数，LSH家族是函数h的家族，其满足以下属性：

\[\forall p,q \in M,\]

\[d\left( p,q \right) \leq r1 \Rightarrow \Pr\left( h\left( p \right) = h\left( q \right) \right) \geq p1\]

\[d(p,q) \geq r2 \Rightarrow Pr(h(p) = h(q)) \leq p2\]

公式 4‑19

这局部敏感哈希家族被称为\((r1,\ r2,\ p1,\ p2) - sensitive\)。在Spark中，不同局部敏感哈希家族都在单独的类（如MinHash）和特征转换的API实现，在每类中提供近似相似连接和近似最近相邻。

在局部敏感哈希中，定义一个假阳性作为一对远输入特征（与\(d(p,q) \geq r2\)），该散列到相同的桶中，并且定义一个假阴性作为一对近的特征（与\(d(p,q) \leq r1\)），该散列到不同的桶中。

### 局部敏感哈希操作 

描述了局部敏感哈希可用了哪些主要类型的操作，一个被拟合的局部敏感哈希模型对这些操作提供了方法。

#### 特征变换 

特征转型是基本功能添加散列值作为一个新列，这对于降维有用。用户可通过设置指定输入和输出列名inputCol和outputCol。

局部敏感哈希也支持多个哈希表。用户可通过设置numHashTables指定哈希表的数量。这也被用于OR-amplification在大致相似性连接和近似相邻中。增加哈希表的数量会增加精度，但也会增加沟通成本和运行时间。

outputCol类型就是Seq\[Vector\]其中数组的长度等于numHashTables和向量的维度当前设置为1。在未来的版本中，将实现AND-amplification，使用户可以指定这些向量的维度。

#### 近似相似连接 

近似相似性连接得到两个数据集，并且近似返回数据集中的行对，其距离小于用户定义的阈值。近似相似连接同时支持连接两个不同的数据集和自连接。自连接会产生一些重复的对。

近似相似性连接，可同时接收转化和未转化的数据集作为输入。如果使用未转换的数据集，它会自动转换。在这种情况下，散列签名会被创建为outputCol。

在被连接的数据集中，原始数据集可以使用datasetA和datasetB被查询。距离列将被添加到输出数据集来显示每对返回行之间的真实距离。

#### 近似最近邻搜索 

近似最近邻搜索获得数据集（特征向量）和一个键（单个特征向量），并且近似返回在数据集中的指定行数，最接近向量。

近似最近邻搜索同时接受转化和未转化的数据集作为输入。如果使用未转换的数据集，它会自动转换。在这种情况下，散列签名会被创建为outputCol。

距离列将被添加到输出数据集，以显示每个输出行和搜索键之间的真实距离。

注：近似最近邻搜索将返回比k少，当没有足够的候选项在哈希桶中。

### 局部敏感哈希算法 

#### 分时段随机投影的欧氏距离 

分时段随机投影是局部敏感哈希家族的欧氏距离。欧几里德距离被定义为如下：

\[d(\mathbf{x},\mathbf{y}) = \sqrt{\sum_{i}^{}{(x_{i} - y_{i})^{2}}}\]

公式 4‑20

局部敏感哈希家族投影特征向量到随机单位向量，并且分配投影的结果到哈希桶中：

\[h\left( \mathbf{x} \right) = \left\lfloor \frac{\mathbf{x} \cdot \mathbf{v}}{r} \right\rfloor\]

公式 4‑21

其中r是一个用户定义的桶长度。桶长度可以被用于控制散列桶的平均大小（以及桶的数量）。较大的桶长度（也是指更少的桶）增加了特征被哈希到同一桶中的概率（增加真假阳性的数量）。分时段的随机投影接受任意的向量作为输入的特征，并支持稀疏和密集向量。

scala\> import org.apache.spark.ml.feature.BucketedRandomProjectionLSH

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> import org.apache.spark.sql.functions.col

import org.apache.spark.sql.functions.col

scala\> val dfA = spark.createDataFrame(Seq(

| (0, Vectors.dense(1.0, 1.0)),

| (1, Vectors.dense(1.0, -1.0)),

| (2, Vectors.dense(-1.0, -1.0)),

| (3, Vectors.dense(-1.0, 1.0))

| )).toDF("id", "features")

dfA: org.apache.spark.sql.DataFrame = \[id: int, features: vector\]

scala\> val dfB = spark.createDataFrame(Seq(

| (4, Vectors.dense(1.0, 0.0)),

| (5, Vectors.dense(-1.0, 0.0)),

| (6, Vectors.dense(0.0, 1.0)),

| (7, Vectors.dense(0.0, -1.0))

| )).toDF("id", "features")

dfB: org.apache.spark.sql.DataFrame = \[id: int, features: vector\]

scala\> val key = Vectors.dense(1.0, 0.0)

key: org.apache.spark.ml.linalg.Vector = \[1.0,0.0\]

scala\> val brp = new
BucketedRandomProjectionLSH().setBucketLength(2.0).setNumHashTables(3).setInputCol("features").setOutputCol("hashes")

brp: org.apache.spark.ml.feature.BucketedRandomProjectionLSH =
brp-lsh\_626478c9acf5

scala\> val model = brp.fit(dfA)

model: org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel =
brp-lsh\_626478c9acf5

scala\> println("The hashed dataset where hashed values are stored in
the column 'hashes':")

The hashed dataset where hashed values are stored in the column
'hashes':

scala\> model.transform(dfA).show(false)

\+---+-----------+-----------------------+

|id |features |hashes |

\+---+-----------+-----------------------+

|0 |\[1.0,1.0\] |\[\[0.0\], \[0.0\], \[-1.0\]\] |

|1 |\[1.0,-1.0\] |\[\[-1.0\], \[-1.0\], \[0.0\]\]|

|2 |\[-1.0,-1.0\]|\[\[-1.0\], \[-1.0\], \[0.0\]\]|

|3 |\[-1.0,1.0\] |\[\[0.0\], \[0.0\], \[-1.0\]\] |

\+---+-----------+-----------------------+

scala\> println("Approximately joining dfA and dfB on Euclidean distance
smaller than 1.5:")

Approximately joining dfA and dfB on Euclidean distance smaller than
1.5:

scala\> model.approxSimilarityJoin(dfA, dfB, 1.5,
"EuclideanDistance").select(col("datasetA.id").alias("idA"),

| col("datasetB.id").alias("idB"),

| col("EuclideanDistance")).show(false)

\+---+---+-----------------+

|idA|idB|EuclideanDistance|

\+---+---+-----------------+

|1 |4 |1.0 |

|0 |6 |1.0 |

|1 |7 |1.0 |

|3 |5 |1.0 |

|0 |4 |1.0 |

|3 |6 |1.0 |

|2 |7 |1.0 |

|2 |5 |1.0 |

\+---+---+-----------------+

scala\> println("Approximately searching dfA for 2 nearest neighbors of
the key:")

Approximately searching dfA for 2 nearest neighbors of the key:

scala\> model.approxNearestNeighbors(dfA, key, 2).show(false)

\+---+----------+-----------------------+-------+

|id |features |hashes |distCol|

\+---+----------+-----------------------+-------+

|0 |\[1.0,1.0\] |\[\[0.0\], \[0.0\], \[-1.0\]\] |1.0 |

|1 |\[1.0,-1.0\]|\[\[-1.0\], \[-1.0\], \[0.0\]\]|1.0 |

\+---+----------+-----------------------+-------+

代码 4‑66

#### MinHash的Jaccard距离

MinHash是局部敏感哈希家族Jaccard距离，这里输入的特征是自然数集。两组集合的Jaccard距离由它的交叉和联合的基数定义：

\[d(\mathbf{A},\mathbf{B}) = 1 - \frac{|\mathbf{A} \cap \mathbf{B}|}{|\mathbf{A} \cup \mathbf{B}|}\]

公式 4‑22

MinHash应用随机散列函数g到集合中的每个元素和获得所有散列值的最小值：

\[h\left( \mathbf{A} \right) = \min_{a \in \mathbf{A}}\left( g\left( a \right) \right)\]

公式 4‑23

MinHash输入集被表示为二级制向量，其中所述向量索引表示元素本身和非零值的向量表示集合中元素的存在。尽管密集和稀疏两种向量被支持，典型地稀疏矢量被推荐，因为效率。例如：

Vectors.sparse(10, Array\[(2, 1.0), (3, 1.0), (5, 1.0)\])

代码 4‑67

这意味在空间中有10个元素。这个集合包含元素2、元素3和元素5。所有的非零值被视为二进制“1”值。

注：MinHash不能转换空集，这意味着任何输入向量必须具有至少1非零项。

scala\> import org.apache.spark.ml.feature.MinHashLSH

import org.apache.spark.ml.feature.MinHashLSH

scala\> import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.linalg.Vectors

scala\> import org.apache.spark.sql.functions.col

import org.apache.spark.sql.functions.col

scala\> val dfA = spark.createDataFrame(Seq(

| (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),

| (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),

| (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))

| )).toDF("id", "features")

dfA: org.apache.spark.sql.DataFrame = \[id: int, features: vector\]

scala\> val dfB = spark.createDataFrame(Seq(

| (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),

| (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),

| (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))

| )).toDF("id", "features")

dfB: org.apache.spark.sql.DataFrame = \[id: int, features: vector\]

scala\> val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

key: org.apache.spark.ml.linalg.Vector = (6,\[1,3\],\[1.0,1.0\])

scala\> val mh = new
MinHashLSH().setNumHashTables(5).setInputCol("features").setOutputCol("hashes")

mh: org.apache.spark.ml.feature.MinHashLSH = mh-lsh\_923018668855

scala\> val model = mh.fit(dfA)

model: org.apache.spark.ml.feature.MinHashLSHModel =
mh-lsh\_923018668855

scala\> println("The hashed dataset where hashed values are stored in
the column 'hashes':")

The hashed dataset where hashed values are stored in the column
'hashes':

scala\> model.transform(dfA).show(false)

\+---+-------------------------+---------------------------------------------------------------------------------------+

|id |features |hashes |

\+---+-------------------------+---------------------------------------------------------------------------------------+

|0 |(6,\[0,1,2\],\[1.0,1.0,1.0\])|\[\[-2.031299587E9\],
\[-1.974869772E9\], \[-1.974047307E9\], \[4.95314097E8\],
\[7.01119548E8\]\] |

|1 |(6,\[2,3,4\],\[1.0,1.0,1.0\])|\[\[-2.031299587E9\],
\[-1.758749518E9\], \[-4.86208737E8\], \[1.247220523E9\],
\[-1.59182918E9\]\]|

|2 |(6,\[0,2,4\],\[1.0,1.0,1.0\])|\[\[-2.031299587E9\],
\[-1.758749518E9\], \[-1.974047307E9\], \[4.95314097E8\],
\[-1.59182918E9\]\]|

\+---+-------------------------+---------------------------------------------------------------------------------------+

scala\> println("Approximately joining dfA and dfB on Jaccard distance
smaller than 0.6:")

Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:

scala\> model.approxSimilarityJoin(dfA, dfB, 0.6,
"JaccardDistance").select(col("datasetA.id").alias("idA"),

| col("datasetB.id").alias("idB"),

| col("JaccardDistance")).show()

\+---+---+---------------+

|idA|idB|JaccardDistance|

\+---+---+---------------+

| 0| 5| 0.5|

| 1| 5| 0.5|

| 2| 5| 0.5|

| 1| 4| 0.5|

\+---+---+---------------+

scala\> println("Approximately searching dfA for 2 nearest neighbors of
the key:")

Approximately searching dfA for 2 nearest neighbors of the key:

scala\> model.approxNearestNeighbors(dfA, key, 2).show(false)

\+---+-------------------------+--------------------------------------------------------------------------------------+-------+

|id |features |hashes |distCol|

\+---+-------------------------+--------------------------------------------------------------------------------------+-------+

|0 |(6,\[0,1,2\],\[1.0,1.0,1.0\])|\[\[-2.031299587E9\],
\[-1.974869772E9\], \[-1.974047307E9\], \[4.95314097E8\],
\[7.01119548E8\]\]|0.75 |

\+---+-------------------------+--------------------------------------------------------------------------------------+-------+

代码 4‑68

## 小结

在本章中，学习到了Apache Spark
MLlib中用于完成特征工程的工具集。根据具体问题，执行特征选择有很多不同的选项。如TF-IDF，Word
2
Vec和Vectorizers用于文本分析问题，适合文本的特征选择；对于特征转换，可以使用各种缩放器、编码器和离散器；对于向量的子集，可以使用VectorSlicer和Chi-Square
Selector，它们使用标记的分类特征来决定选择哪些特征。



