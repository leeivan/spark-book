# 使用参考资料整理润色正文计�?

## 目标

本计划用于指导如何利用本地参考资料提升书稿正文质量，同时保持正文�?Spark 4.x 主线清晰、语言统一、工程感稳定�?

本计划只描述编辑流程，不进入书稿正文�?

## 边界

- `references/` 仅作为本地资料库使用，不提交参考资料文件本体�?
- 参考资料只用于核对事实、优化解释和改写案例组织，不做书摘式搬运�?
- 编辑计划、资料分层、章节改写顺序不写入 `md/` 下的正文�?
- 正文中只保留读者可直接消费的内容，不保留编辑备注�?

## 参考资料使用规�?

1. 官方文档负责核对 API、版本、行为和推荐做法�?
2. 图书资料负责补背景、优化解释、增强案例的工程表达�?
3. 历史资料只用�?DStream、Spark 2.x、旧�?`spark.mllib` 等迁移语境�?
4. 平台专项资料�?Databricks、Dataproc、Delta Lake 只作为专题补充，不替�?Apache Spark 通用事实�?
5. 每次编辑只读取当前章节直接相关的资料，避免无关扩写�?

## 资料分工

### 核心主线资料

- `Learning Spark`
- `High Performance Spark`
- `Practical Machine Learning with Spark`
- `Data Algorithms with Spark`

### 专题补充资料

- `Modern Data Engineering with Apache Spark`
- `Data Engineering with Apache Spark, Delta Lake`
- `Querying Databricks with Spark SQL`
- `Data Engineering with Scala and Spark`
- `Scaling Machine Learning with Spark`
- `Time Series Analysis with Spark`
- `Dataproc Cookbook`
- `Hands-on Guide to Apache Spark 3`

### 历史迁移资料

- `Apache Spark 2 for Beginners`
- `Apache Spark 2.x Machine Learning Cookbook`
- `Learning Real-time Processing with Spark Streaming`
- `Machine Learning with Spark and Python, 2nd Edition`

## 资料使用限制

### 允许补充的内�?

- `背景补强`：帮助读者理解当前章节在现代 Spark 平台中的位置�?
- `概念解释`：把核心术语、组件边界和执行逻辑讲得更自然、更工程化�?
- `案例工程化`：补充“为什么这样做”“还有哪些常见替代方案”“线上要注意什么”�?
- `平台差异`：说�?Standalone、YARN、Kubernetes、湖仓、对象存储等环境差异�?
- `历史迁移`：明确旧 API、旧范式或存量系统代码的阅读方式与迁移边界�?

### 禁止扩写的内�?

- 不把 Databricks、Dataproc、Delta Lake 的专有能力写�?Apache Spark 通用事实�?
- 不把 Spark 2.x、DStream、旧�?`spark.mllib` 资料重新写回现代主线�?
- 不把专题资料扩写成独立新章节，除非已经改变整书定位�?
- 不把书中案例改写成外部资料的书摘或变相搬运�?
- 不为补资料而打断现有章节主线；资料只能服务于当前章节目标�?

## 资料到章节映�?

### 主线章节

#### �?1 �?`01_spark_ecosystem`

- 优先资料：`Learning Spark`、`Modern Data Engineering with Apache Spark`、`Data Engineering with Apache Spark, Delta Lake`
- 辅助资料：`Querying Databricks with Spark SQL`、`Hands-on Guide to Apache Spark 3`
- 建议补充�?
  - Spark 在现代数据平台中的定�?
  - 对象存储、湖仓表格式、元数据服务的关�?
  - SQL 服务入口与统一计算层的语境
- 禁止扩写�?
  - 不把 Databricks SQL 写成 Spark SQL 的默认形�?
  - 不回到旧�?Hadoop 组件罗列

#### �?4 �?`04_structured_data`

- 优先资料：`Learning Spark`、`Querying Databricks with Spark SQL`
- 辅助资料：`Data Engineering with Apache Spark, Delta Lake`
- 建议补充�?
  - `SparkSession`、Schema、DataFrame 的统一入口视角
  - Spark SQL 在平台中的角�?
  - Catalyst / Tungsten 的工程意�?
- 禁止扩写�?
  - 不把平台厂商特性混�?Spark 通用概念
  - 不把 Dataset 写成主线默认入口

#### �?5 �?`05_stream_processing`

- 优先资料：`Learning Spark`、`Data Engineering with Scala and Spark`
- 辅助资料：`Modern Data Engineering with Apache Spark`
- 历史资料：`Learning Real-time Processing with Spark Streaming`
- 建议补充�?
  - 事件时间、水位线、状态、输出模式的工程语境
  - 流应用与批处理在交付上的相同点与差异
  - Structured Streaming �?Spark 4.x 中的默认地位
- 禁止扩写�?
  - 不让 DStream 再回到主线位�?
  - 不用旧式流处理案例替代结构化流主�?

#### �?7 �?`07_machine_learning`

- 优先资料：`Practical Machine Learning with Spark`、`Scaling Machine Learning with Spark`
- 辅助资料：`Learning Spark`
- 历史资料：`Machine Learning with Spark and Python, 2nd Edition`
- 建议补充�?
  - `spark.ml`、Pipeline、评估与调参的工程流�?
  - 推荐、分类、聚类案例的训练与验证逻辑
  - 冷启动、特征准备、预测结果解�?
- 禁止扩写�?
  - 不把 `spark.mllib` 历史 API 写回现代主线
  - 不把深度学习框架内容扩写�?Spark 原生能力

#### �?8 �?`08_feature_engineering`

- 优先资料：`Practical Machine Learning with Spark`、`Data Algorithms with Spark`
- 辅助资料：`Scaling Machine Learning with Spark`
- 建议补充�?
  - 文本特征、类别特征、数值特征处理链�?
  - `Estimator` / `Transformer` / Pipeline 串接关系
  - 特征可解释性、维度控制、缺失值处理的工程边界
- 禁止扩写�?
  - 不把单个 API 写成词条式手�?
  - 不补与当前章节主线无关的算法推导

#### �?9 �?`09_algorithm_catalog`

- 优先资料：`Data Algorithms with Spark`、`Practical Machine Learning with Spark`
- 辅助资料：`Time Series Analysis with Spark`
- 历史资料：`Apache Spark 2.x Machine Learning Cookbook`
- 建议补充�?
  - 算法选型建议
  - 输入列、输出列和适用场景
  - 基线模型与大规模场景的取�?
- 禁止扩写�?
  - 不把算法索引扩成长篇算法教材
  - 不把时间序列专题扩成主线章节

#### �?10 �?`10_running_applications`

- 优先资料：`High Performance Spark`、`Modern Data Engineering with Apache Spark`
- 辅助资料：`Data Engineering with Scala and Spark`、`Dataproc Cookbook`
- 建议补充�?
  - 构建、打包、提交、依赖分�?
  - Standalone / YARN / Kubernetes 的选择逻辑
  - 版本敏感模板与稳定正文的边界
- 禁止扩写�?
  - 不把某一云平台实践写成唯一推荐方案
  - 不堆砌过多特定版本命令模�?

#### �?11 �?`11_monitoring_and_optimization`

- 优先资料：`High Performance Spark`
- 辅助资料：`Modern Data Engineering with Apache Spark`、`Dataproc Cookbook`
- 建议补充�?
  - 先观测后调优的路�?
  - Spark UI、日志、Shuffle、内存、并行度的排障顺�?
  - 资源隔离与性能参数之间的取�?
- 禁止扩写�?
  - 不把参数列表写成配置手册
  - 不把平台监控细节写成 Spark 内核事实

### 次级章节

#### �?2 �?`02_spark_execution_model`

- 优先资料：`Learning Spark`
- 辅助资料：`Hands-on Guide to Apache Spark 3`
- 建议补充�?
  - 执行模型、谱系、缓存、Stage / Shuffle 的现代解�?
- 编辑策略�?
  - 只做精修，不大扩�?

#### �?3 �?`03_pair_rdd_and_partitioning`

- 优先资料：`Learning Spark`
- 辅助资料：`High Performance Spark`
- 建议补充�?
  - 分区器、键值对操作、Shuffle 成本的工程表�?
- 编辑策略�?
  - 只补边界与代价，不回�?RDD 为主线的写法

#### �?6 �?`06_graph_processing`

- 当前判断�?
  - 本地资料库对图计算帮助有�?
- 编辑策略�?
  - 继续�?GraphX 专题能力定位为主，不额外扩写

## 每轮补充形式

- `章首定位段`：用来补现代平台语境和章节定位�?
- `关键解释段`：用来替换旧表述、解释边界或补工程含义�?
- `案例导入/收束`：用来说明为什么这样设计、如何在项目里使用�?
- `小型提示框`：只在确有必要时加入，例如“版本相关示例”“历史兼容说明”�?

正文补充时，优先使用�?3 类；提示框只能少量使用�?

## 执行顺序

### 第一轮：平台与结构化主线

- `01_spark_ecosystem`
- `04_structured_data`

目标：统一现代 Spark 平台语境，讲�?Spark SQL / DataFrame 主线�?

### 第二轮：流处理与应用交付

- `05_stream_processing`
- `10_running_applications`

目标：统一 Structured Streaming、批流组织和工程交付口径�?

### 第三轮：机器学习主线

- `07_machine_learning`
- `08_feature_engineering`
- `09_algorithm_catalog`

目标：统一 `spark.ml + DataFrame + Pipeline` 主线，压缩旧 API 的存在感�?

### 第四轮：监控优化与全书收�?

- `11_monitoring_and_optimization`
- 全书术语、语气、图文说明通刷

目标：统一调优思路、收束全书语气、清理零散旧表述�?

## 每章编辑步骤

1. 先确认本章主线和历史内容的边界�?
2. 抽取与本章最相关�?1 �?3 份参考资料�?
3. 先决定这次只补哪一类内容：`背景补强 / 概念解释 / 案例工程�?/ 平台差异 / 历史迁移`�?
4. 优先改章首导语、关键概念解释段、案例导入、小结�?
5. 再清理术语不一致、翻译腔、旧版本表述和案例收束�?
6. 最后检查是否把历史内容误写回主线，或把专题资料写成通用事实�?

## 每章验收标准

- 章首能明确本章在 Spark 4.x 里的位置�?
- 核心概念解释不依赖旧版本背景也能读懂�?
- 案例说明服务于工程理解，而不是单纯罗�?API�?
- 历史 API 有清晰边界，不与主线混写�?
- 术语统一，表述自然，没有明显翻译腔�?
- 小结能回到工程实践，而不是只重复名词�?
- 平台专项内容被明确写成“补充视角”，没有抢走主线位置�?

## 重点关注�?

### �?1 �?

- 平台设计
- 湖仓与对象存�?
- Hive Metastore
- SQL 服务入口

### �?4 �?

- Spark SQL
- Catalyst
- DataFrame / Dataset 使用边界

### �?5 �?

- Structured Streaming 的工程语�?
- DStream 的历史定�?

### �?7 �?9 �?

- Pipeline
- 训练评估
- 特征工程组件关系
- 算法选型表达

### �?10 �?11 �?

- 构建、打包、提�?
- 运行环境差异
- 监控先于调优
- 常见瓶颈判断路径

## 工作节奏

- 每次只处�?1 �?2 章�?
- 一轮先改结构与解释，下一轮再改案例与细节�?
- 每轮结束后单独提交，避免改动面过大�?
- 如果发现新资料会改变全书主线判断，先暂停当前章节，重新确认边界后再继续�?

## 下一阶段建议

### 第一步：平台与结构化补强

- 目标章节：`01`、`04`
- 重点动作�?
  - 用现代数据平台视角收紧导语和过渡�?
  - �?Spark SQL / DataFrame 在平台中的工程角�?
  - 只少量引入湖仓、对象存储、SQL 服务入口

### 第二步：交付与调优补�?

- 目标章节：`10`、`11`
- 重点动作�?
  - �?`High Performance Spark` 补交付与调优的工程路�?
  - 把参数说明继续压缩成“先判断问题，再选抓手�?
  - 控制版本模板的篇�?

### 第三步：机器学习与特征工程补�?

- 目标章节：`07`、`08`、`09`
- 重点动作�?
  - �?Pipeline、评估、算法选型与特征链�?
  - 让案例更像项目流程，而不�?REPL 记录
  - 保持 `spark.ml + DataFrame` 为唯一主线

### 第四步：流处理细�?

- 目标章节：`05`
- 重点动作�?
  - 补事件时间、水位线、状态与输出模式的项目语�?
  - 保持 DStream 只作为历史兼容内�?









