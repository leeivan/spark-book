# 使用参考资料整理润色正文计划

## 目标

本计划用于指导如何利用本地参考资料提升书稿正文质量，同时保持正文的 Spark 4.x 主线清晰、语言统一、工程感稳定。

本计划只描述编辑流程，不进入书稿正文。

## 边界

- `references/` 仅作为本地资料库使用，不提交参考资料文件本体。
- 参考资料只用于核对事实、优化解释和改写案例组织，不做书摘式搬运。
- 编辑计划、资料分层、章节改写顺序不写入 `md/` 下的正文。
- 正文中只保留读者可直接消费的内容，不保留编辑备注。

## 参考资料使用规则

1. 官方文档负责核对 API、版本、行为和推荐做法。
2. 图书资料负责补背景、优化解释、增强案例的工程表达。
3. 历史资料只用于 DStream、Spark 2.x、旧版 `spark.mllib` 等迁移语境。
4. 平台专项资料如 Databricks、Dataproc、Delta Lake 只作为专题补充，不替代 Apache Spark 通用事实。
5. 每次编辑只读取当前章节直接相关的资料，避免无关扩写。

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

## 执行顺序

### 第一轮：平台与结构化主线

- `01_spark_ecosystem`
- `04_structured_data`

目标：统一现代 Spark 平台语境，讲顺 Spark SQL / DataFrame 主线。

### 第二轮：流处理与应用交付

- `05_stream_processing`
- `10_running_applications`

目标：统一 Structured Streaming、批流组织和工程交付口径。

### 第三轮：机器学习主线

- `07_machine_learning`
- `08_feature_engineering`
- `09_algorithm_catalog`

目标：统一 `spark.ml + DataFrame + Pipeline` 主线，压缩旧 API 的存在感。

### 第四轮：监控优化与全书收尾

- `11_monitoring_and_optimization`
- 全书术语、语气、图文说明通刷

目标：统一调优思路、收束全书语气、清理零散旧表述。

## 每章编辑步骤

1. 先确认本章主线和历史内容的边界。
2. 抽取与本章最相关的 1 到 3 份参考资料。
3. 优先改章首导语、关键概念解释段、案例导入、小结。
4. 再清理术语不一致、翻译腔、旧版本表述和案例收束。
5. 最后检查是否把历史内容误写回主线。

## 每章验收标准

- 章首能明确本章在 Spark 4.x 里的位置。
- 核心概念解释不依赖旧版本背景也能读懂。
- 案例说明服务于工程理解，而不是单纯罗列 API。
- 历史 API 有清晰边界，不与主线混写。
- 术语统一，表述自然，没有明显翻译腔。
- 小结能回到工程实践，而不是只重复名词。

## 重点关注点

### 第 1 章

- 平台设计
- 湖仓与对象存储
- Hive Metastore
- SQL 服务入口

### 第 4 章

- Spark SQL
- Catalyst
- DataFrame / Dataset 使用边界

### 第 5 章

- Structured Streaming 的工程语境
- DStream 的历史定位

### 第 7 至 9 章

- Pipeline
- 训练评估
- 特征工程组件关系
- 算法选型表达

### 第 10 至 11 章

- 构建、打包、提交
- 运行环境差异
- 监控先于调优
- 常见瓶颈判断路径

## 工作节奏

- 每次只处理 1 到 2 章。
- 一轮先改结构与解释，下一轮再改案例与细节。
- 每轮结束后单独提交，避免改动面过大。
- 如果发现新资料会改变全书主线判断，先暂停当前章节，重新确认边界后再继续。
