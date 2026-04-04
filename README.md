# Spark Book 学习导航

<p align="center">
  <img src="assets/cover.png" alt="Spark 大数据处理与分析" width="420" />
</p>

本仓库以 Apache Spark 4.x 为主线，覆盖执行模型、结构化数据、流处理、机器学习、应用部署与性能优化，并保留部分历史主题用于存量系统维护与迁移理解。

## 章节导读
1. `00 概览`：先建立阅读方式和全局学习地图。  
入口：[01-怎么读这本书-通俗版.md](md/00_overview/01-怎么读这本书-通俗版.md)

2. `00 前言`：说明读者对象、学习建议和全书结构。  
入口：[01-学习建议-先易后难.md](md/00_preface/01-学习建议-先易后难.md)

3. `01 Spark 生态`：理解 Spark 在现代数据平台中的定位。  
入口：[01-本章先看懂什么.md](md/01_spark_ecosystem/01-本章先看懂什么.md)

4. `02 Spark 执行模型`：掌握 RDD 与执行流程基础。  
入口：[01-本章先看懂什么.md](md/02_spark_execution_model/01-本章先看懂什么.md)

5. `03 Pair RDD 与分区`：理解键值对算子、分区与 Shuffle。  
入口：[01-本章先看懂什么.md](md/03_pair_rdd_and_partitioning/01-本章先看懂什么.md)

6. `04 结构化数据`：Spark SQL 与 DataFrame 的核心实践。  
入口：[01-本章先看懂什么.md](md/04_structured_data/01-本章先看懂什么.md)

7. `05 流处理`：Structured Streaming 主线与 DStream 历史背景。  
入口：[01-本章先看懂什么.md](md/05_stream_processing/01-本章先看懂什么.md)

8. `06 图处理`：GraphX/Pregel 与图计算案例。  
入口：[01-本章先看懂什么.md](md/06_graph_processing/01-本章先看懂什么.md)

9. `07 机器学习`：Spark ML 的数据类型、算法与流水线。  
入口：[01-本章先看懂什么.md](md/07_machine_learning/01-本章先看懂什么.md)

10. `08 特征工程`：特征提取、转换、选择与 LSH。  
入口：[01-本章先看懂什么.md](md/08_feature_engineering/01-本章先看懂什么.md)

11. `09 算法目录`：按任务类型组织分类、回归、聚类算法。  
入口：[01-本章怎么用-通俗版.md](md/09_algorithm_catalog/01-本章怎么用-通俗版.md)

12. `10 运行应用`：构建、运行与部署 Spark 应用。  
入口：[01-本章先看懂什么.md](md/10_running_applications/01-本章先看懂什么.md)

13. `11 监控与优化`：性能调优、Shuffle、内存与实践方法。  
入口：[01-本章先看懂什么.md](md/11_monitoring_and_optimization/01-本章先看懂什么.md)

14. `12 参考资料`：参考书目与资料使用建议。  
入口：[01-使用建议.md](md/12_references/01-使用建议.md)

## 目录结构链接（md）
- [md/00_overview/](md/00_overview/)
  - [01-怎么读这本书-通俗版.md](md/00_overview/01-怎么读这本书-通俗版.md)
  - [02-一个最小例子.md](md/00_overview/02-一个最小例子.md)
- [md/00_preface/](md/00_preface/)
  - [01-学习建议-先易后难.md](md/00_preface/01-学习建议-先易后难.md)
  - [02-关于本书.md](md/00_preface/02-关于本书.md)
  - [03-本书结构.md](md/00_preface/03-本书结构.md)
  - [04-读者对象.md](md/00_preface/04-读者对象.md)
  - [05-致谢.md](md/00_preface/05-致谢.md)
- [md/01_spark_ecosystem/](md/01_spark_ecosystem/)
  - [01-本章先看懂什么.md](md/01_spark_ecosystem/01-本章先看懂什么.md)
  - [02-Spark与大模型技术发展的关系.md](md/01_spark_ecosystem/02-Spark与大模型技术发展的关系.md)
  - [03-平台设计.md](md/01_spark_ecosystem/03-平台设计.md)
  - [04-Spark简介.md](md/01_spark_ecosystem/04-Spark简介.md)
  - [05-虚拟环境.md](md/01_spark_ecosystem/05-虚拟环境.md)
  - [06-HBase技术.md](md/01_spark_ecosystem/06-HBase技术.md)
  - [07-环境部署.md](md/01_spark_ecosystem/07-环境部署.md)
  - [08-小结.md](md/01_spark_ecosystem/08-小结.md)
- [md/02_spark_execution_model/](md/02_spark_execution_model/)
  - [01-本章先看懂什么.md](md/02_spark_execution_model/01-本章先看懂什么.md)
  - [02-数据处理.md](md/02_spark_execution_model/02-数据处理.md)
  - [03-认识RDD.md](md/02_spark_execution_model/03-认识RDD.md)
  - [04-操作RDD.md](md/02_spark_execution_model/04-操作RDD.md)
  - [05-Scala编程.md](md/02_spark_execution_model/05-Scala编程.md)
  - [06-案例分析.md](md/02_spark_execution_model/06-案例分析.md)
  - [07-小结.md](md/02_spark_execution_model/07-小结.md)
- [md/03_pair_rdd_and_partitioning/](md/03_pair_rdd_and_partitioning/)
  - [01-本章先看懂什么.md](md/03_pair_rdd_and_partitioning/01-本章先看懂什么.md)
  - [02-键值对RDD.md](md/03_pair_rdd_and_partitioning/02-键值对RDD.md)
  - [03-分区和洗牌.md](md/03_pair_rdd_and_partitioning/03-分区和洗牌.md)
  - [04-共享变量.md](md/03_pair_rdd_and_partitioning/04-共享变量.md)
  - [05-Scala高级语法.md](md/03_pair_rdd_and_partitioning/05-Scala高级语法.md)
  - [06-案例分析.md](md/03_pair_rdd_and_partitioning/06-案例分析.md)
  - [07-小结.md](md/03_pair_rdd_and_partitioning/07-小结.md)
- [md/04_structured_data/](md/04_structured_data/)
  - [01-本章先看懂什么.md](md/04_structured_data/01-本章先看懂什么.md)
  - [02-Spark-SQL概述.md](md/04_structured_data/02-Spark-SQL概述.md)
  - [03-结构化数据操作.md](md/04_structured_data/03-结构化数据操作.md)
  - [04-案例分析.md](md/04_structured_data/04-案例分析.md)
  - [05-小结.md](md/04_structured_data/05-小结.md)
- [md/05_stream_processing/](md/05_stream_processing/)
  - [01-本章先看懂什么.md](md/05_stream_processing/01-本章先看懂什么.md)
  - [02-Spark-4.x-流处理主线.md](md/05_stream_processing/02-Spark-4.x-流处理主线.md)
  - [03-处理范例.md](md/05_stream_processing/03-处理范例.md)
  - [04-理解时间.md](md/05_stream_processing/04-理解时间.md)
  - [05-离散化流-DStream-历史-API.md](md/05_stream_processing/05-离散化流-DStream-历史-API.md)
  - [06-离散流的操作.md](md/05_stream_processing/06-离散流的操作.md)
  - [07-结构化流.md](md/05_stream_processing/07-结构化流.md)
  - [08-案例分析-DStream-历史案例.md](md/05_stream_processing/08-案例分析-DStream-历史案例.md)
  - [09-小结.md](md/05_stream_processing/09-小结.md)
- [md/06_graph_processing/](md/06_graph_processing/)
  - [01-本章先看懂什么.md](md/06_graph_processing/01-本章先看懂什么.md)
  - [02-理解图的概念.md](md/06_graph_processing/02-理解图的概念.md)
  - [03-图并行系统.md](md/06_graph_processing/03-图并行系统.md)
  - [04-一个例子.md](md/06_graph_processing/04-一个例子.md)
  - [05-创建和探索图.md](md/06_graph_processing/05-创建和探索图.md)
  - [06-图运算符.md](md/06_graph_processing/06-图运算符.md)
  - [07-Pregel.md](md/06_graph_processing/07-Pregel.md)
  - [08-案例分析.md](md/06_graph_processing/08-案例分析.md)
  - [09-小结.md](md/06_graph_processing/09-小结.md)
- [md/07_machine_learning/](md/07_machine_learning/)
  - [01-本章先看懂什么.md](md/07_machine_learning/01-本章先看懂什么.md)
  - [02-Spark-4.x-机器学习主线.md](md/07_machine_learning/02-Spark-4.x-机器学习主线.md)
  - [03-数据类型.md](md/07_machine_learning/03-数据类型.md)
  - [04-统计基础.md](md/07_machine_learning/04-统计基础.md)
  - [05-算法概述.md](md/07_machine_learning/05-算法概述.md)
  - [06-交叉验证.md](md/07_machine_learning/06-交叉验证.md)
  - [07-机器学习管道.md](md/07_machine_learning/07-机器学习管道.md)
  - [08-实例分析.md](md/07_machine_learning/08-实例分析.md)
  - [09-小结.md](md/07_machine_learning/09-小结.md)
- [md/08_feature_engineering/](md/08_feature_engineering/)
  - [01-本章先看懂什么.md](md/08_feature_engineering/01-本章先看懂什么.md)
  - [02-特征提取.md](md/08_feature_engineering/02-特征提取.md)
  - [03-特征转换.md](md/08_feature_engineering/03-特征转换.md)
  - [04-特征选择.md](md/08_feature_engineering/04-特征选择.md)
  - [05-局部敏感哈希.md](md/08_feature_engineering/05-局部敏感哈希.md)
  - [06-小结.md](md/08_feature_engineering/06-小结.md)
- [md/09_algorithm_catalog/](md/09_algorithm_catalog/)
  - [01-本章怎么用-通俗版.md](md/09_algorithm_catalog/01-本章怎么用-通俗版.md)
  - [02-决策树和集成.md](md/09_algorithm_catalog/02-决策树和集成.md)
  - [03-分类和回归.md](md/09_algorithm_catalog/03-分类和回归.md)
  - [04-聚类.md](md/09_algorithm_catalog/04-聚类.md)
  - [05-小结.md](md/09_algorithm_catalog/05-小结.md)
- [md/10_running_applications/](md/10_running_applications/)
  - [01-本章先看懂什么.md](md/10_running_applications/01-本章先看懂什么.md)
  - [02-SparkContext与SparkSession.md](md/10_running_applications/02-SparkContext与SparkSession.md)
  - [03-构建应用.md](md/10_running_applications/03-构建应用.md)
  - [04-部署应用.md](md/10_running_applications/04-部署应用.md)
  - [05-小结.md](md/10_running_applications/05-小结.md)
- [md/11_monitoring_and_optimization/](md/11_monitoring_and_optimization/)
  - [01-本章先看懂什么.md](md/11_monitoring_and_optimization/01-本章先看懂什么.md)
  - [02-工作原理.md](md/11_monitoring_and_optimization/02-工作原理.md)
  - [03-洗牌机制.md](md/11_monitoring_and_optimization/03-洗牌机制.md)
  - [04-内存管理.md](md/11_monitoring_and_optimization/04-内存管理.md)
  - [05-优化策略.md](md/11_monitoring_and_optimization/05-优化策略.md)
  - [06-最佳实践.md](md/11_monitoring_and_optimization/06-最佳实践.md)
  - [07-案例分析.md](md/11_monitoring_and_optimization/07-案例分析.md)
  - [08-小结.md](md/11_monitoring_and_optimization/08-小结.md)
- [md/12_references/](md/12_references/)
  - [01-使用建议.md](md/12_references/01-使用建议.md)
  - [02-图书与系统性资料.md](md/12_references/02-图书与系统性资料.md)
  - [03-使用这些资料时的原则.md](md/12_references/03-使用这些资料时的原则.md)
- [md/media/](md/media/)
