# Spark Book 章节导航

<p align="center">
  <img src="assets/cover.png" alt="Spark 大数据处理与分析" width="420" />
</p>

本书以 Apache Spark 4.x 为基线，主线围绕四件事展开：理解 Spark 的执行与结构化处理模型，掌握批流一体的数据处理方式，建立 `spark.ml + DataFrame + Pipeline` 的机器学习工作流，以及补齐部署、监控和性能优化的工程能力。

书中仍保留部分历史内容，例如 RDD 深入、DStream、GraphX 和 `spark.mllib` 风格示例，但它们主要用于帮助读者理解 API 演进、阅读存量系统和进行迁移判断，而不是作为 Spark 4.x 新项目的默认入口。

> **版本基线（更新于 2026-02-13）**
> 本书默认适配 Apache Spark 4.1.1（稳定版），并兼容 4.0.2 维护分支。
> 推荐环境：JDK 17+（建议 JDK 21）、Scala 2.13、Python 3.10+。

## 阅读建议
- 如果你是第一次系统学习 Spark，建议按 `00 -> 01 -> 04 -> 05 -> 07 -> 08 -> 10 -> 11` 的顺序先抓主线。
- 如果你需要补底层理解，再回看 `02` 和 `03` 两章，把执行模型、RDD、分区和 Shuffle 串起来。
- 如果你在维护旧系统，可重点关注 `05` 中的 DStream 历史内容、`06` 的 GraphX 专题，以及 `07/09` 中保留的 `spark.mllib` 示例。

## 章节目录
- [00 概览](md/00_overview.md)
- [00 前言](md/00_preface.md)
- [01 Spark 生态](md/01_spark_ecosystem.md)
- [02 Spark 执行模型](md/02_spark_execution_model.md)
- [03 Pair RDD 与分区](md/03_pair_rdd_and_partitioning.md)
- [04 结构化数据](md/04_structured_data.md)
- [05 流处理](md/05_stream_processing.md)
- [06 图处理](md/06_graph_processing.md)
- [07 机器学习](md/07_machine_learning.md)
- [08 特征工程](md/08_feature_engineering.md)
- [09 算法目录](md/09_algorithm_catalog.md)
- [10 运行应用](md/10_running_applications.md)
- [11 监控与优化](md/11_monitoring_and_optimization.md)
- [12 参考资料](md/12_references.md)
