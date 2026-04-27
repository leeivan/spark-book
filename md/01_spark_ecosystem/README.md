# 01 Spark 生�?

## 学习目标
- 理解 Spark 在现代数据平台中的定位和作用
- 掌握 Spark 生态系统的核心组件及其功能
- 了解 Spark �?Hadoop 的关系及互补�?
- 熟悉 Spark 支持的编程语言和数据格�?
- 掌握 Spark 的技术特性和优势
- 了解 Spark 在大模型时代的应用场�?

## 核心概念
- **Spark Core**：Spark 的核心引擎，提供分布式任务调度和基本 I/O 功能
- **RDD**：弹性分布式数据集，Spark 的基础数据结构
- **Spark SQL**：结构化数据处理模块，主线抽象是 DataFrame / Dataset
- **Structured Streaming**：基�?Spark SQL 的流处理模块
- **MLlib**：机器学习库，主线为 `spark.ml` 管道�?API
- **GraphX**：分布式图计算组�?
- **存储系统**：HDFS、S3、HBase �?Spark 支持的存储系�?
- **集群管理�?*：Standalone、YARN、Kubernetes �?

## 本章介绍
理解 Spark 在现代数据平台中的定位�?

## 本章目录
- [01-本章先看懂什么](01-本章先看懂什�?md)
- [02-Spark与大模型技术发展的关系](02-Spark与大模型技术发展的关系.md)
- [03-平台设计](03-平台设计.md)
- [04-Spark简介](04-Spark简�?md)
- [05-虚拟环境](05-虚拟环境.md)
- [06-HBase技术](06-HBase技�?md)
- [07-环境部署](07-环境部署.md)
- [08-小结](08-小结.md)

## 练习与思考题

### 概念理解
1. 解释 Spark 在现代数据平台中的定位和作用�?
2. 描述 Spark �?Hadoop 的关系，它们是如何协同工作的�?
3. 列举 Spark 的核心组件及其主要功能�?
4. 解释 Spark 的技术特性，为什么它比传统的 MapReduce 更高效？
5. 描述 Spark 支持的编程语言及其各自的优势和适用场景�?

### 实践练习
1. 安装并配�?Spark 环境，尝试运行一个简单的 Spark 应用程序�?
2. 使用 Spark SQL 处理一�?CSV 数据集，执行基本的查询和聚合操作�?
3. 编写一�?Structured Streaming 应用，从 socket 读取数据并进行实时处理�?
4. 使用 MLlib 构建一个简单的机器学习模型，如线性回归或分类�?
5. 尝试在不同的集群管理器（�?Standalone、YARN �?Kubernetes）上部署 Spark 应用�?

### 思考讨�?
1. 为什么说 Spark 是“统一计算层”而不是“全家桶平台”？
2. 在大模型时代，Spark 扮演了什么角色？它如何支持大模型的训练和推理�?
3. 现代 Spark 平台的最小闭环需要哪些组件？为什么？
4. 如何根据业务需求选择合适的 Spark 部署模式和存储系统？
5. 比较 Scala、Python �?Java �?Spark 开发中的优缺点，你会如何选择�?

## 返回
- [返回总目录](../../README.md)


