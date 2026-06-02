# flink-ext

Flink 扩展库，提供了一系列增强的工具和组件，用于简化 Flink 应用的开发和管理。

## 功能特性

### 📦 核心模块

- **缓存管理 (cache)**: 基于 Caffeine 的高性能缓存聚合和归约功能
    - `CacheAggregator`: 缓存聚合器
    - `CacheReducer`: 缓存归约器
    - `JobCache`: 作业级缓存管理

- **Kafka 增强 (kafka)**: 自定义 Kafka 分区器和生产者配置
    - `BalancedFlinkKafkaPartitioner`: 负载均衡的 Kafka 分区器
    - `CustomFlinkKafkaPartitioner`: 自定义分区策略
    - `RetryableProducerCallback`: 支持重试的生产者回调

- **并发控制 (concurrent)**: 单例工厂和资源状态管理
    - `JobSingletonFactory`: 作业单例工厂
    - `ResourceState`: 资源状态管理

- **TiCDC 集成 (ticdc)**: TiDB CDC 元数据管理

- **工具类 (utils)**: 日志工具和其他辅助功能

### 🔧 主要功能

- **AbstractProgram**: 提供统一的 Flink 程序入口框架
    - 自动提取 JAR 包元数据（版本、分支、Git 提交等）
    - 参数合并与管理
    - Stream/Batch 执行环境初始化

- **自定义 Kafka Sink**: 扩展 Flink Kafka Connector
    - `CustomKafkaSink`: 自定义 Kafka Sink 实现
    - `CustomKafkaRecordSerializationSchema`: 灵活的序列化方案

## 技术栈

- **Apache Flink**: 流处理引擎
- **Apache Kafka**: 消息队列
- **Caffeine**: 高性能 Java 缓存库
- **Maven**: 构建工具

## 快速开始

### 前置条件

- JDK 8+
- Maven 3.x

### 构建项目

