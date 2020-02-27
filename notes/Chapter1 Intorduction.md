# Spark

## 基本概念

### RDD

- 弹性分布式数据集
Resilient Distributed Database  
- 分布式内存的抽象概念，
提供了一种高度受限的共享内存模式

### DAG

- 有向无环图
Directed Acyclic Graph
- 反映RDD之间的依赖关系

### Executor

- 运行在工作节点（Worker Node）上的一个进程，负责运行任务，并为应用程序存储数据

### 应用

- 用户编写的spark应用程序

### 任务

- 运行在executor上的工作单元

### 作业

- 包含多个RDD及作用于相应RDD上的各种操作

### 阶段（任务集）

- 作业的基本调度单位

## 生态系统

### Spark Core

- 基本功能
- 内存计算、任务调度、部署模式、
故障恢复、存储管理等

### Spark SQL

- 允许开发人员直接处理RDD
- 可查询Hive、HBase等外部数据源
- 重要特点

	- 能够统一处理关系表和RDD，
使开发人员可以轻松地使用SQL命令进行查询，并进行更复杂的数据分析

### Spark Streaming

- 支持高吞吐量、可容错处理的实时流数据处理
- 将流式计算分解成一系列的批处理作业
- 支持的数据输入源

	- Kafka、Flume和TCP套接字等

### MLlib

- 提供了常用的机器学习算法的实现

	- 聚类
	- 分类
	- 回归
	- 协同过滤

### GraphX

- 用于图计算的API

## 运行架构

### 集群资源管理器
Cluster Manager

### 应用
application

- 组成

	- 任务控制节点
Driver
	- 若干个作业

### 运行作业任务的工作节点
Worker Node

- 每个工作节点上负责
具体任务的执行进程
Executor

### Hbase
HDFS

## 运行基本流程

### 1.当一个Spark应用被提交时，
首先需要为这个应用构建起基本的运行环境

- Spark Context

	- 由Driver创建一个SparkContext，由SparkContext负责和Cluster Manager的通信以及进行资源的申请、任务的分配和监控等
	- SparkContext会向Cluster Manager注册
并申请运行Executor的资源

### 2.Cluster Manager为Executor分配资源，并启动Executor进程，Executor运行情况将随着“心跳”发送到Cluster Manager上

### 3.SparkContext根据RDD的依赖关系构建DAG图

- DAG图提交给DAG调度器进行解析，将DAG图分解成多个“阶段”（每个阶段都是一个任务集），并且计算出各个阶段之间的依赖关系
- 把一个个“任务集”提交给底层的任务调度器（TaskScheduler）进行处理
- Executor向SparkContext申请任务，任务调度器将任务分发给Executor运行，同时，SparkContext将应用程序代码发放给Executor

### 4.任务在Executor上运行，把执行结果反馈给任务调度器，然后反馈给DAG调度器，运行完毕后写入数据并释放所有资源

## 三种部署方式

### standalone

- 可以独立部署到一个集群中去
- 由一个Master和若干个Salve构成
- 以槽（slot）作为资源分配单位

### Spark on Mesos

- Mesos

	- 资源调度管理框架
	- 运行在Spark之下提供服务
	- Spark程序所需要的资源都由Mesos负责调度

- 充分支持，oficially recommended

### Spark on Yarn

- Spark运行于YARN之上，与Hadoop进行统一部署

	- 好处

		- 计算资源按需伸缩
		- 不用负载应用混搭，集群利用率高
		- 共享底层存储，避免数据跨集群迁移

- 资源管理和调度依赖YARN
- 分布式存储则依赖HDFS

*XMind: ZEN - Trial Version*