# Chapter8 Tuning and Debugging Spark

## Configuring Spark with SparkConf

Tuning spark often simply means changing the Spark application's runtime configuration.
<br>A **SparkConf** instance is required when you are creating a new SparkContext.

> Creating an application using a SparkConf in python
```python
conf = new SparkConf()
conf.set('spark.app.name', 'my spark app')	# can also call setAppName('my spark app')
conf.set('spark.master', 'local[4]')		# can also call setMaster('local[4]')
conf.set('spark.ui.port', '36000')		# override the default port

sc = SparkContext(conf)
```

- A SparkConf instance contains key/value pairs of configuration options the user would like to override.
- to use a SparkConf object you created
	1. call **set()** to add configuration values
	2. supply it to the SparkContext constructor


- In many cases, it is more convenient to populate configurations dynamically for a given application through the `spark-submit` tool
	- the spark-submit tool provides 
		- built-in flagss for the most common Spark configuration parameters 
		- a generic `--conf` flag that accepts any Spark configuration value
	- spark-submit also supports loading configuration values from a file
		- (default) read whitespace-delimited ley/value pairs from *conf/spark-default.conf*<br>
		  using the `--properties-file` flag to spark-submit

> setting configuration values at runtime using flags
```bash
$ bin/spark-submit \
   --class com.example.MyApp \
   --master local[4] \
   --name "my spark app" \
   --conf spark.ui.port=36000 \
   myapp.jar
```

> setting configuration values at runtime using a defaults file
```bash
$ bin/spark-submit \ 
   --class com.example.MyApp \
   --properties-file my-config.conf \
   myApp.jar
```

> NOTE THAT
>> the SparkConf associated with a given application is **immutable** once it is passed to the SparkContext constructor

<br>

- In some cases, the same configuration property might be set in multiple places
	- a specific precedence order :
		1. configurations declared explicitly in the user's code using the **set()** function on a SparkConf object
		2. flags passed to spark-submit
		3. values in the properties file
		4. default values

<br>

> common spark configuration values

Options | Default | Explanation
 :-: | :-: | :-:
spark.executor.memory<br>(--executor-memory) | 512m | Amount if memory to use per executor process, in the same format as JVM memory strings
spark.executor.cores<br>(--executor-cores) | 1 | configurations for bounding the number of cores used by the application.
spark.cores.max<br>(--total-executor-cores) | (none) | in standalone mode, you can upper-bound the total number of cores across all executors
spark.speculation | false | Setting to true will enable speculative execution of tasks. <br>This means tasks that are running slowly will have a second copy launched on another node.<br>Enabling this can help cut down on straggler tasks in large clusters.
spark.storage.blockManagerTimeoutIntervalMs | 45000 | An internal timeout used for tracking the liveness of executors. <br>For jobs that have long garbage collection pauses, tuning this to be 100 seconds (a value of 100000) or higher can prevent thrashing.<br>In future versions of Spark this may be replaced with a general timeout setting, so check current documentation.
spark.executor.extraJavaOptions<br>spark.executor.extraClassPath<br>spark.executor.extraLibraryPath | (empty) | These three options allow you to customize the launch behavior of executor JVMs. <br>The three flags add extra Java options, classpath entries, or path entries for the JVM library path.<br> These parameters should be specified as strings <br>(e.g.,spark.executor.extraJavaOptions="-XX:+PrintGCDetails-XX:+PrintGCTimeStamps"). <br>Note that while this allows you to manually augment the executor classpath, the recommended way to add dependencies is through the --jars flag to spark-submit (not using this option)\
spark.serializer | org.apache.spark.serializer.JavaSerializer | Class to use for serializing objects that will be sent over the network or need to be cached in serialized form. The default of Java Serialization works with any serializable Java object but is quite slow, so we recommend using org.apache.spark.serializer.KryoSerializer and configuring Kryo serialization when speed is necessary. Can be any subclass of org.apache.spark.Serializer.
spark.[X].port | (random) | Allows setting integer port values to be used by a running Spark applications. This is useful in clusters where network access is secured. The possible values of X are driver, fileserver, broadcast, replClassServer, blockManager, and executor.
spark.eventLog.enabled | false | Set to true to enable event logging, which allows completed Spark jobs to be viewed using a history server. 
spark.eventLog.dir | file:///tmp/sparkevents | The storage location used for event logging, if enabled. This needs to be in a globally visible filesystem such as HDFS.

<br>

- almost all Spark configurations occur through the SparkConf construct, but `SPARK_LOCAL_DIRS` doesn't.
	- to set the local storage directories for spark to use for shuffle data, you export the SPARK-LOCAL-DIRS environment variable inside of *conf/spark-env.sh* to a common-seperated list of storage locations

<br>

## Components of Execution : Jobs, Tasks, and Stages

First step in tuning and debugging : have a deeper understanding of the system's internal design
- when executing, Spark translates this logical representation into a physical execution plan by merging multiple operations into tasks


> input.txt (the example source file)
```text
## input.txt ##
INFO This is a message with content
INFO This is some other content
(empty line)
INFO Here are more messages
WARN This is a warning
(empty line)
ERROR Something bad happened
WARN More details on the bad thing
INFO back to normal messages
```

open this file in the Spark shell and compute how many log messages appear at each level of severity

> processing text data in the Scala Spark shell
```scala
scala> val input = sc.textFile("input.txt")

// split words and remove empty lines
scala> val tokenized = input.map(line => line.split(" ")).filter(words => words.size > 0)

// extract the first word from each line and do a count
scala> counts = tokenized.map(words => (words(0), 1)).reduceByKey{(a,b) => a + b}

scala> counts.collect()
res0: Array[(String, Int)] = Array((ERROR,1), (INFO,4), (WARN,2), (##,1), ((empty,2))
```

- Each RDD mantains a pointer to one or more parents along with metadata about what type of relationship they have.
- after transformations, it has implicitly defined a directed acyclic graph(DAG) of RDD objects that will be used later once an action occurs

<br>

to display the lineage of an RDD, Spark provides a `toDebugString()` method

> visualizing RDDs  with toDebugString() in Scala
```scala
scala> input.toDebugString
res1: String =
(1) /home/shinyou/桌面/input.txt MapPartitionsRDD[1] at textFile at <console>:24 []
 |  /home/shinyou/桌面/input.txt HadoopRDD[0] at textFile at <console>:24 []

scala> counts.toDebugString
res2: String =
(1) ShuffledRDD[5] at reduceByKey at <console>:25 []
 +-(1) MapPartitionsRDD[4] at map at <console>:25 []
    |  MapPartitionsRDD[3] at filter at <console>:25 []
    |  MapPartitionsRDD[2] at map at <console>:25 []
    |  /home/shinyou/桌面/input.txt MapPartitionsRDD[1] at textFile at <console>:24 []
    |  /home/shinyou/桌面/input.txt HadoopRDD[0] at textFile at <console>:24 []
```

- Spark's scheduler starts at the final RDD being computed and work backwards to find what it must compute

- When the scheduler performs *piplining* or *collasping of multiple RDDs into a single stage*
	- the physical set of stages will not be  an exact 1
	- pipelining occurs when RDDs can be computed from the parents without data movement
- Spark UI : `http://localhost:4040` if running on your own machine

<br>

- Spark's internal scheduler may truncate the lineage of the RDD graph if 
	1. an existing RDD has already been persisted in cluster memory or on disk
	2. an RDD is already materialized as a side effect of an early shuffle, even if it was not explicitly persisted

- **caching** reduces the number of stages required when executing future computations


> computing an already cached RDD
```scala
// Cache the RDD
scala> counts.cache()
// The first subsequent execution will again require 2 stages
scala> counts.collect()
res87: Array[(String, Int)] = Array((ERROR,1), (INFO,4), (WARN,2), (##,1),
((empty,2))
// This execution will only require a single stage
scala> counts.collect()
res88: Array[(String, Int)] = Array((ERROR,1), (INFO,4), (WARN,2), (##,1),
((empty,2))
```

- the set of stages produced for a particular action is termed a **job**
	- in each case when we invoke actions, we are creating a job composed of one or more stages

once the stage graph is defined, tasks are created and dispatched to an internal scheduler, which varies depending on the deployment mode being used
<br>Each task internally performs the same steps :
1. **fetch its input**
<br>either form data storage, an existing RDD, or shuffle outputs
2. **perform the operation necessary to compute RDDs that it represents**
3. **write outputs**
<br>to a shuffle, to external storage, or back to the driver


Most logging and instrumentation in Spark is expressed in terms of stages, tasks and shuffles.

<br>


> SUMMARY
>> **User code defines a DAG of RDDs**
<br>operations on RDDs create new RDDs that refer back to their parents, thereby creating a graph

>> **Actions force traslation of the DAG to an execution plan**
<br>Spark's scheduler submits a job to compute all needed RDDs.
<br>A job will have one or more stages, which are parallel waves of computation composed of tasks. 
<br>Each stage will correspond to one or more RDDs in the DAG. A single stage can correspond to multiple RDDs due to *pipelining*

>> **Tasks are scheduled and executed on a cluster**
<br>stages are processed in order, which individual tasks launching to compute segments of the RDD.

<br>

## Finding Information

Applications' detailed progress information and performance metrics are presented to the user in 2 places :
1. the Spark web UI
2. the logfiles produced by the driver program and executor processes

<br>

### Spark Web UI (default port : 4040)

#### Jobs : Progress ans metrics of stages, tasks and more

contains detailed execution information for active and recently completed Spark jobs

common use : 
1. assess the performance of a job
2. look at task skew, which occurs when a small number of tasks take a very large amount of time compared to others
3. identify how much time tasks are spending in each of the phases of the task lifecycle : reading, computing and writing

<br>

#### Storage : Information for RDDs that are persisted

<br>

#### Environment : Debugging Spark's configuration

enumerates the set of active properties in the environment of your Spark application

<br>

### Driver and Executor logs

Logs contain more detailed traces of anomalous events such as internal warnings or detailed exceptions from user code.

the exact location of Spark's logfiles depend on the deployment mode :
- in standalone mode : 
	- logs are displayed in the standalone master's web UI
	- stored by default in the *work/* directory of the Spark distribution on each worker

how to customize Spark's logging : 
1. copy the example to a file called *log4j.properties*
2. modify behaviors
3. once tweaked the logging to match your desired level or format, you can add the *log4j.properties* file using the `--files` flag of spark-submit

<br>

## Key Performance Considerations

common performance issues you might encounter in Spark applications along with tips for tuning your application to get the best possible performance

### Level of Parallelism

When Spark schedules and runs tasks, it creates a single task for data stored in one partition, and that task will require a single core in the cluster to execute by default.
Spark will infer what it thinks is a good degree of parallelism for RDDs, which is sufficient for many use cases.


- the degree of parallelism can affect performance in 2 ways
	1. if there is too little parallelism, Spark might leave resources idle.
	2. if there is too much parallelism, small overheads associated with each partition can add up and become significant

<br>

- 2 ways to tune the degree of parallelism for operations
	1. during operations that shuffle data, you can always give a degree of parallelism for the produced RDD as a parameter
	2. any existing RDD can be redistributed to have more or fewer partitions
		- `repartition()` operator for randomly shuffling an RDD into the desired number of partitions
		- `claleasce()` for shrinking the RDD
			- more efficient than repartition() since it avoids a shuffle operation

<br>

By default the RDD returned by filter() will have the same size as its parent and might have many empty or small partitions
<br>In this case, you can improve the application's performance by coalescing down to a samller RDD.

> Coalescing a large RDD in the pyspark shell
```python
>>> input = sc.textFile("xxx.log")
>>> input.getNumPartitions()
35154

# add a filter that exclude almost all data
>>>lines = input.filter(lambda line : line.startswith("2014-10-17"))
>>>lines.getNumPartitions()
35154

# coalesce the lines RDD before caching
>>>lines = lines.coalesce(5).cache()
>>>lines.getNumPartitions()
4

# subsequent analysis can operate on the coalesced RDD
>>>lines.count()
```

<br>

### Serialization Format

When Spark is transferring data over the network or spilling data to disk, it needs to serialize objects into a binary format.

default : Java's built-in serializer
<br> but almost all applications will benefit from shifting to Kryo (a third-party serialization library) for serialization

- to use Kryo serialization
	1. set the `spark.serializer` setting to `org.apache.spark.serializer.KryoSerializer`
	2. register classes with Kryo that you plan to serialize
		- allow Kryo to avoid writing full class names with individual objects
		- if you want to force this registration, set `spark.kryo.registrationRequired` to **true**

> Using the Kryo serializer and registering classes
```scala
val conf = new SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

// be strict about class registration
conf.set("spark.kryo.registrationRequired", "true")
conf.registerKryoClasses(Array(classOf[MyClass], classOf[MyOtherClass]))
```

<br>

**NotSerializableException**
- your code refers to a class that does not extend Java's Serializable interface
- Many JVMs support a special option to help debug this situation : 
```
-Dsun.io.serialization.extended DebugInfo=true
```
you can enable this option using the `--driver-java-options` and `--executor-java-options` flags to spark-submit

<br>

### Memory Management

memory is used for a few purpose : 
1. **RDD storage**
	- when you call persist() or cache() on an RDD, its partitions will be stored in memory buffers
	- Spark will limit the amount of memory used when caching to a certain fraction of the JVM's overall heap, set by `spark.storage.memoryFraction`
 
2. **Shuffle and aggragation buffers**
	- when performing shuffle operations, Spark will create intermediate buffers for storing shuffle output data
	- Spark will attempt to limit the total amount of memory used in shuffle-realted buffers to `spark.shuffle.memoryFraction`

3. **User code**
	- Spark executes arbitrary user code, so user functions can themselves require substantial memory

> by default Spark will leave 60% of space for RDD storage, 20% for shuffle memory and the remaining 20% for user program

<br>

1. tewaking memory regions

2. improve certain elements of Spark's default caching behavior for some workloads
	- **cache()** operation persist memory using the **MEMORY_ONLY** storage level
		- if there is not enough space to cache new RDD partitions,
		<br>old ones will simply be deleted and recomputed if they are needed again
	- **persist()** uses the **MEMORY_AND_DISK** storage level
		- drops RDD partitions to disk and simply reads them back to memory from a local store if needed again

3. improve the default caching policy by caching serialized objects instead of raw Java objects
	- using the **MEMORY_ONLY_SER** or **MEMORY_AND_DISK_SER** storage levels
	- this will slightly slow down the cache operation due to the cost of serialization objects, 
	<br>but it can substantially reduce time spent on garbage collection in the JVM, 
	<br>since many individual records can be stored as a single serialized buffer

<br>

### Hrdware Provisioning

In all deployment mode, executor memory is set with `spark.executor.memory` or the `--executor-memory` flag to spark-submit

- In Standalone mode
	- Spark will greedily acquire as many cores and executors as are offered by the scheduler
	- supports setting `spark.cores.max` to limit the total number of cores across all executors for an application

<br>

1. Spark applications will benefit from having more memory and cores
	- spark's architecture allows for linear scaling

2. Using a large number of local disks can help accelerate the performance of Spark applications
	- in Standalone mode
		- you can set the **SPARK_LOCAL_DIRS** environment variable in **spark-env.sh** when deploying the Standalone cluster
		- Spark applications will inherit this config when they are launched
	- im all cases, you specify the local directories using a single comma-separated list
		- common to have one local dorectory for each disk volume available to Spark
		<br>, so that writes will be evenly striped across all local directories provided

<br>

- one caveat th the "more is better" guideline : when sizing memory for executor
	- Using very large heap size can cause garbage collection pauses to hurt the throughput of a Spark job
	- In standalone mode
		- you need to launch multiple workers for a single application to run more than one executor on a  host


















