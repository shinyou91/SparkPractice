# INTRODUCTION

## spark's python and scala shells

- python version of spark sell
```
./bin/pyspark
```
- scala version of spark shell
```
./bin/spark-shell
```

- to exit the shell, press Ctrl-D

### create an RDD with a local text

- python
```python
>>> lines = sc.textFile("README.md")	#create an RDD called lines
>>> lines.count()		# the numbere of items
>>> lines.first()		# first item
``` 

- scala
```scala
scala> val lines = sc.textFile("README.md")
scala> lines.count()
scala> lines.first()
```

## Core Spark Concepts

> Driver Program
- every spark application consists of a *driver program* that launches various parallel operations on a cluster
- driver program contains the main function and defines distributed datasets on the cluster

> SparkContext
- in the shell, a *SparkContext* is automatically created as the variable called sc
- once you hava a SparkContext, you can use it to build RDDs

> Executor

### filtering example

- python
```python
>>> lines = sc.textFile("README.md")
>>> pythonLines = lines.filter(lambda line: "Python" in line)
>>> pythonLines.first()
```
- scala
```scala
scala> val lines = sc.textFile("README.md")
scala> val pythonLines = lines.filter(line => line.contains("Python"))
scala> pythonLines.first()
```

## standalone applications

### the process of linking to Spark

- java/scala->give application a Maven dependency on the spark-core artifact
```
groupId = org.apache.spark
artifactId = spaark-core_2.10
version = 1.2.0
```
- python->simply write applications as python scripts, and run then using
```
bin/spark-submit name.py
```

### initializing a SparkContext

- first create a SparkConf object to configure your applications
- the build a SparkContext for it

> python
```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
```

> scala

```scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext(conf)
```

- 2 parameters in SparkContext
	- a Cluster URL : tell the spark how to connect to a cluster
	- an application name
	- there are additional parameters->Chapter 7

- to shut down Spark
	- call sc.stop() method
	- simply exit the application

### Build Standalone WORD COUNT application

- scala file
```scala
// Create a Scala Spark Context.
val conf = new SparkConf().setAppName("wordCount")
val sc = new SparkContext(conf)

// Load our input data.
val input =  sc.textFile(inputFile)

// Split it up into words.
val words = input.flatMap(line => line.split(" "))

// Transform into pairs and count.
val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

// Save the word count back out to a text file, causing evaluation.
counts.saveAsTextFile(outputFile)
```

- sbt built file
```
name := "learning-spark-mini-example"
version := "0.0.1"
scalaversion := "2.12.7"

//additional libraries
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.2.0" % provided
)
```

- python version
```python
#test.py

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

inputs = sc.textFile("README.md")
words = inputs.flatMap(lambda line : line.split(" "))
counts = words.map(lambda word : (word,1)).reduceByKey(lambda x,y : x + y)
counts.saveAsTextFile("TEST")
```
