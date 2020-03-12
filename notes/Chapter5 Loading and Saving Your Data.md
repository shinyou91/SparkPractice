# Chapter5 Loading and Saving Your Data

## Motivation

In particular, Spark can access data through the InputFormat and OutputFormat interfaces used by Hadoop MapReduce, which are available for many common file formats and storage systems

three common sets of data sources :
- `File formats and filesystems`
	- data stored in a local or distributed filesystem, such as NFS, HDFS, Amazon S3
	- can access a variety pf file formats including text, JSON, SequenceFiles and protocal buffers
- `Structured data sources through Spark SQL`
	- provides a nicer and often more efficient API for structured data sources, including JSON and Apache Hive.
	- more details are covered in *Chapter9*
- `Databases and key/value stores`
	- we'll sketch built-in and third-party libraries for connecting to Cassandra, HBase, Elasticsearch and JDBC database

*some libraries are Java and Scala only*


## File Formats

the input formats that Spark wraps all transparently handle compressed formats based on the file extension

common supported file formats : 

Name | Structured | Comments
:-: | :-: | :-:
text | no | plain old text files. records are assumed to be one per line
JSON | semi | Common text-based format, semistructured; most libraries require one record per line.
CSV | Yes | Very common text-based format, often used with spreadsheet applications.
SequenceFiles | Yes | A common Hadoop file format used for key/value data.
Protocol buffers | Yes | A fast, space-efficient multilanguage format.
Object files | Yes | Useful for saving data from a Spark job to be consumed by shared code. Breaks if you change your classes, as it relies on Java Serialization.

In addition to the output mechanisms supported directly in Spark, we can use bothHadoop's new and old file APIs for keyed (or paired) data
- we can use these only with key/value data because the Hadoop interfaces require it even though some formats ignore the key
- in that case we can use a **dummy key** such as null


### Text Files

- when loading a single text file as an RDD
	- each input line becomes an element in the RDD
- when loading multiple whole text files at the same time into a pair RDD
	- key being the name and the value being the contents of each file

#### loading text files

1. loading a single text file
- as simple as calling **sc.textFile(path)**
- If we want to control the number of partitions, we can also specify **minPartitions**

> python
```python
input = sc.textFile("README.md")
```

> scala
```scala
val input = sc.textFile("README.md")
```

2. multipart inputs in the form of a directory containing all of the parts can be handled in 2 ways:
- use the same **textFile()** method and pass it a directory
	- sometimes it's important to know which file which piece of input came from or we need to process an entire file at a time
- use the **SparkContext.wholeTextFiles()** method and get back a pair RDD where the key is the name of the input file
	- can be very useful when each file represents a certain time period's data

> average value per file in scala
```scala
val input = sc.wholeTextFiles("README.md")
val result = input.mapValues{y =>
	val nums = y.split(" ").map(x => x.toDouble)
	nums.sum / nums.size.toDouble
}
```

Spark supports reading all the files in a given directory and doing wildcard expansion on the input(such as part-*.txt).

#### Saving text files

**saveAsTextFiles(path)** takes in a path and Spark will output multiple files underneath that directory

```python
result.saveAsTextFiles(outputFile)
```

### JSON

#### loading JSON

1. the simplest way : **loading the data as a text file and then mapping over values with a JSON parser**

this works assuming that you have one JSON record per row

If you have multiple JSON files, you will instead have to load the whole file and then parse each file

If constructing a JSON parser is expensive in your language, you can use **mapPartitions()** to reuse the parser

<br>

2. use preferred **JSON serialization library **to write out the value to strings, which we can then write out

There are a wide variety of JSON libraries available
- in python we use the built-in library
- in java and scala we use Jackson

> loading unstructured JSON in python
```python
data = input.map(lambda x : json.loads(x))
```

In scala and Java, it's common to load records into a class representing their schemas.

> loading JSON in Scala
```scala
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

case class Person(name : String, lovesPandas : Boolean)	// must be a top-level class

val result = input.flatMap(record => {
	try{	// parse it into a specific class
		Some(mapper.readValue(record, classOf[Person]))
	} catch {	// handle errors
		case s : Exception => None
	}})
```

<br>

> handling incorrectly formatted records
>> with small datasets it can be acceptable to stop the world on malformed input
>> but with large datasets malformed input is simply part of life, we can use **accumulator** to keep track of the number of errors skipped

#### Saving JSON

much simpler compared to loading it, because we don't have to worry about incorrectly formatted data

> python
```python
(data.filter(lambda x : x['lovesPandas']).map(lambda x : json.dump(x)).savaAsTextFiles(outputFile))
```

> scala
```scala
result.filter(p => p.lovesPandas).map(mapper.writeValueAsString(_)).saveAsTextFiles(outputFile)
```

### Comma-Separated Values (CSV) and Tab-Separated Values (TSV)

- CSV files contain a fixed number of fields per line, and the fields are seperated by comma (or tab in TSV)

- records are not always stored one per line as they can sometimes span lines
	- CSV and TSV files can sometimes be inconsistent, most frequently with respect to handling newlines, escaping, and rendering non-ASCII characters, or noninteger numbers

- CSV cannot handle nested field types natively, so we have to pack and unpack to specific fields manually
 

#### Loading CSV

we can first load it as text and the process it

there are many different CSV libraries
- python -> included csv library
- scala and java -> opencsv & CSVInputFormat(doesnot support newlines)

if CSV data happens to not containing newlines in any of the fields, you can load data with textFiles() and parse it

> loading CSV with textFiles in python
```python
import csv
import StringIO

def loadRecord(line) :
	'''parse a csv line'''
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames = ['name', 'favoriteAnimal'])
	return reader.next()
input = sc.textFile(inputFile).map(loadRecord)
```

> loading CSV with textFiles() in scala
```scala
import Java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

val input = sc.textFiles(inputFile)
val result = input.map{ line =>
	val reader = new CSVReader(new StringReader(line));
	reader.readNext();
}
```

if there are embedded newlines in fields, we will need to load each file in full and parse the entire segment, which can introduce bottlenecks in loading and parsing.

> loading CSV in full in python
```python
def loadRecords(fileNameContents) :
	# load all the records in a given file
	input = StringIO.StringIO(fileNameContents[1])
	reader = csv.DictReader(input, fieldnames=["name", "favoriteAnimal"])
	return reader

fullFileData = sc.wholeTextFiles(inputFile).map(loadRecords)
```

> loading CSV in full in scala
```scala
case class Person(name : String, favoriteAnimal : String)

val input = sc.wholeTextFiles(inputFile)
val result = input.flatMap{ case (_, txt) =>
	val reader = new CSVReader(new StringReader(txt));
	reader.readAll().map(x => Person(x(0), x(1)))
}
```

If there are only a few input files, and you need to use the wholeFile() method, you may want to repartition your input to allow Spark to effectively parallelize your future operations.


#### Saving CSV

Since we don't output the field name with each record, to have consistent output we need to create a mapping,

one of the easiest ways : write a function that converts the fields to given pisitions in an array
- in python, if we are outputing dictionaries, the CSV writer can do this for us based on the order in which we provide the fieldnames when constructing the writer
- the CSV libraries we are using output to files/writers so we can use **StringWriter/StringIO** to allow us to put the result in our RDD


> writing CSV in python
```python
def writeRecords(records) : 
	# write out CSV lines
	output = StringIO.StringIO()
	writer = csv.DictWriter(output, fieldnames = ['name', 'favoriteAnimal'])
	for record in records :
		writer.writerow(record)
	return [output.getValue()]

pandaLovers.mapPartitions(writeRecords).saveAsTextFile(outputFile)
```

However, the preceding examples work only provided that we know all if the fields that we will be outputting.

-> the simplest approach is to go over all of our data and extract the distinct keys and then take another pass for output

### SequenceFiles

1. A popular Hadoop format composed of flat files with key/value pairs.

2. SequenceFiles have **sync makers** 
- allow Spark to seek to a point in the file and then resyncronize with the record boundries
- allow Spark to efficiently read SequenceFiles in parallel from multiple nodes.

3. SequenceFiles consist of elements that implement Hadoop's Writable interface, as Hadoop uses a custom serialization framework.

> some common types and their corresponding Writable class

- the standard rule of thumb : try adding the word **Writable** to the end of your class name and see if it's a known subclass of `org.apache.hadoop.io.Writable`
- if you cannot find a Writable for the data you are trying to write out, you can implement your own Writable class by overriding `readFields` and `write` from `org.apache.hadoop.io.Writable`

>> Hadoop's RecordReader reuses the same object for each record, so directly calling cache on an RDD you read in like this can fail -> instead, add a map() operation and cache its result

>> many Hadoop Writable classes don't implement `java.io.Serializable`, so for them to work in RDDs we need to convert them with a map() anyway

Scala Type | Java Type | Hadoop Writable
 :-: | :-: | :-: 
Int | Integer | IntWritable or VIntWritable
Long | Long | LongWritable or VLongWritable
Float | Float | FloatWritable
Double | Double | DoubleWritable
Boolean | Boolean | BooleanWritable
Array[Byte] | byte[ ] | BytesWritable
String | String | Text
Array[T] | T[ ] | ArrayWritable<TW>
List[T] | List<T> | ArrayWritable<TW>
Map[A, B] | map<A, B> | MapWritable<AW, BW>

<br>

we will need to use Java and Scala to define custom Writable types. 
The python Spark API knows only how to convert the basic Writables available in Hadoop to Python, and makes a best effort for other classes based on their available getter methods.

#### Loading SequenceFiles

On the SparkContext we can call **SequenceFile(path, keyClass, valueClass, minPartitions)**
- note that **keyClass** and **valueClass** both have to be the correct Writable classes

example : load people and the number of pandas they have seen from a SequenceFile

keyClass -> Text	valueClass -> IntWritable or VIntWritable

> python
```python
val data = sc.sequenceFile(inputFile, 
		'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.IntWritable')
```

> scala
```scala
val data = sc.sequenceFile(inputFile, classOf[Text], classof[IntWritable])
		.map{case (x, y) => (x.toString, y.get())}
```

In scala, there is a convenience function that can automatically convert Writable to their corresponding Scala type, instead of specifying the keyClass and valueClass
```scala
sequenceFile[Key, Value](path, minPartitions)
```

#### Saving SequenceFiles

- need a PairRDD with types that our SequenceFile can write out
- for native types, the conversion between Scala types and Hadoop Writables is implicit, so we can just save the PairRDD by just calling **saveAsSequenceFiles(path)**
- for types that don't have automatic conversion or that use variable-length
	- just map over the data and convert it before saving

```scala
val data = sc.parallelize(List(('panda',3), ('kay', 6), ('snail', 2)))
data.saveAsSequenceFile(outputFile)
```

### Object Files

- Object files are deceptively simple wrapper around SequenceFiles that allows us to save our RDDs containing just values.

- Unlike with SequenceFiles, with object files the values are written out using Java Serialization.
	- a number of implications
		- the output will be different that Hadoop outputting the same objects
		- object files are mostly intended to be used for Spark jobs communicating with the others
		- can be quite slow

- If we change classes, the old object files may no longer be readable.

- reading : **rdd = sc.objectFile()**
- saving : **rdd.saveAsObjectFile()**

- the primary reason to use object files : they require almost no work to save almost arbitrary objects

- object files are not available in Python
	- but python RDDs and SparkContext support **saveAsPickleFile()** and **pickleFile()** instead.
		- these use python's *pickle serialization library*
		- can be slow, and old files may not be readable if you change your classes

### Hadoop Input and Output Formats

Spark supports both the old and new Hadoop file APIs, providing a great amount of flexibility

#### Loading with other Hadoop input formats

the **newAPIHadoopFile** takes a path, and three classes.
- **format** class : representing our input format
	- similar function : **hadoopFile()** exists for working with Hadoop input formats with the older API
- class for our **key**
- class of our **value**


one of the simplest Hadoop input formats : **KeyValueTextInputFormat**
- used for reading in key/value data from text files
- each line is processed individually, with the key and value separated by a tab character

> loading KeyValueTextInputFormat() with old-style API in Scala
```scala
val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile).map{
	case (x, y) => (x.toString, y.toString)
}
```

many of spark's built-in convenience functions are implemented using the old-style Hadoop API

#### Non-filesystem data sources

In addition to the *hadoopFile()* and *saveAsHadoopFile()* family of functions, 
we can use **hadoopDataset/saveAsHadoopDataset** and **newAPIHadoopDataset/saveAsNewAPIHadoopDataset** to
access Hadoop-supported storage formats that are not filesystems.

the *hadoopDataset* family of functions just take a **Configuration** object on which you set the Hadoop properties needed to access your data source. 
- configuration 
	- 1. follow the instructions for accessing one of these data sources in MapReduce
	- 2. pass the object to Spark


#### Example : Protocol buffers

Protocol buffers are structed data, with the fields and types of fields being clearly defined.
They are optimized to be fast for encoding and decoding and also take up the minimum amount of space.

While a PB has a consistent encoding, there are multiple ways to create a file consisting of many PB messages.

1. Protocol Buffers are defined using a domain-specific language, and then the protocol buffer *compiler* can be used to generate accessor methods in a variety of languages.
	- as encoding the description of data formatted as PB takes up additional space, we need the protocol buffer *definition* to make sense of it.

2. PBs consist of fields that can be either optional, required or repeated.
	- a missing optional field does not result in a failure while a missing required field does
	- it's good practice to make new fields optional

3. PB fields can be many predefined types, or another PB message, including string, int32, enums and more.

> sample protocol buffer definition
```
message Venue{
	required int32 id = 1;
	required string name = 2;
	required VenueType type = 3;
	optional string address = 4;
	enum VenueType{
		COFFEESHOP = 0;
		WORKPLACE = 1;
		CLUB = 2;
		OMNOMNOM = 3;
		OTHER = 4;
	}
}

message VenueResponse {
	repeated Venue results = 1;
}
```

> Twitter's Elephant Bird protocol buffer writeout in Scala
```scala
val job = new Job()
val conf = job.getConfiguration
LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);

val dnaLounge = Places.Venue.newBuilder()
dnaLounge.setId(1);
dnaLounge.setName("DNA Lounge")
dnaLounge.setType(Places.Venue.VenueType.CLUB)

val data = sc.parallelize(List(dnaLounge.build()))
val outputData = data.map { pb =>
	val protoWritable = ProtoBufWritable.newInstance(classOf[Places.Vemue]);
	protoWritable.set(pb)
	(null, protoWritable)
}

outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text],
	classOf[ProtobufWritable[Places.Venue]],
	classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)
```


### File Compression

The compression options apply only to the Hadoop formats that support compression, namely those that are written out to a filesystem.

Formats that can be easily read from multiple machines are called 'splittable'

> compression options

formats | splittable | average compression speed | effectiveness on text | Hadoop compression codec | pure java | native | comments
 :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: 
gzip | N | Fast | High | org.apache.hadoop.io.compress.GzipCodec | Y | Y | 
lzo | Y | Very fast | Medium | com.hadoop.compression.lzo.LzoCodec | Y | Y | LZO requires installation on every worker node
bzip2 |Y | Slow | Very high | org.apache.hadoop.io.compress.BZip2Codec | Y | Y | Uses pure Java for splittable version
zlib | N | Slow | Medium | org.apache.hadoop.io.compress.DefaultCodec | Y | Y | Default compression codec for Hadoop 
Snappy | N | Very Fast | Low | org.apache.hadoop.io.compress.SnappyCodec | N | Y | There is a pure Java port of Snappy but it is not yet available in Spark/Hadoop

>> While Spark's **textFile()** method can handle compressed input, it automatically disables **splittable** even if the input is compressed auch that it could be read in a splittable way.
Therefore, use either **newAPIHadoopFile** or **hadoopFile** instead and specify the correct compression codec.


## Filesystems

### Local/"Regular" FS

**it requires that the files are available at the same path on all nodes in your cluster.**
	- if your data is already in one of the systems that are exposed to the user as a regular filesystem, you can use it as an input by just specifying a path

> loading a compressed text file from the local filesystem in Scala
```scala
val rdd = sc.textFile("xxx.gz")
```

- if your file isn't already on all nodes in the cluster
	- you can
		1. load it locally on the driver without going  through Spark
		2. then call parallelize() to distribute the contents to workers
	- slow approach
	- recommend putting files in a shared filesystem like HDFS, NFS or S3

### Amazon S3

S3 is an increasingly popular option for storing large amounts of data
- especially fast when your compute nodes are located inside of Amazon EC2
- can easily have worse performance if you have to go over the public Internet

<br>

To access S3 :
1. set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables to the S3 credentials
2. pass a path starting with *s3n://* to Spark's file input method, of the form of *s3n://bucket/path-within-bucket*
	- as with all the other filesystems, Spark supports wildcard paths for S3
- if you get an S3 access permissions error : make sure that the account for which you specified an access key has both "read" and "list" permission on the bucket

### HDFS(Hadoop Distributed File System)

- HDFS is designed to work on commodity hardware and be resilent to node failure while providing high data throughput.
- Spark and HDFS can be collocated on the same machines, and Spark can take advantage of this data locality to avoid network overhead.

- using Spark with HDFS : specifying `hdfs://master:port/path` for I/O



## Structed Data with Spark SQL

By structed data, we mean data that has a *schema* : a consistent set of fields across data records.

we give Spark SQL a SQL query to run on the data source and get back an RDD of **Row objects**, one per record.
- in Scala and Java
	1. the Row objects allow access based on the column number
	2. each Row has a **get()** method that gives back a general type we can cast, and specific get() method for common basic types
		- e.g. getFloat(), getInt(), getLong(), getString(), getShort(), getBoolean()
- in Python
	- we can just access the elements with `row[column_number]` and `row.column_name`



### Apache Hive --- one common structed data source on Hadoop

Hive can store tables in a variety of formats, from plain text to column-oriented formats, inside HDFS or other storage systems
Spark SQL can load any table supported by Hive.

- connect Spark SQL to an existing Hive installation
	1. provide a Hive configuration
		- copy your **hive-site.xml** file to Spark's **./conf/directory**
	2. create a **HiveContext** object, which is the entry point to Spark SQL
	3. write HQL(Hive Query Language) queries against your tables to get data back as RDDs of rows

> creating a HiveContext and selecting data in Python
```python
from pyspark.sql import HiveContext

hiveCtx = HiveContext(sc)
rows = hiveCtx.sql('select name, age from users')
firstRow = rows.first()
print firstRow.name
```

> creating a HiveContext and selecting data in Scala
```scala
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
val rows = hiveCtx.sql('select name,age from users')
val firstRow = rows.first()
println(firstRow.getString(0))		// field 0 is the name
```
	

### JSON

- to load JSON data
	1. create a **HiveContext** as when using Hive
		- no installation of Hive is needed in this case
	2. use the **HiveContext.jsonFile** method to get an RDD of Row objects for the whole file
		- apart from using the whole Row object, you can also register this RDD as a table and select specific fields from it

> smaple tweets in JSON -> tweets.json
```JSON
{"user" : {"name" : "Holden", "location" : "San Francisco"}, "text" : "Nice day out today"}
{"user" : {"name" : "Matei", "location" : "Berkeley"}, "text" : "even nicer here : )"}
```

> JSON loading with Spark SQL in python
```python
from pyspark.sql import HiveContext

hiveCtx = HiveContext(sc)
tweets = hiveCtx.read.json('/home/shinyou/桌面/tweets.json')
tweets.registerTempTable('tweets')
results = hiveCtx.sql('select user.name, text from tweets')

>>> results.collect()
[Row(name=u'Holden', text=u'Nice day out today'), Row(name=u'Matei', text=u'even nicer here : )')]
```

> JSON loading with Spark SQL in Scala
```scala
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
val tweets = = hiveCtx.jsonFile("tweets.json")
tweets.registerTempTable("tweets")
val results = hiveCtx.sql("SELECT user.name, text FROM tweets")
```


## Databases

Spark can access several popular databases using either their Hadoop connectors or custom Spark connectors.

### Java Database Connectivity

Spark can load data from any relational database that supports Java Database Connectivity(JDBC), including MySQL, Postgresm and other systems.

- to access this data
	1. construct an `org.apache.spark.rdd.JdbcRDD` 
	2. provide it with our SparkContext and the other parameters

> using JdbcRDD for a MySQL database in Scala
```scala
def createConnection() = {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	DriverManager.getConnection("jdbc:mysql://localhost/test?user=holden");
}

def extractValues(r : ResultSet) = {
	(r.getInt(1), r.getString(2))
}

val data = new JdbcRDD(sc, 
	createConnection, "select * from panda where ? <= id and id <= ?",
	lowerBound = 1, upperBound = 3, numPartitons = 2, mapRow = extractValues)

println(data.collect().toList)
```

- JdbcRDD takes several parameters:
	1. Spark Context
	2. a function to establish a connection to our database
		- lets each node create its own connection to load data over, after performing any configuration required to connect
	3. a query that can read a range of the data, as well as a lowerBound and upperBound value for the parameter to this query
		- allow Spark to query different ranges of the data on different machines, so we don't get bottlenecked trying to load all the data on the single node
	4. a function that converts each row of output from a **java.sql.ResultSet** to a format that is useful for manipulating our data
		- by default it convert each row to an array of objects

<br>

when using JdbcRDD, make sure that
- [x] your database can handle the load of parallel reads from Spark
- [x] if you'd like to query the data offline, you can always use your database's export features to export a text file


### Cassandra

- the connector is not currently part of Spark
	- you will need to add some further dependencies to your build file
	- only available in Java and Scala
- Cassandra doesn't yet use Spark SQL, but it returns RDDs of *CassandraRow* obbjects, which have some of the same methods as Spark SQL's Row object

> sbt requirements for Cassandra connector
```
"com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-rc5",
"com.datastax.spark" %% "spark-cassandra-connector-java" % "1.0.0-rc5"
```

> Maven requirements for Cassandra connector
```
<dependency> <!-- Cassandra -->
 <groupId>com.datastax.spark</groupId>
 <artifactId>spark-cassandra-connector</artifactId>
 <version>1.0.0-rc5</version>
</dependency>
<dependency> <!-- Cassandra -->
 <groupId>com.datastax.spark</groupId>
 <artifactId>spark-cassandra-connector-java</artifactId>
 <version>1.0.0-rc5</version>
</dependency>
```

- Cassandra connector reads a job property to determine which cluster to connect to
	- set the `spark.cassandra.connection.host` to point out the Cassandra cluster
	- set username and password (if we have) with `spark.cassandra.auth.username` and `spark.cassandra.auth.password`

> setting up Cassandra property in Scala
```Scala
val conf = new SparkConf(true)
	.set("spark.cassandra.connection.host", "hostname")
val sc = new SparkContext(conf)
```

<br>

- Cassandra cnnector uses implicits in Scala to provide additional functions on top of the SparkContext and RDDs

> loading the entire table as an RDD with key/value data in Scala
```scala
// Implicits that add functions to the SparkCcontext & RDDs
import com.datastax.spark.connector._

// assuming that table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
val data = sc.cassandraTable("test", "kv")
//print some basic stats on the value field
data.map(row => row.getInt("value")).stats()
```

- In addition to loading the entire table, we can also query subsets of our data
	-restrict data by adding a **where** clause to the **cassandraTable()
```
sc.cassandraTable(...).where("key = ?", "panda")
```

- saving Cassandra from RDDs
	- directly save RDDs of CassandraRow objects
	- save RDDs that aren't in row form as tuples and lists by specifying the column mapping

> saving to Cassandra in Scala
```scala
val rdd = sc.parallelize(List(Seq("moremagic", 1)))
rdd.saveToCassandra("test", "kv", SomeColumns("key", "value"))
```

### HBase

- Spark can access HBase through its Hadoop input format, implemented in the `org.apache.hadoop.hbase.mapreduce.TableInputFormat` class
	- returns key/value pairs
	- type
		- key : `org.apache.hadoop.hbase.io.ImmutableBytesWritable`
		- value : `org.apache.hadoop.hbase.client.Result`
			- the *Result* class includes various methods for getting values based on their column family

- to use Spark with HBase : call **SparkContext.newAPIHadoopRDD**

> reading from HBase in Scala
```scala
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

val conf = HBaseConfiguration.create()
conf.set(TableInputFormat.INPUT_TABLE, "tablename")

val rdd = sc.newAPIHadoopRDD(
	conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
```


### Elasticsearch

- Spark can both read and write data from Elasticsearch using **Elasticsearch-Hadoop**
	- the Elasticsearch connector is a bit different than the other connectors
		1. it ignores the path information we provide
			- instead depends on setting up configuration on our SparkContext
		2. the Elasticsearch OutputFormat connector also doesn't quite have the types to use Spark's wrappers
			- instead use **saveAsHadoopDataSet**, which means we need to set more properties by hand

> the latest Elasticsearch Spark connector supports returning Spark SQL rows
>> this connector is still covered, as the row conversion doesn't yet support all of the native types in Elasticsearch


> Elasticsearch output in Scala
```scala
val jobConf = new JobConf(sc.hadoopConfiguration)

jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
jobConf.setOutputCommiter(classOf[FileOutputCommiter])
jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "twitter/tweets")
jobConf.set(ConfigurationOptions.ES_NODES, "localhost")

FileOutputFormat.setOutputPath(jobConf, new Path("-"))
output.saveAsHadoopDataset(jobConf)
```


> Elasticsearch input in Scala
```scala
def mapWritableToInput(in: MapWritable): Map[String, String] = {
 in.map{case (k, v) => (k.toString, v.toString)}.toMap
}

val jobConf = new JobConf(sc.hadoopConfiguration)
jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, args(1))
jobConf.set(ConfigurationOptions.ES_NODES, args(2))
val currentTweets = sc.hadoopRDD(jobConf,
 classOf[EsInputFormat[Object, MapWritable]], classOf[Object],
 classOf[MapWritable])
// Extract only the map
// Convert the MapWritable[Text, Text] to Map[String, String]
val tweets = currentTweets.map{ case (key, value) => mapWritableToInput(value) }
```

On the write side, Elasticsearch can do mapping inference, but this can occasionally infer the types incorrectly
 -> explicitly set a mapping

















