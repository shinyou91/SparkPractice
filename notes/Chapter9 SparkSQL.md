# Chapter 9 Spark SQL

Structed data is any data that has a *schema* —— a known set of fields for each record.
<br>When we have this type of data, Spark SQL makes it both easier and more efficient to load ang query.

<br>

in particular, Spark provides 3 main capabilities :
1. load data from a variety of structed sources
2. let you query rhe data using SQL, both inside a Spark program and from external tools that connect to Spark SQL through standard database connectors
3. when used within a Spark program, Spark SQL provides rich integration between SQL and regular Python/Java/Scala code,
<br>including the ability to join RDDs and SQL tables, expose custom functions in SQL.

<br>

**SchemaRDD** : an RDD of Row objects, each representing an record.
- internally, they store data in a more efficient manner, taking advantage of their shcema
- provide new operations not available on RDDs, such as the ability to run SQL queries
- can be created from 
	- external data sources
	- the results of queries
	- regualr RDDs

<br>

## Linking with Spark SQL

### step 1
Spark can be built with or without Apache Hive, the Hadoop SQL engine.
<br>It is important to note that including the Hive libraries does not require an existing Hive installation.
- with Hive(general)
	- if Spark is downloaded in binary form, it should already be built with Hive support
	- if Spark is built from source, you should run `sbt/sbt -Phive assembly`
- without Hive
	- Python -> no changes to build is required
	- Java/Maven -> Maven coordinates are required
```Maven
groupId = org.apache.spark
artifactId = spark-hive_2.10	# or spark-sql_2.10
version = 1.2.0
```
<br>

### step 2

2 entry points when programming against Spark SQL :
1. (recommended) the **HiveContext** provides access to HiveQL and other Hive-dependent functionality
	- does not require an exsting Hive setup
2. (more basic) the SQLContext provides a subset of the Spark SQL support that does not depend on Hive

<br>

HiveSQL is the recommended query language for working with Spark SQL.

<br>

### step 3

copy your `hive-site.xml` file to Spark's configuration directory *$SPARK_HOME/conf*

- if you don't have an exsting Hive installation, Spark SQL will still run
	- Spark SQL will create its own Hive metastore(metadata DB) in your program's work directory called `metastore_db`
	- if you attempt to create tables using HiveQL's `CREATE TABLE` statement, they will be placed in the */suer/hive/warehouse* directory on your default filesystem

<br>

## Using Spark SQL in Applications <br>—— the most powerful way to use Spark SQL

This allows us to easily load data and query it with SQL while simultaneously combining it with program code in Python/Scala/Java.
<br>We need to construct a HiveContext(or SQLContext) based on our SparkContext.

### Initializing Spark SQL

1. add a few imports toour program

> Scala SQL imports and implicits
```scala
// Import Spar SQL
import org.apache.sql.hive.HiveContext
// or if you cannot have the hive dependencies
import org.apache.spark.sql.SQLContext
```
>> Scala users should note that we don't import `HiveContext._` like we do with the SparkContext to get access to implicits.
<br>Instead, once we have constructed an instance of the HiveContext we can then import implicits by adding following codes:

```scala
// Create a Spark SQL implicits
val HiveCtx = 
// Import the implicit conversions
import hiveCtx._
```

<br>

> python SQL imports
```python
# Import Spark SQL
from pyspark.sql import HiveContext, Row
# Or if you can't include the jive requirement
from pyspark.sql import SQLContext, Row
```

<br>

2. create a HiveContext or a SQLContext if we cannot bring in the Hive dependencies

> constructing a SQL context in Scala
```scala
val sc = new SparkContext(...)
val hiveCtx = new HiveContext(sc)
```

> constructing a SQL context in Python
```python
hiveCtx = HiveContext(sc)
```

<br>

### Basic Query Example

To make a query against a table, we call the **sql()** method on the HiveContext or SQLContext.

> loading and quering tweets in scala
```scala
val input = hiveCtx.jsonFile(inputFile)
// Register the input schema RDD
input.registerTempTable("tweets")
// Select tweets based on the retweetCount
val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM 
	tweets ORDER BY retweetCount LIMIT 10")
```

> loading and quering tweets in python
```python
input = hiveCtx.jsonFile(inputFile)
# Register the input schema RDD
input.registerTempTable('tweets')
# select tweets based on the retweetCount
topTweets = hiveCtx.sql('''SELECT text, retweetCount FROM 
	tweets ORDER BY retweetCount LIMIT 10''')
```

> if you have an existing Hive installation, and have copied your *hive-site.xml* file to *$SPARK_HOME/conf*
<br>you can also just run `hiveCtx.sql` to query your existing Hive tables

<br>

### SchemaRDDs

- Both loading data and executing queries return SchemaRDDs

- a SchemaRDD is an RDD composed of **Row** objects with additional schema information of the types in each column.
	- Row objects are just wrappers around arrays of basic types

- you can register any SchemaRDD by using `registerTempTable()` method 
<br>as a temporary table to query it via *HiveContext.sql* or *SQLContext.sql*

- SchemaRDDs can store several basic types, as well as structures and arrays of these types

<br>

Spark SQL/HiveQL type | Scala type | Java type | Python type
 :-: | :-: | :-: | :-:
TINYINT | Byte | Byte/byte | int/long<br> (in range of –128 to 127)
SMALLINT | Short | Short/short | int/long (in range of –32768 to 32767)
INT | Int | Int/int | int or long
BIGINT | Long | Long/long | long
FLOAT | Float | Float/float | float
DOUBLE | Double | Double/double | float
DECIMAL | Scala.math.BigDecimal | Java.math.BigDecimal | decimal.Decimal
STRING | String | String | string
BINARY | Array[Byte] | byte[] | bytearray
BOOLEAN | Boolean | Boolean/boolean | bool
TIMESTAMP | java.sql.TimeStamp | java.sql.TimeStamp | datetime.datetime
ARRAY<DATA_TYPE> | Seq | List | list, tuple, or array
MAP<KEY_TYPE,VAL_TYPE> | Map | Map | dict
STRUCT<COL1:COL1_TYPE, ...> | Row | Row | Row

<br>

#### Working with Row objects

Row objects represent recoreds inside SchemaRDDs, and are simply fixed-length arrays of fields.

- in Scala/Java
	- Row objects have a number of getter functions to obtain the value of each field given its index
	- the standard getter, **get**(or **apply** in Scala), takes a column number and returns an **Object** type(or **Any** in Scala) that we are responsible for casting to the correct type.
	- **getType()** method returns the type
		- Boolean, Byte, Double, Float, Int, Long, Short, and String

> accessing the text column(the first) in the topTweets SchemaRDD in Scala
```scala
val topTweetText = topTweets.map(row => row.getString(0))
```

<br>

- In python
	- just access the ***i*th** element using `row[i]`
	- Python Rows support named access to their fields, of the form `row.column_name`

> accessing the text column(the first) in the topTweets SchemaRDD in Python
```python
topTweetText = topTweets.map(lambda row : row.text)
```

<br>

### Caching

1. To make sure that we cache using the memory efficient representation, rather than the full objects, 
<br>we should use the special `hiveCtx.cahceTable("tableName")` method

when caching a table Spark SQL represents the data in an in-memory columnar format.
<br>this cached table will remain in memory only for the life of our driver program.

2. you can also cache tables using HiveQL/SQL statements
<br>simply run CAHCE TABLE tableName` or `UNCACHE TABLE tableName`

<br>

## Loading and Saving Data

1. Spark SQL supports a number of structed data sources out of the box, letting you get Row objects from them without any complicated loading process.
	- including Hive tables, JSON, and Parquet files

2. you can also convert regular RDDs in your program to SchemaRDDs by assigning them a schema
	- this makes it easy to write SQL queries even when your underlying data is Python or Java objects

3. you can easily join these RDDs with SchemaRDDs from any other Spark SQL data source

<br>

### Apache Hive

Spark SQL supports any Hive-supported storage formats, 
<br>including text files, RCFiles, ORC, Parquet, Avro, and Protocal Buffers

Our example Hive table has 2 columns, key(integer) and value(string)

> Hive load in Python
```python
from pyspark.sql import HiveContext

hiveCtx = HiveContext(sc)
rows = hiveCtx.sql('SELECT keey, value FROM mytable')
keys = rows.map(lambda row : row[0])
```

> Hive load in Scala
```scala
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
val rows = hiveCtx.sql("SELECT key, value FROM mytable")
val keys = rows.map(row => row.getInt(0))
```

<br>

### Parquet

It is often used with tools in the Hadoop ecosystem, and it supports all of the data types in Spark SQL.
<br>Spark SQL provides methods for reading data directly to and from Parquet files.

<br>

1. use `HiveContext.parquetFile` or `SQLContext.parquetFile` to load data

> Parquet load in python
```python
# load some data in from a parquet file with field's name and favoriteAnimal
rows = hiveCtx.parquetFile(parquetFile)
names = rows.map(lambda row : row.name)
print 'Everyone'
print names.collect()
```

<br>

2. you can also register a Parquet file as a Spark SQL temp table and write queries against it

> parquet query in python
```python
# find the panda lovers
tbl = rows.registerTempTable('people')
pandaFriends = hiveCtx.sql('SELECT name FROM people WHERE favoriteAnimal = \'panda\'')
print 'panda friends'
print pandaFriends.map(lambda row : row.name).collect()
```

<br>

3. save the contents of a SchemaRDD to Parquet with `saveAsParquetFile()`

> parquet file save in python
```python
pandaFriends.saveAsParquetFile('hdfs://...')
```

<br>

### JSON

If you have a JSON file with records fitting the same schema, Spark SQL can infer the schema by scanning the file and let you access fields by name.

> JSON input records
```json
{"name" : "Holden"}
{"name" : "Sparky The Bear", "lovesPandas" : true, "knows" : {"friends" : ["holden"]}}
```
<br>

- to load JSON data, all we need to do is call `hiveCtx.jsonFile()`
	- to see the inferred schema for your data, call `printSchema` on the resulting SchemaRDD

> loading JSON with Spark SQL in python
```python
input = hiveCtx.jsonFile(inputFile)
```

> loading JSON with Spark SQL in scala
```scala
val input = hiveCtx.jsonFile(inputFile)
```

<br>

> resulting schema from **printSchema()**
```
root
 |-- knows: struct (nullable = true)
 | |-- friends: array (nullable = true)
 | | |-- element: string (containsNull = false)
 |-- lovesPandas: boolean (nullable = true)
 |-- name: string (nullable = true)
```

> Partial schema of tweets
```
root
 |-- contributorsIDs: array (nullable = true)
 | |-- element: string (containsNull = false)
 |-- createdAt: string (nullable = true)
 |-- currentUserRetweetId: integer (nullable = true)
 |-- hashtagEntities: array (nullable = true)
 | |-- element: struct (containsNull = false)
 | | |-- end: integer (nullable = true)
 | | |-- start: integer (nullable = true)
 | | |-- text: string (nullable = true)
 |-- id: long (nullable = true)
 |-- inReplyToScreenName: string (nullable = true)
 |-- inReplyToStatusId: long (nullable = true)
 |-- inReplyToUserId: long (nullable = true)
 |-- isFavorited: boolean (nullable = true)
 |-- isPossiblySensitive: boolean (nullable = true)
 |-- isTruncated: boolean (nullable = true)
 |-- mediaEntities: array (nullable = true)
 | |-- element: struct (containsNull = false)
 | | |-- displayURL: string (nullable = true)
 | | |-- end: integer (nullable = true)
 | | |-- expandedURL: string (nullable = true)
 | | |-- id: long (nullable = true)
 | | |-- mediaURL: string (nullable = true)
 | | |-- mediaURLHttps: string (nullable = true)
 | | |-- sizes: struct (nullable = true)
 | | | |-- 0: struct (nullable = true)
 | | | | |-- height: integer (nullable = true)
 | | | | |-- resize: integer (nullable = true)
 | | | | |-- width: integer (nullable = true)
 | | | |-- 1: struct (nullable = true)
 | | | | |-- height: integer (nullable = true)
 | | | | |-- resize: integer (nullable = true)
 | | | | |-- width: integer (nullable = true)
 | | | |-- 2: struct (nullable = true)
 | | | | |-- height: integer (nullable = true)
 | | | | |-- resize: integer (nullable = true)
 | | | | |-- width: integer (nullable = true)
 | | | |-- 3: struct (nullable = true)
 | | | | |-- height: integer (nullable = true)
 | | | | |-- resize: integer (nullable = true)
 | | | | |-- width: integer (nullable = true)
 | | |-- start: integer (nullable = true)
 | | |-- type: string (nullable = true)
 | | |-- url: string (nullable = true)
 |-- retweetCount: integer (nullable = true)
...

```

<br>

both in python and when we register a table, we can access nested elements by using the `.` for each level of nesting.
<br>you can access array elements un SQL by specifying the index with `[elememt]`

> SQL query nested and array elements
```sql
select hashtagEntities[0].text from tweets LIMIT 1;
```

<br>

### From RDDs

- In python, we create an RDD of Row objects and then call `inferSchema()`

> Creating a SchemaRDD using Row and named tuple in python
```python
happyPeopleRDD = sc.parallelize([Row(name = 'holden', favoriteBevarage = 'coffee')])
happyPeopleSchemaRDD = hiveCtx.inferSchema(happyPeopleRDD)
happyPeopleSchemaRDD.registerTempTable('happy_people')
```

<br>

- In scala, RDDs with case classes are implicitly converted into SchemaRDDs

> creating a SchemaRDD from case class in Scala
```
case class HappyPerson(handle : String, favoriteBevarage : String)
...
// create a person and implicitly turn it into a SchemaRDD
val happyPeopleRDD = sc.parallelize(List(HappyPerson("holden", "coffee")))
happyPeopleRDD.registerTempTable('happy_people')
```

<br>

## JDBC/ODBC Server

### JDBC : Java Database Connectivity

the JDBC server runs as a standalone Spark driver program that can be shared by multiple clients

> NOTE THAT the JDBC server requires Spark be built with Hive support

- the server can be lanunched with `sbin/start-thriftserver.sh` in the Spark directory
	- this script takes many of the same options as spark-submit
	- default port : `localhost:10000`
		- we can change this with
			1. environment variables(`HIVE_SERVER2_THRIFT_PORT` and `HIVE_SERVER2_THRIFT_BIND_HOST`)
			2. Hive configuration properties(`hive.server2.thrift.port` and `hive.server2.thrift.bind.host`)
				- you can also specify them on the cmd with `--hiveconf property=value`

> launching the JDBC server
```bash
./sbin/start-thriftserver.sh --master sparkMaster
```

<br>

Spark also ships with the Beeline client program we can use to connect to our JDBC server.
<br>This is a simple SQL shell that lets us run commands on the server.

> connecting to the JDBC server with Beeline
```bash
holden@hmbp2:~/repos/spark$ ./bin/beeline -u jdbc:hive2://localhost:10000
Spark assembly has been built with Hive, including Datanucleus jars on classpath
scan complete in 1ms
Connecting to jdbc:hive2://localhost:10000
Connected to: Spark SQL (version 1.2.0-SNAPSHOT)
JDBC/ODBC Server | 175
Driver: spark-assembly (version 1.2.0-SNAPSHOT)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 1.2.0-SNAPSHOT by Apache Hive
0: jdbc:hive2://localhost:10000> show tables;
+---------+
| result |
+---------+
| pokes |
+---------+
1 row selected (1.182 seconds)
0: jdbc:hive2://localhost:10000>
```

>> if you encounter issues with queries you run against the JDBC server, check out the logs for more complete error messages

<br>

### ODBC : Open Database Connectivity

Many external tools can also connect to Spark SQL via its ODBC driver.

<br>

### Working with Beeline

within the Beeline client, you can use standard HiveQL commands to create, list, and query tables.
<br>The Beeline shell is great for quick data exploration on cached tables shared by multiple users.

1. use the **CREATE TABLE** command followed by **LOAD DATA** to create a table from local data

> load table
```sql
> CREATE TABLE IF NOT EXIST mytable (key INT, value STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
> LOAD DATA LOCAL INPATH 'learning-spark-examples/files/int_string.csv'
    INTO TABLE mytable;
```

<br>

2. use **SHOW TABLES** to list tables and use **DESCRIBE tableName** to describe each table's schema

> show tables
```sql
> SHOW TABLES;
mytable
Time taken: 0.052 seconds
```

<br>

3. use **CACHE TABLE tableName** to cache table and **UNCACHE TABLE tableName** to uncache

> NOTE THAT
>> the cached tables are shared across all clients od this JDBC server

<br>

4. run **EXPLAIN** on a given query to see what the execution plan will br

> Spark SQL shell EXPLAIN
```sql
spark-sql > EXPLAIN SELECT * FROM mytable WHERE key = 1;
== Physical Plan ==
Filter(key#16 = 1)
  HiveTableScan [key#16, value#17], (MetastoreRelation default, mytable, None), None
Time taken: 0.551 seconds
```

<br>

### Long-Lived Tables and Queries

One of the advantages of using Spark SQL's JDBC server is we can share cachedtables between multiple programs
<br> To do this, you only need to register the table and then run the CACHE command on it

> Standalone Spark SQL Shell --- most useful for local development
>> a simple shell you can use as a single process, available through *./bin/spark-sql*
<br>the shell connects to the Hive metastore you've set in *conf/hive-site.xml* or create one locally

> in a shared cluster, you should instead use the JDBC server and have users connect with beeline

<br>

## User-Defined Functions

UDFs are a very popular way to expose advanced functionality to SQL users  in an organization, so that these users can call into it without writing code.
<br>Spark SQL supports both its own UDF interface and exsiting Apache Hive UDFs

<br>

### Spark SQL UDFs

Spark SQL offers a built-in method to easily register UDFs by passing a function in your programming language.

- In scala and python
	- use the native function and lambda syntax of the language
	- need to specify the return type using one of the SchemaRDD types
		- in python we import `DataType`
<br>

> python string length UDF
```python
# make a UDF to tell us how long some text is
hiveCtx.registerFunction('strLenPython', lambda x :len(x), IntegerType())
lengthSchemaRDD = hiveCtx.sql('SELECT strLenPython('text') FROM tweets LIMIT 10')
```

> scala string length UDF
```scala
registerFunction("strLenScala", (_: String).length)
val tweetLength = hiveCtx.sql("SELECT strLenScala('tweet') FROM tweets LIMIT 10")
```

<br>

### Hive UDFs

- the standard Hive UDFs are already included
- the custom UDFs are required to be included with your application
	- if we run the JDBC server, we can add this with the `--jars` command-line flag

<br>

the Hive UDFs are not supported by SQLContext
To make a Hive UDF available, simply call
```python
hiveCtx.sql('CREATE TEMPORARY FUNCTION name AS class.function')
```

<br>

## Spark SQL performance

1. Spark SQL can easily perform conditional aggragate operations

> Spark SQL multiple sums
```sql
SELECT SUM(user.favoriteCount), SUM(retweetCount), user.id 
FROM tweets
GROUP BY user.id
```

<br>

2. Spark is able to use the knowledge of types to more efficiently represent our data

- When caching data, Spark SQL uses an in-memory columnar storage
	- take up less space
	- if subsequent queries depend only on subsets of data, it can minimize the data read

<br>

3. predicate push-down allows Spark SQL to move some parts of our query "down" to the engine we are querying

<br>

### Performance Tuning Options

Option | Default | Usage
 :-: | :-: | :-:
spark.sql.codegen | false | When true, Spark SQL will compile each query to Java bytecode on the fly. This can improve performance for large queries, but codegen can slow down very short queries.
spark.sql.inMemoryColumnarStorage.compressed | false | Compress the in-memory columnar storage automatically.
spark.sql.inMemoryColumnarStorage.batchSize | 1000 | The batch size for columnar caching. Largervalues may cause out-of-memory problems
spark.sql.parquet.compression.codec | snappy | Which compression codec to use. Possible options include uncompressed, snappy, gzip, and lzo.

<br>

we can set the performance options with the **set** command using the JDBC connector and the Beeline shell

> Beeline command for enabling codegen
```
beeline> set spark.sql.codegen = true;
SET spark.sql.codegen=true
spark.sql.codegen=true
Time taken: 1.1196 seconds
```

<br>

in a traditional Spark SQL application, we can ser these properties on our Spark configuration instead

> scala code for enabling codegen
```scala
conf.set("spark.sql.codegen", "true")
```














