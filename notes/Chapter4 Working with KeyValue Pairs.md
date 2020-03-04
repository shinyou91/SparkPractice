# Working with Key/Value Pairs

key/value RDDs are commonly used to perform aggregations, and often we do some initial ETL(Extract, Transform and Load) to get our data into a key/value format

**PARTITIONING** : an advanced feature that lets users control the layout of pair RDDs across nodes

Choosing the right partitioning for a distributed dataset is similar to choosing the right data structure for a local one --- in both cases, data layout can greatly affect performance


## Motivation

- Pair RDDs are useful building block in many programs
	- they expose operations that allow you to act on each key in parallel or regroup data across the network
	- example
		- **reduceByKey()** : aggregate data seperately for each key
		- **join()** : merge 2 RDDs together by grouping elememts with the same key
	- common to extract fields from an RDD and use them as keys in pair RDD operations


## Creating Pair RDDs

- turn a regular RDD into a pair RDD
	- running a map() function that returns key/value pairs
	- example : creating a pair RDD using the first word as the key

> python --- return a tuple
```
pairs = lines.map(lambda x : (x.split(" ")[0], x))
```
> scala
```
val pairs = lines.map(x => (x.split(" ")(0), x))
```

- when creating a pair RDD from an in-memory collection in Scala and Python, we only need to call SparkContext.parallelize() on a collection of pairs


## Transformations on Pair RDDs

Since pair RDDs contain tuples, we need to pass functions that operate on tuples rather than on individual elements

example : transformations on one pair RDD {(1,2),(3,4),(3,6)}

function | purpose | example(scala) | result
- | - | - | -
reduceByKey(func) | combine values with the same key | rdd.reduceByKey((x,y) => x + y) | {(1,2),(3,10)}
groupByKey() | group values with the same key | rdd.groupByKey() | {(1,[2]),(3,[4,6])}
combineByKey(createCombiner,mergeValue,mergeCombiners,partitoner) | combine values with the same key using a different result type |  | 
mapValues(func) | apply a function to each value of a pair RDD without changing the key | rdd.mapValues(x => x + 1) | {(1,3),(3,5),(3,7)}
flatMapValues(func) | apply a function that returns an iteratorto each value of a pair RDD, and for each element returned, produce a key/valueentry with the old key. Often used for tokenization | rdd.flatMapValues(x => (x to 5) | {(1,2),(1,3),(1,4),(1,5),(3,4),(3,5)}
keys() | return an RDD of just the keys | rdd.keys() | {1,3,3}
values() | return an RDD of just the values | rdd.values() | {2,4,6}
sortByKey() | return an RDD sorted by the key | rdd.sortByKey() | {(1,2),(3,4),(3,6)}



example: transformations on 2 pair RDDs rdd = {(1,2),(3,4),(3,6)} , other = {(3,9)}


function | purpose | example(scala) | result
- | - | - | -
subtractByKey | Remove elements with a key present in the other RDD | rdd.subtractByKey(other) | {(1, 2)}
join | Perform an inner join between two RDDs | rdd.join(other){(3, (4, 9)), (3,(6, 9))} 
rightOuterJoin | Perform a join between two RDDs where the key must be present in the first RDD. | rdd.rightOuterJoin(other) | {(3,(Some(4),9)), (3,(Some(6),9))}
leftOuterJoin | Perform a join between two RDDs where the key must be present in the other RDD. | rdd.leftOuterJoin(other) | {(1,(2,None)), (3,(4,Some(9))), (3,(6,Some(9)))}
cogroup | Group data from both RDDs sharing the same key. | rdd.cogroup(other) | {(1,([2],[])), (3,([4, 6],[9]))}


- pair RDDs are also still RDDs and thus support the same function as RDDs

> python
```
result = pairs.filter(lambda keyValue : keyValue[1] < 20)
```

- sometimes we only want to access the values
	- *mapValue(func) == map(lambda (x,y) : (x,func(y)))*


### Aggregations

- **reduceByKey()** : similar to reduce(), take a function and use it to combine values
	- runs several parallel reduce operations, one for each key in the dataset, where each operation combines values that have the same key

- **foldByKey()** : similar to fold(), use a *zero value* of the same type of the data in RDD and combination function
	 - the provided zero value should have no impact when added with your combination function to another element
	- zero value is kind of a initial value

> calculate per-key average with Python
```
rdd.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))
```

- note that calling reduceByKey() and foldByKey() will automatically perform combining locally on each machine before computing global totals for each key

> word count in python
```
rdd = sc.textFile("sample.txt")
words = rdd.flatMap(lambda x : x.split(" "))
result = words.map(lambda x : (x,1)).reduceByKey(lambda x,y : x + y)
```

> even faster by using **countByValue()**
```
rdd.flatMap(lambda x : x.split(" ")).countByValue()
```

- **combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)**
	- the most general of the per-key aggregation functions, and most of the other per-key combiners are implemented using it
	- how it handles each element it processes
		- if it is a new element
			- combineByKey() will use a function called **createCombiner(value)** to create the initial value of for the accumulator on that key
			- this happens the first time a key is found in each partition, rather than only the first time the key is found in the RDD
		- if it is a value we have seen bofore while processing that partition
			- combineByKey() will use a function called **mergeValue(accumulator, value)**, with the current value for the accumulator for the key and the new value
			- *each partition is processed independently*
			- if 2 or more partitions have an accumulator for the same key, we merge the accumulators sing the **mergeCombiners(accumulator1,accumulator2)** function 
	- we can disable map-side aggregation in combineByKey() if we know that our data won't benefit from it
		- we can just use the partitioner on the source RDD by passing **rdd.partitioner**

	- per-key average examples

> python
```
>>> data = [
	('A', 2.), ('A', 4.), ('A', 9.),
	('B', 10.), ('B', 20.), 
	('Z', 3.), ('Z', 5.), ('Z', 8.), ('Z', 12.)
	]
>>> input = sc.parallelize(data)
>>> sumCount = input.combineByKey(lambda value : (value, 1),
	lambda x, value : (x[0] + value,x[1] + 1),
	lambda x, y : (x[0] + y[0], x[1] + y[1])
	)
>>> averageByKey = sumCount.map(lambda (key, (totalSum, count)) : (key, totalSum / count))
>>> averageByKey.collectAsMap()
{'A': 5.0, 'Z': 7.0, 'B': 15.0}   
```

- using specialized aggregation functions in Spark can be much faster than the maive approach of grouping our data and then reducing it

#### Tuning the level of parallelism

- every RDD has a fixed number of *partitions* that determine the degree of parallelism to use when executing operations on the RDD

- most operators accept a second parameter giving the number of partitions to use when creating the grouped or aggregated RDD

> reduceByKey() with custom parallelism in Python
```
>>> data = [
	('a', 3), ('b', 4), ('a', 1)
	]
>>> sc.parallelize(data).reduceByKey(lambda x, y : x + y).collect()
[('a', 4), ('b', 4)]
>>> sc.parallelize(data).reduceByKey(lambda x, y : x + y, 10).collect()
[('b', 4), ('a', 4)]  
```
- **repartition()** : shuffles the data across the network to create a new set of partitions
	- fairly expensive
- **coalesce()** : an optimized version of repartition()
	- allows avoiding data movement, but only when decreasing the number of RDD partitions
	- to know whether we can safely call it, we can use **rdd.getNumPartitions()** to make sure that we are coalescing it to fewer partitions than current

### Grouping Data

- **groupByKey()** : group the data using the key in our RDD
	- groupBy() works in unpaired data or data where we want to use a different condition besides equality on the current key
	- it takes another function that it applies to every element in the source RDD and uses the result to determine the key
	- *if we groupByKey() and the use a reduce() or fold() in rhe values, we can switch to a per-key aggregation function to make it more efficient*

		
	rdd.reduceByKey(func) == rdd.groupByKey().mapValues(lambda value : value.reduce(func))

- **cogroup()** : group data sharing the same key from multiple RDDs 
	- if one of the RDDs doesn't have elements for a given key that is present in other RDDs, the corresponding Iterable os simply empty
	- used as a building block for the joins we discuss in the next section

### Joins

- **inner join** : the simplest
	- only keys that are present in both pair RDDs are output

- **leftOuterJoin()** : the resulting pair RDD has entries for each key in the source RDD
	- the value associated with each key in the result is a tuple of the value from the source RDD and *Option* from the value from the other pair RDD
	- **Optional** represents a possibly missing value
		- **isPresent()** to see if it's set
		- **get()** to return the contained instance provided data is present

- **rightOuterJoin()** : identical to left one except the key must be present in the other RDD and the tuple has an option for the source rather than the other RDD

> scala example
```
storeAddress = {
  (Store("Ritual"), "1026 Valencia St"), (Store("Philz"), "748 Van Ness Ave"),
  (Store("Philz"), "3101 24th St"), (Store("Starbucks"), "Seattle")}

storeRating = {
  (Store("Ritual"), 4.9), (Store("Philz"), 4.8))}

storeAddress.join(storeRating) == {
  (Store("Ritual"), ("1026 Valencia St", 4.9)),
  (Store("Philz"), ("748 Van Ness Ave", 4.8)),
  (Store("Philz"), ("3101 24th St", 4.8))}

storeAddress.leftOuterJoin(storeRating) ==
{(Store("Ritual"),("1026 Valencia St",Some(4.9))),
  (Store("Starbucks"),("Seattle",None)),
  (Store("Philz"),("748 Van Ness Ave",Some(4.8))),
  (Store("Philz"),("3101 24th St",Some(4.8)))}

storeAddress.rightOuterJoin(storeRating) ==
{(Store("Ritual"),(Some("1026 Valencia St"),4.9)),
  (Store("Philz"),(Some("748 Van Ness Ave"),4.8)),
  (Store("Philz"), (Some("3101 24th St"),4.8))}
```

### Sorting Data

once we have sorted our data, any subsequent call on the sorted data to collect() or save() will result in ordered data

the **sortByKey()** function takes a boolean parameter called *ascending* (default : True)

> sorting integers as if strings in python
```
rdd.sortByKey(acsending = True, numPartitions = None, Keyfunc = lambda x : str(x))
```

## Actions Available on Pair RDDs

all of the traditional actions available on the base RDD are also available on pair RDDs

example :  {(1,2),(3,4),(3,6)}

function | discription | example(scala) | result
- | - | - | -
countByKey() | Count the number of elements for each key. | rdd.countByKey() | {(1, 1), (3, 2)}
collectAsMap() | Collect the result as a map to provide easy lookup. | rdd.collectAsMap() | Map {(1, 2), (3, 4), (3, 6)}
lookup(key) | Return all values associated with the provided key. | rdd.lookup(3) | [4, 6]

## Data Partitioning(Advanced)

> how to control dataset's  partitioning across nodes

In a distributed program, communication is very expensive, so laying out data to minimize network traffic can greatly improve performance.

If a given RDD is scanned only once, there is no point in partitioning it in advance.

It is useful only when a dataset is reused multiple times in key-oriented operations such as joins.

Spark's partitioning is available on all RDDs of key/value pairs, and causes the system to group elements based on a function of each key.

<br>

> scala sample application : count how many user visited a link that was not to ine of their subscribed topics
```
val sc = new SparkContext(...)

// load the user info from a Hadoop SequenceFile on HDFS
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist()

// function called periodically to process a logfile of events in the past 5 minutes
// assuming that this is a sequenceFile containing (UserID, UserInfo) pairs
def processNewLogs(logFileName : String){
	val events = sc.sequenceFile[UserID, LinkInfo](logFileName)
	val joined =  userData.join(events)	//RDD of (UserID, (UserInfo, LinkInfo))
	
	val offTopicVisits = joined.filter{
		case(userId, (userInfo, linkInfo)) =>
		!userInfo.topics.contains(linkInfo.topic)
	}.count()

	println('number of visits to non-subscribed topics: ' + offTopicVisits)
}
```

However, this code is inefficient because the join() operation which is called each time processNewLogs() is invoked, does not know how the keys are partitioned in the datasets, so that the userData table is hashed and shuffled across the network on every call, even though it doesn't change.

SOLUTION 

-> use the **partitionBy()** transformation on userData to hash-partition it at the start of the program

> version 2
```
val sc = new SparkContext(...)
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...")
		.partitionBy(new HashPartitioner(100))		// create 100 partitions
		.persist()

// processNewLogs() unchanged
```

In particular, when we call userData.join(events) Spark will shuffle only the events RDD so that a lot of data is communicated over the network.

NOTE THAT:
- partitionBy() is a transformation
- RDDs can never be modified once created
- it is important to persist and save as userData the result of partitionBy()


in addition, failure to persist an RDD after transformed with partitionBy() will cause
- subsequent uses of the RDD to repeat the partitioning of data
- reevaluation if the RDDs complete lineage
- negate the advantage of partitionBy()


<br>


1. in fact, many Spark operations automatically result in a RDD with known partitioning information, and many operations will take advantage of this information
2. operations like map() cause the new RDD to **forget** the parent's partitioning information, because such operations could theoretically modify the key of each record
3. In Python, you cannot pass a *HashPartitioner* object to partitionBy(); instead, use the number of partitions desired
```
rdd.partitionBy(100)
```

### Determining an RDD's Partitioner

in Scala, we can determine how an RDD is partitioned using **partitioner property**
- a great way to test how different Spark operations affect partitioning and to check that the operations you want to do in your program will yield the right result
- returns a **scala.Option** object 
	- a Scala class for a container that may or may not contain one item
	- the value will be a **spark.Partitioner** object
		- call **isDefined()** to check whether it has a value
		- call **get()** to get this value

```
// don't forget to import

scala> import org.apache.spark._
import org.apache.spark._

scala> import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext._

scala> val pairs = sc.parallelize(List((1,1),(2,2),(3,3)))
pairs: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:30

scala> pairs.partitioner
res0: Option[org.apache.spark.Partitioner] = None

scala> val partitioned = pairs.partitionBy(new HashPartitioner(2))
partitioned: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[1] at partitionBy at <console>:31

scala> partitioned.partitioner
res1: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.HashPartitioner@2)
```

### Operations That Benefit from Partitioning

most Spark's operations that involve shuffling data by key across the network will benefit from partitioning

such as *cogroup(), groupWith(), join(), leftOuterJoin(), rightOuterJoin(), groupByKey(), reduceByKey(), combineByKey(), lookup()*

- for operations that act on a single RDD
	- cause all values for each key to be computed *locally* on a single machine, requiring only the final, locally reduced value to be sent from each worker node back to the master
- for binary operations
	- cause at least onr of the RDDs (the one with known partitioner) to not be shuffled
	- if both RDDs have the same partitioner, no shuffling across the network will occur

### Operations That Affect Partitioning

Spark knows internally how each of its operations affects partitioning, and automatically sets the **partitioner** on RDDs created by operations that partition the data

For transformations that cannot be guaranteed to produce a known partitioning, the output RDD will not have a partitioner set.
- Spark doesn't analyze functions to check whether they retain the key.
- Instead, it provides **mapValues()** and **flatMapValues()** to guarantee that each tuple's key remains the same

> Operations that result in a partitioner

*cogroup(), groupWith(), join(), leftOuterJoin(), rightOuterJoin(), groupByKey(), reduceByKey(), combineByKey(), partitionBy(), sort(), mapValues(), flatMapValues(), filter()*

- for binary opeartions, which partition is set on the output depends on the parent RDD's partitioner.
	- default -> a hash partitioner, with the number of partitions set to the level of parallelism of the operation
	- if one of the parents has a partitioner set -> it will be that partitioner
	- if both parents have a partitioner set -> it will be the 1st one

### Example : PageRank

- PageRank algorithm
	- aims to assign a measure of importance (a "rank") to each document in a set based on how many documents have links to it
	- maintains 2 datasets : 
		- (pageID, linkList) containing the list of neighbors of each page
		- (pageID, rank) containing the current rank of each page
	- process :
		- Initialize each page's rank to 1.0
		- on each iteration, have page **p** send a contribution of **rank(p)/numNeighbors(p)** to its neighbors
		- set each page's rank to ** 0.15 + 0.85 * contributionReceived**
		- in practice, it's typical to run about 10 iterations

> implement PageRank in Scala
```
// Assume that our neighbor list was saved as a Spark objectFile (pageID,linkList)
val links = sc.objectFile[(String, Seq[String])]("links")
		.partitionBy(new HashPartitioner(100))
		.persist()

// Initialize rank to 1.0 (pageID, 1.0)
var ranks = links.mapValues(v => 1.0)

// Run 10 iterations of PageRank
for (i <- 0 until 10){
	val contributions = links.join(ranks).flatMap{
		case (pageId, (links, rank)) =>
			links.map(dest => (dest, rank / links.size))
	}
	ranks = contributions.reduceByKey((x,y) => x + y).mapValues(v => 0.15 + 0.85 * v)
}

// Write out the final ranks
ranks.saveAsTextFile("ranks")
```

the example does several things to ensure that the RDDs are partitioned in an efficient way, and to minimize communication :

1. notice that the links RDD is joined against ranks on each iterations. Since links is a static dataset, we partition it at the start with partitionBy(), so that it is needless to be shuffled across the network.

2. call persist() on links to keep it in RAM across iterations

3. when we first create ranks, we use mapValues() instead of map() to preserve the partitioning of parent RDD(links), so that our first join() against it is cheap

4. follow our reduceByKey() with mapValues() in the loop body so that it will be more efficient


### Custom Partitioners

- Spark's **HashPartitioner** and **RangePartitioner** are well suited to many yse cases
- Spark also allows you to tune how an RDD is partitioned by providing a custom **Partitioner** object

<br>

e.g.

in PageRank algorithm, using a simple hash function to do the partitioning, pages with the similar URLs might be hashed to completely different nodes.

we can group pages within the same domain with a custom Partitioner that looks at just the domain name instead of the whole URL.

1. Scala

To implement a custom partitioner, we need to subclass the **org.apache.spark.Partitioner** class and implement 3 methods:
- **numPartitions** : *Int*, returns the number of partitions we will create
- **getPartition(key : Any)** : *int*, returns thr partition ID [0,numPartitions) for a given key
- **equals()** : the standard Java equality method, it's important to implement because Spark will need to test Partition object against other instances of itself when it decides whether 2 of RDDs are partitioned in the same way.

we have to ensure that getPartition() allways returns a nonnegative result.

> scala custom partitioner
```
class DomainNamePartitioner(numParts : Int) extends Partitioner{
	override def numPartitions : Int = numParts
	override def getPartition(key : Any) : Int = {
		val domain = new Java.net.URL(key.toString).getHost()
		
		// hashCode may return a negative number
		val code = (domain.hashCode % numPartitions)
		
		if(code < 0){
			code + numPartitions
		}else{
			code
		}
	}

	override def equals(other : Any) : Boolean = other match{	// use scala pattern matching operation
		case dnp : DomainNamePartitioner =>
			dnp.numPartitions == numPartitions
		case _ =>
			false
	}
}
```

Using a custom Partitioner : just pass it to the partitionBy() method or many other shuffle-based methods in Spark


2. Python

we don't need to extend a Partitioner class, but instead pass a hash function as an additional argument to RDD.partitionBy()

```
import urlparse

def hash_domain(url) : 
	return hash(urlparse.urlparse(url).netloc)

rdd.partitionBy(20,hash_domain)
```

note that the hash function we pass will be compared *by identity* to that of other RDDs.






































