# Chapter 6 Advanced Spark Programming

## Introduction

1. two types of shared variables : 
	- **accumulators** to aggreegate information
	- **broadcast variables** to efficiently distribute large values

2. In addition to the languuages directly supported by Spark, the system can call into programs written in other languages.
	- using Spark's language-agnostic **pipe()** method to interact with other programs through standard input and output

3. Spark has methods for working with numeric data

## Accumulators

When we naomally pass functions to Spark, they can use variables defined outside them in the driver program
- each task running on the cluster gets a new copy of each variable
- updates from these copies are not propagated back to the driver

Spark's shared variables relax this restriction for 2 common types of communication patterns : aggregation of results and broadcasts

ACCUMULATORS provide a simple syntax for aggregating values from worker nodes back to the driver program
- one of the most common uses
	- count events that occur during job execution for debugging purposes

> accumulator empty line count in Python
```python
file = sc.textFile(inputFile)
blankLines = sc.accumulator(0)

def extractCallSigns(line) :
	global blankLines	# make global variable accessible
	if(line == '') : 
		blankLines += 1
	return line.split(' ')

callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile(outputDir + '/callsigns')
print('blank lines : %d' % blankLines.value)
```

> accumulator empty line count in Scala
```scala
val sc = new SparkContext(...)
val file = sc.textFile("file.txt")

val blankLines = sc.accumulator(0)

val callSigns = file.flatMap(line => {
	if (line == ""){
		blankLines += 1
	}
	line.split(" ")
})

callSigns.saveAsTextFile("output.txt")
println("Blank Lines : " + blankLines.value)
```

NOTE THAT we will see the right count only after we run the *saveAsTextFile()* action

- accululators work as follows :
	1. create them in the driver by calling the **SparkContext.accumulator(initial)** method
		- returns an `org.apache.spark.Accumulator[initialValue]` type
	2. worker code in Spark closure can add to the accumulator with += method
	3. the driver program can call the value property on the accumulator to access its value
		- tasks on worker nodes cannot access the accumulator's value()
			- from their point of view, accumulators are *write-only* variables


> Accumulator error count in Python
```python
# following previous codes

validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign) :
	global validSignCount, invalidSignCount
	if re.match(r'\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z', sign) :
		validSignCount += 1
		return True
	else:
		invalidSignCount += 1
		return False

validSigns = callSigns.filter(validateSigns)
contactCount = validSigns.map(lambda sign : (sign, 1)).reduceByKey(lambda (x, y) : x + y)

# force evaluation so the counters are populated
contactCount.count()

if invalidSignCount.value < 0.1 * validSignCount.value :
	contactCount.saveAsTextFile(outputDir + '/contactCount')
else : 
	print 'too many errors : %d in %d' % (invalidSignCount.value, validSignCount.value)
```

### Accumulators and Fault Tolerance

Spark automatically deals with failed or slow machines by re-executing failed or slow tasks.
- even if no nodes fail, Spark may have to rerun a task to rebuild a cache value that falls out of memory
- the net result is that the same function may run multiple times on the same data depending on what happens on the cluster

1. for accumulators used in actions
	- spark applies each task's update to each accumulator only once
	- if we want a reliable absolute value counter, regardless of failures or multiple evaluations, we must put it inside an action like *foreach()*

2. for accumulators used in RDD transformations, this guarantee does not exist
	- an accumulator update within a transformation can occur more than once
	- within transformations, accumulators should be used only for debugging purpose

### Custom Accumulators

Custom accumulators need to extend **AccumulatorParam**
- beyond adding to a numeri c value, we can use any operation for add, provided that operation is commutive and associative
	- **commutative** : `a op b = b op a`
	- **associative** : `(a op b) op c = a op (b op c)`



## Broadcase Variables

- broadcast variables allow the program to efficiently send a large, read-only value to all the worker nodes for use in one or more spark operations

- recall that spark automatically sends all variables referenced in your closures to the worker nodes
	- convenient yet inefficient because
		1. the default task launching mechanism is optimized for small task sizes
		2. you might use the same variables in *multiple* parallel operations but Spark will send it separately for each operation

> country lookup by prefix(前缀) matching in an array in Python
```python
signPrefixes = loadCallSignTable()

def processSignCount(sign_count, signPrefixes) :
	country = lookupCountry(sign_count[0], signPrefixes)
	count = sign_count[1]
	return (country, count)

countryContactCounts = (contactCounts
			.map(processSignCount)
			.reduceByKey(lambda x, y : x + y))
```

problem :
- if we have a large table, the signPrefixes could easily be several megabytes -> expensive
- if we used the same signPrefixes object later, it would be sent again to each node

**we can fix this by making *signPrefixes* a broadcast variable**
- type : `spark.broadcast.Broadcast[value type]`

> country lookup with Broadcast values in Python
```python
signPrefixes = sc.broadcast(loadCallSignTable())

def processSignCount(sign_count, signPrefixes) :
	country = lookupCountry(sign_count[0], signPrefixes)
	count = sign_count[1]
	return (country, count)

countryContactCounts = (contactCounts
			.map(processSignCount)
			.reduceByKey(lambda x, y : x + y))

countryContactCounts.saveAsTextFile(outputDir + '/countries.txt')
```

- the process of using broadcast variables :
	1.  create a **Broadcast[T]** by calling `sparkContext.broadcast` on an object of any type T which is *Serializable*
	2. access its value with the *value* property
	3. the variable will be sent to each node only once, and should be treated as read-only
		- updates will not be propagated to other nodes
		- the easiest way to satisfy read-only requirement
			- broadcast a primitive value or a reference to an immutable value
		- however, sometimes it can be more convenient or more efficient to broadcast a mutable object
		

### Optimizing Broadcast

- when broadcasting large values, it is important to choose a data serialization format that is both fast and compact
	- because the time to send the value over the network can quickly become a bottleneck if
		- it takes a long time to either serialize a value 
		- or to send the serialized value over the network

- in particular, Java Serialization (the default serialization library used in Spark's Scala and Java APIs) can be very inefficient out of the box for anything except arrays of primitive types.
	- optimization
		1. select a different serialization library using the `spark.serializer` property
		2. implement your own serialization routines for your data types



## Working on a Per-Partition Basis

1. working with data on a per-partition basis allows us to avoid redoing setup work for each data item.
	- Spark has *per-partition* versions of **map** and **foreach** to help reduce the cost of these operations by letting you run code only once for each partition of an RDD
	- in the ham radio call signs example
		- by using partition-based opeartions
			- we can share a connection pool to this database to avoid setting up many connections, and reuse our JSON parser

> shared commection pool in Python
```python
def processCallSigns(signs) :
	 # create a connection pool
	http = urllib3.PoolManager()
	
	# the url associated with eacg call sign record
	urls = map(lambda x : 'http://73s.com/qsos/%s.json' % x , signs)

	# create the requests
	request = map(lambda x : (x, http.request('GET', x)), urls)
	
	# fetch the results
	result = map(lambda x : (x[0], json.loads(x[1].data)), requests)

	# remove any empty results and return
	return filter(lambda x : x[1] is not None, result)

def fetchCallSigns(input) :
	return input.mapPartitions(lambda callSigns : processCallSigns(callSigns))

contactsContactList = fetchCallSigns(validSigns)
```

When operating on a per-partition basis, Spark gives our function an **Iterator** of the elements in that partition.

> per-patition operators

function name | we are called with | we return | function signature on RDD[T]
 :-: | :-: | :-: | :-: 
mapPartitions() | iterator of the elements in that partition | iterator of our return elements |  f : (Iterator[T]) -> iterator[U]
mapPartitionsWithIndex() |  integer of partition number, and iterator of the elements in that partition | iterator of our return elements | f : (Int, Iterator[T]) -> Iterator[U]
foreachPartition() | Iterator of the elements | Nothing | f : (Iterator[T])-> Unit


2. In addition to avoiding setup work, we can sometimes use **mapPartitions()** to avoid object creation overhead.

- sometimes we need to make an object for aggregating the result that is of a different type

> average without mapPartitions() in Python
```python
def combineCtrs(c1, c2) : 
	return (c1[0] + c2[0], c1[1] + c2[1])

def basicAvg(nums) :
	nums.map(lambda num : (num, 1)).reduce(combineCtrs)
```

> average with mapPartitions() in python
```python
def partitionCtr(nums) :
	sumCount = [0, 0]
	for num in nums :
		sumCount[0] += num
		sumCount[1] += 1
	return [sumCount]

def fastAvg(nums) :
	sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
	return sumCount[0] / sumCount[1]
```


## Piping to External Programs(omitted here)


## Numeric RDD Opeartions

- Spark's numeric operations are implemented with a streaming algorithm that allows for building up our model one element at a time.
- the desriptive statistics are all computed in a single pass over the data and returned as a **StatsCounter** object by calling **stat()**

> summary statistics available from StatsCounter

method | meaning
 :-: | :-:
count() | number of elements in the RDD
mean() | average of the elements
sum() | total
max() | maximum value
min() | minimum value
variance() | variance of elements 方差
sampleVariance() | variance of the elements, computed for a sample
stdev() | standard deviation 标准差
sampleStdev() | sample standard deviation

> use summary statistics to remove some outliers from our data in python
```python
# convert the RDD of string to numeric data so we can compute stats and remove the outliers
distanceNumerics = distances.map(lambda string : float(string))

stats = distanceNumerics.stats()
stddev = std.stdev()
mean = stats.mean()

reasonableDistances = distanceNumerics.filter(
	lambda x : math.fabs(x - mean) < 3 * stddev)
print reasonableDistances.collect()
```


















