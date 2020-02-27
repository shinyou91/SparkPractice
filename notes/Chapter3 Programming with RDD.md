# Chapter 3 Programming with RDDs

in Spark all work is expressed as either creating new RDDs, transforming existing RDDs, or calling operations on RDDs to compute a result.

## RDD basics

- each RDD is split into multiple *partitions*, which may be computed on different nodes of the cluster

### Create RDDs

- users create RDDs in 2 ways:
	- by loading an external dataset
	- by distributing a collection of objects in their driver program
```
>>> lines = sc.textFile("README.md")
```
### once created, RDDs offer 2 operations

- *Transformations*:construct a new RDD from a previous one(such as filtering)
```
>>> pythonLines = lines.filter(lambda line : "Python" in line)
```

- *Actions*:computes a result based on an RDD and return either to
	- the driver program
	- or an external storage system
```
>>> pythonLines.first()
```

- Spark computes RDDs only the first time they are used in an action
	- if Spark were to load and store all the lines as soon as we write them, it would waste a lot of storage space, given that we then immediately filter out many lines
	- Instead, once Spark sees the whole chain of transformation, it can compute just the data needed for its result
- After computing it the first time, Spark will store the RDD contents in memory, and reuse them in future actions.
- Spark's RDDs are by default recomputed each time you run an action on them

- if you'd like to reuse an RDD in multiple actions, you can ask Spark to persist it using
```
RDD.persist()
```
- **it doesn't persist by default**
	- if you will not reuse the RDD, there's no reason to waste storage space when Spark could instead stream through the
data once and just compute the result

- in practice, *persist()* is used to load a subset of data into memory and query it repeatedly.
```
>>> pythonLines.persist
>>> pythonLines.count()
>>> pythonLines.first()
```

### SUMMARY

every Spark program and shell session will work as follows:
- Create some input RDDs from external data
- Transform them to define new RDDs using transformations like filter()
- Ask Spark to persist() any intermediate RDDs that will need to be reused
- Launch actions to kick off a parallel computation, which is them optimized and execured by Spark


## Creating RDDs
load an external dataset or parallelize a collection in driver system

### the simplest way to create RDDs
- take an existing collection in your program and pass it to SparkContext's *parallelize()* method
- quick, useful
- outside of prototyping and testing, it is not widely used 
	- since it requires that you have the entire dataset in memory on one machine

> python
```
lines = sc.parallelize(["pandas","i like pandas"])
```
> scala
```
val lines = sc.parallelize(List("pandas","i like pandas"))
```

### a more common way to create RDDs

- load data from external storage
- details->Chapter5

> python
```
lines = sc.textFile("README.md")
```
> scala
```
val lines = sc.textFile("README.md")
```

## RDD Operations

### Transformation

- returns a new RDD
	- such as *map()* and *filter()*
- many transformations are element-wise
	- they work on onr element at a time

- example:**filter()**

> python
```
inputRDD = sc.textfile("log.txt")
errorsRDD = inputRDD.filter(lambda x : "error" in x)
```

>scala
```
val inputRDD = sc.textFile("log.txt")
val errorsRDD = inputRDD.filter(line => line.contains("error"))
```
	- note that filter() doesnot mutate exisiting inputRDD

- example:**union()**
>python
```
errorsRDD = inputRDD.filter(lambda x : "error" in x)
warningsRDD = inputRDD.filter(lambda x : "warning" in x)
badLinesRDD = errorsRDD.union(warningsRDD)
```
or we can just simply
```
badLinesRDD = inputRdd.filter(lambda x : "error" in x or "warning" in x)
```

- transformations can actually operate on any number of input RDDs
- **lineage graph**
	- the set of dependencies between different RDDs
	- usage
		- to compute each RDD on demand 
		- to recover lost data if pert of a persistenr RDD is lost

### Action

- returns a result to the driver program or write it to storage,and kick off a computation
	- such as *count()* and *first()*

- example : 
	- *count()* : returns the count as a number
	- *take()* : collects a number of elements from the RDD

> python
```
print "input has " + badLinesRDD.count() + "lines"
print "Here are 10 examples:"
for line in badLinesRDD.take(10):
	print line
```
> scala
```
println("input has " + badLinesRDD.count() + "lines")
println("Here are 10 examples:")
badLinesRDD.take(10).foreach(println)
```

- example : **collect()**->retrieve the entire RDD
	- your entire dataset must fit in memory on a single machine to use collect() on it
	- in most cases, RDDs can't just be collect()ed to the driver because they are too large
		- it's common to write data out to a distributed storage system
			- such as HDFS or Amazon S3
			- can save contents using *SaveAsTextFile()* and *SaveAsSequenceFile()* -> Chapter5

- **inefficiency**
	- each time we call a new action, the entire RDD must be computed "from each scratch"
	- users can **persist** intermediate results -> page 44

## Lazy Evaluation

- after transformations, Spark internally records metadata to indicate that this operation has been requested
- computed when running an action
- loading data into an RDDD is lazily evaluated in the same way transformations are

<br>

- Spark uses lazy evaluation to reduce the number of passes it has to take over our data by grouping operations together
	- users are free to organize their program into smaller, more managable operations

## Passing Functions to Spark

### Python

1. **lambda expressions** : for shorter functions
```
word = rdd.filter(lambda s : "error" in s)
```

2. **top-level or locally defined functions**
```
def containsError(s):
	return "error" in s
word = rdd.filter(containsError)
```
- **issue** : inadvertently serializing the object containing the function
	- when
		- pass a function that is the member of an object
		- contains references to fields in an object(such as self.field)
	- Spark sends the entire object to worker nodes, which can be much larger than the bit of information you need
	- sometimes cause programs to fail

> bad example
```
class SearchFunctions(object):
	def __init__(delf,query):
		self.query = query
	def isMatch(self,s):
		return self.query in s
	def getMatchesFunctionReference(self,rdd):
		#problem: references all of "self" in "self.isMatch"
		return rdd.filter(self.isMatch)
	def getMatchesMemberReference(self,rdd):
		#problem: references all of "self" in "self.query"
		return rdd.filter(lambda x : self.query in x)
```

- **Instead, just extract the fields you need from your object into a local variable and pass that in**

> good example
```
class SearchFunctions(object):
	...
	def getMatchesNoReference(self,rdd):
		#safe: extract only the field we need into a local wariable
		query = self.query
		return rdd.filter(lambda x : query in x)
```

### Scala

in Scala, we can pass in

- functions defined inline
- references to methods
- static functions

as we do for scala's other functional APIs

<br>

as we did in Python, we can extract the fields we need as local variables and avoid needing to pass the whole object containing them
```
class SearchFunctions(val query : String){
	def isMatch(s : String) : Boolean = {
		s.contains(query)
	}
	def getMatchesFunctionReference(rdd : RDD[String]) : RDD[String] = {
		#problem: "isMatch" means "this.isMatch", so we pass all of "this"
		rdd.map(isMatch)
	}
	def getMatchesFieldReference(rdd : RDD[String]) : RDD[String] = {
		#problem: "query" means "this.query", so we pass all of "this"
		rdd.map(x => x.split(query))
	}
	def getMatchesNoReference(rdd : RDD[String]) : RDD[String] = {
		#Safe : extract just the field we need into a local variable
		val query_ = this.query
		rdd.map(x => x.split(query_))
	}
}
```

- passing in local serializable variables or functions that are members of a top-level object is always safe
	- otherwise, *NotSeralizableException*

## Common Transformations and Actions

### Basic RDDs
transformations and actions we can perform on all RDDs regardless of the data

#### Element-wise transformations

two most common transformations you will likely be using:

- **map()**
	- takes in a function
	- apply it to each element in the RDD with the result of the function being the new value of each element in the resulting RDD
	- return type does not have to be the same as its input type

> python
```
nums = sc.parallelize([1,2,3,4])
squared = nums.map(lambda x : x * x).collect()
for num in squared:
	print "%i" % (num)
```
> scala
```
val input = sc.parallelize(List(1,2,3,4))
val result = input.map(x => x * x)
println(result.collect().mkString(","))
```

- **flatmap()**
	- produce multiple output elements for each input element
	- return an iterator with our return values
		- rather than producing an RDD of iterators, we get back an RDD that consists of the elements from all of the iterators
	- simple usage : *splitting up an input string into words*

> python
```
>>> lines = sc.parallelize(['hello world','hi'])
>>> words = lines.flatMap(lambda line : line.split(' '))
>>> words.first()
'hello'

```
> scala
```
val lines = sc.parallelize(List('hello world','hi'))
val words = lines.flatMap(lambda line => line.split(' '))
words.first()	// returns "hello"
```

- *the difference between map() and flatMap()*
	- instead of ending up with an RDD of lists, we have an RDD of the elements in those lists


- **filter()**
	- take in a function
	- return an RDD that only has elements that pass the filter() function

#### Pseudo set operations
RDDs support many of the operations of mathematical sets, even when the RDDs themselves are not properly sets

all of those opeartions require that the RDDs being operated on are of thensame type

- RDD.distinct()
	- the set property most frequently missing from RDDs are uniqueness of elements
	- however, it requires shuffling all the data over the network to ensure uniqueness

- RDD1.union(RDD2)
	- simplest
	- if input contains duplicates, output will also contain duplicates

- RDD1.intersection(RDD2)
	- removes all duplicates
	- worse performance than union
	- needs shuffling

- RDD1.substract(RDD2)

- RDD1.cartesian(RDD2) 笛卡尔积
	- useful when we wish to consider the similarities between all possible pairs
	- very expensive

#### Actions

- the most common action on basic RDDS : **reduce()**
	- take a function that operates on 2 elements of the type in your RDD anf returns a new element of the same type

> python
```
sum = rdd.reduce(lambda x,y : x + y)
```
> scala
```
 val sum = rdd.reduce((x,y) => x + y)
```

- **fold()**
	- similar to reduce()
	- in addition takes a *"zero value"* to be used for the initial call on each partition
		- zero value provoded should be the identity element for the operation
		- the zero value should not change

- when computing a running average

first use map() where we transform every element into the element and the number 1

single -> pair

```
rdd.map(lambda s : (s,1))
```

then reduce() can work on pairs
```
rdd.reduceByKey(lambda a,b : (a[0] + b[0] , a[1] + b[1]))
```

- **aggregate()**
	- free of the constraint that return must be the same type of the RDD
	- have an initial zero value
	- 2 functions
		- combine the elements from RDD with the accumulator
		- merge 2 accumulators, given that each node accumulates its own results locally

> python
```
sumCount = nums.aggregate((0,0),
		(lambda acc, value : (acc[0] + value, acc[1] + 1)),
		(lamdda acc1, acc2 : (acc1[0] + acc2[0], acc1[1] + acc2[1])))
print sumCount[0] / float(sumCount[1])
```

> scala
```
val result = input.aggregate((0,0))(
	(acc,value) => (acc._1 + value, acc._2 + 1),
	(acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
val avg = result._1 / result._2.toDouble
```

- ***note that these operations do not return the elements in the order you might expect***
- *these operations are useful for unit tests and quick debugging, but may introduce bottlenecks when you're dealin with large amounts of data*

##### Basic Actions

- collect()
	- returns all elements from the RDD
- count()
	- number of elements in the RDD
- countByValue()
	- returns a map of each unique value in the RDD to its count
- take(num)
	- return num elements from the RDD
- top(num)
	- return the top num elements of the RDD
- takeOrdered(num)(ordering)
	- return num elements based on provided ordering
- takeSample(withReplacement, num, [seed])
	- Return num elements at random
- reduce(func)
	- Combine the elements of the RDD together in parallel
- fold(zero)(func)
	- same as reduce() but with the provided zero value.
- aggregate(zeroValue)(seqOp, combOp)
	- Similar to reduce() but used to return a different type.
- foreach(func)
	- Apply the provided function to each element of the RDD.

### Converting between RDD types

- some functions are available only on certain types of RDDs

- in Scala and Java, these methods aren't defined on standard RDD class, so to access this additional functionality we have to make sure we get the correct specialized class

- in Python all of the functions are implemented on the base RDD class but fail at runtime if the type of data in the RDD is incorrect

## Persistence(Caching)

- sometimes we use the RDD multiple times
	- if we do this naively, Spark will recompute the RDD and all of its dependencies each time we call an action on the RDD
	- to avoid multiple computing, we can ask Spark to *persist* the data
		- if a node that has data persisted on  it fails, Spark will recompute the lost partition of the data when needed
		- we can also replicate our data on multiple nodes if we want to be able to handle node failure without slowdown

- Spark has many levels of persistence to choose from based on what our goals are

- data storage
	- Scala & Java
		- default persist() will store the data in the JVM heap as unserialized objects
	- Python
		- always serialize the persisted data, so default is stored in the JVM heap as pickled objects

- persistence levels


Level | Space Used | CPU Time | In Memory | On Disk | Comments
- | - | - | - | - | -
MEMORY_ONLY | high | low | Y | N |  
MEMORY_ONLY_SER | low | high | Y | N
MEMORY_AND_DISK | high | medium | some | some | split to disk if there is too much data to fit in the memory
MEMORY_AND_DISK_SER | low | high | some | some | split to disk if there is too much data to fit in the memory & stores serialized  representation in memory
DISK_ONLY | low | high | N | Y

> persist() in Scala
```
val result = input.map(x => x * x)
result.persist(StorageLevel.DISK_ONLY)
println(result.count())
println(result.collect().mkString(","))
```
- call persist() on the RDD before the first action
- the persist() on its own doesn't force evaluation

- if you attempt to cache too much data to fit in memory, Spark will automatically evict old partitions using a LRU cache policy
- RDDs also come with a ***unpersist()*** to manually remove them from cache
