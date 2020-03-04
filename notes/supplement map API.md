# Supplement map API

## map(function)

是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。

```python
# python
>>> rdd = sc.parallelize(['b','a','c'])
>>> sorted(rdd.map(lambda x : (x,1)).collect())
[('a', 1), ('b', 1), ('c', 1)]
```

## mapPartitions(function)

mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。

```python
>>> rdd = sc.parallelize([1,2,3,4], 2)
>>> def f(iterator) : yield sum(iterator)
>>> rdd.mapPartitions(f).collect()
[3,7]
```

## mapValues(function)

原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。因此，该函数只适用于元素为KV对的RDD

```python
>>> x = sc.parallelize([('a', ['apple', 'banana', 'lemon']), ('b', ['grapes'])])
>>> x.mapValues(lambda x : len(x)).collect()[('a', 3), ('b', 1)]
```

## flatMap(function)

与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素

```python
>>> rdd = sc.parallelize([2,3,4])
>>> sorted(rdd.flapMap(lambda x : range(1,x)).collect())
[1,1,1,2,2,3]
>>> sorted(rdd.flatMap(lambda x : [(x,x), (x,x)]).collect())
[(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
```

## flatMapValues(function)

flatMapValues类似于mapValues，不同的在于flatMapValues应用于元素为KV对的RDD中Value。每个一元素的Value被输入函数映射为一系列的值，然后这些值再与原RDD中的Key组成一系列新的KV对。

```python
>>> x = sc.parallelize([('a', ['x', 'y', 'z']), ('b', ['p', 'r'])])
>>> x.flatMapValues(lambda x : x).collect()
[('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]
```






