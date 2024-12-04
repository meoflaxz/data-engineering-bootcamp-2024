# SPARK FUNDAMENTAL

- spark is java based. that is the function is in camelcase - eg withColumn, not with_column
- df.collect() can crash spark kernel cause it spill to disk
- restart the kernel if crash
- use take(n) and show instead of collect() filter it down first
<br></br>
- take(n) vs collect()
    - take(n) - Returns first n elements to driver
    - collect() - Returns all elements to driver
<br></br>
- sort() vs sortWithinPartitions()
    - sort() - do a global sort - very expensive
    - sortWithinPartitions() - sort locally in the partition within the partition
    - global sort is very slow and painful at scale
    - using sort() will have double sort
<br></br>
- use explain() to understand the spark strategy
- exchange in explain() means shuffle
<br></br>
- when sorting data, starts with lowest cardinality first
- play out with sortWithinPartitions to get the optimize partition size for better compression
- Why would one use sortWithinPartitions instead of sort?
    - sortWithinPartitions() does not trigger a shuffle, as the data is only moved within the executors.
    - sort() however will trigger a shuffle. Therefore sortWithinPartitions() executes faster. 