# SPARK FUNDAMENTAL

- spark is java based. that is the function is in camelcase - eg withColumn, not with_column
- df.collect() can crash spark kernel cause it spill to disk
- restart the kernel if crash
- use take(n) and show instead of collect() filter it down first


- take(n) vs collect()
    - take(n) - Returns first n elements to driver
    - collect() - Returns all elements to driver

