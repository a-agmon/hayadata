### build tables

```
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --conf spark.hadoop.hive.cli.print.header=true


create table local.myDB.customers
(
    customer_id int,
    tier string,
    name string,
    transaction bigint
     
) partitioned by (tier);

ALTER TABLE local.mydb.customers SET TBLPROPERTIES (
    'format-version'=2,
    'write.delete.mode'='copy-on-write',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read'
);


INSERT INTO local.mydb.customers
VALUES 
(1, "A", "customer1", 23450),
(2, "A", "customer2", 12345),
(3, "B", "customer3", 6578);


INSERT INTO local.mydb.customers
VALUES 
(10, "A", "customer10", 23450),
(20, "A", "customer20", 12345),
(30, "B", "customer30", 6578);

delete from local.mydb.customers where customer_id = 1;


select
    committed_at,
    element_at(summary, "added-records") as new_records,
    element_at(summary, "added-data-files") as new_datafiles,
    element_at(summary, "added-files-size") as new_size
from local.mydb.customers.snapshots;

select
    date_trunc('HOUR', committed_at),
    operation,
    sum(element_at(summary, "added-records")) as new_records,
    sum(element_at(summary, "added-data-files")) as new_datafiles,
    sum(element_at(summary, "added-files-size")) as new_size
from local.mydb.customers.snapshots
group by 1,2;
```

### some of the queries 

```
SELECT
    partition,
    sum(file_size_in_bytes) partition_size,
    count(*) files_num
FROM local.mydb.customers.files
GROUP BY partition


SELECT
    readable_metrics.transaction.upper_bound tx_upper,
    readable_metrics.transaction.lower_bound tx_lower,
    readable_metrics.customer_id.null_value_count null_ids
FROM local.mydb.customers.files;
```
