# Querying Outliers in Time Series Database

## How to Load Data

Execute the following command in the client of IoTDB.

```sql
load './open_source_data/PAMAP2' autoregister=true, sglevel=1
```

## How to Modify Hyper-Parameters

Modify the time range of segments, the value range of buckets by modifying the bucket_width and window_size in the  configuration file *iotdb-engine.properties*. For instance, 'bucket_width = 0.1' denotes that the value range of buckets is 0.1, and 'window_size = 60000' denotes that the time range of segments is 60000 milliseconds.

```
	bucket_width = 0.1
	window_size = 60000
```

## How to Execute LSMOD

After loading data and modifying hyper-parameters, execute the following command in the client of IoTDB.

```sql
select dodds (s0, 'r'='0.4', 'k'='50', 'w'='20', 's'='10') from root.PAMAP2.d0
```

Note that 'w = 20' denotes that the query window size is 20 times the size of the time range of segments. Given the hyper-parameters above, 'w = 20' means that the query window size is 20*60000 milliseconds, i.e., 20 minutes. The query also supports time filter, by executing the following command in the client of IoTDB.

```sql
select dodds (s0, 'r'='0.4', 'k'='50', 'w'='20', 's'='10') from root.PAMAP2.d0
where time >= 1970-01-01T08:00:00
  and time <= 1970-01-01T17:23:59
```

