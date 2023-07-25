# Outlier Query Processing in Time Series Database

- The code of all the experiments can be found in the folder ***exp-code***. 

- All the opensource data can be found in the folder ***opensource-data***. 

- A well complied database IoTDB is given in the folder ***apache-iotdb-0.13.3-all-bin***, which is built from the [source code of LSMOD](https://github.com/apache/iotdb/tree/research/outlier).
- The main code of our proposal LSMOD can be found in [DoddsExecutor](https://github.com/apache/iotdb/blob/research/outlier/server/src/main/java/org/apache/iotdb/db/query/executor/DoddsExecutor.java).

## How to execute

1. Compile the time series database [IoTDB](https://github.com/apache/iotdb/tree/research/outlier) by the following command. There is also a well compiled database in the folder ***opensource-data***.

```
mvn clean package -DskipTests -Dcheckstyle.skip=True
```

2. Start the server and the client. Detailed steps can be found in the [online documentation](https://iotdb.apache.org/UserGuide/V0.13.x/QuickStart/QuickStart.html#use-cli).

3. Load the dataset TAO-OceanGraphic by executing the following commands in the client.

```sql
LOAD '../../opensource-data/root.tao/1/'
```

4. Process distance-based outlier query.

```sql
SELECT DODDS(s9, 'r'='15', 'k'='50', 'w'='1200000', 's'='600000') from root.tao.d0
```

Note: For UCI-Gas and UCI-PAMAP2, the bucket_width could be easily set by 'g' = '0.1' in the query for convenience. You can also set the bucket width in the configuration of the database.

