# inventorylevel



bin/hdfs dfs -mkdir /user/prezende/inventory
bin/hdfs dfs -mkdir /inventory
bin/hdfs dfs -mkdir /inventory/input


bin/hdfs dfs -copyFromLocal /Users/prezende/git/InventoryLevel/input/* /inventory/input

bin/hdfs dfs -ls /inventory/input



bin/hadoop jar /Users/prezende/git/InventoryLevel/target/InventoryLevel.jar