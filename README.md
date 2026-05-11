# CSCI 461: Introduction to Big Data - Assignment #2

## Team Members
- Karim Bassem Joseph (231000797)
- Ali Essam Ali (231001040)
- Omar Emad Sholkamy (231000802)

---

# Part One: MongoDB Setup

## Start MongoDB Container

```bash
docker run --name mongo -p 27017:27017 -d mongo:7.0
```

Check running containers:

```bash
docker ps
```

---

## Import Dataset

```bash
docker cp mds.json mongo:/mds.json
```

```bash
docker exec mongo mongoimport --db moviesDB --collection moviesColl --file /mds.json --jsonArray
```

---

## Run Queries

Open MongoDB shell:

```bash
docker exec -it mongo mongosh
```

Switch database:

```javascript
use moviesDB
```

Run the queries from:

```text
mDBQ.txt
```

Expected outputs are included in:

```text
mDBR.txt
```

---

# Part Two: Hadoop MapReduce

## Start the Cluster

```bash
docker-compose up -d
```

---

## Copy Files into Namenode

```bash
docker cp mds.csv namenode:/tmp/
docker cp ARDriver.java namenode:/tmp/
```

---

## Open Namenode Container

```bash
docker exec -it namenode /bin/bash
```

---

# HDFS Setup

## Create Input Directory

```bash
hdfs dfs -mkdir -p /input
```

## Upload Dataset

```bash
hdfs dfs -put /tmp/mds.csv /input/
```

---

# Compile and Run

## Set Hadoop Classpath

```bash
export HADOOP_CLASSPATH=$(hadoop classpath)
```

## Compile Java File

```bash
javac -classpath ${HADOOP_CLASSPATH} -d /tmp/ ARDriver.java
```

## Create JAR File

```bash
jar -cvf /tmp/ardriver.jar -C /tmp/ .
```

## Run MapReduce Job

```bash
hadoop jar /tmp/ardriver.jar ARDriver /input/mds.csv /output
```

---

# Export Results

## Merge Output Files

```bash
hdfs dfs -getmerge /output /tmp/final_results.txt
```

## Exit Container

```bash
exit
```

## Copy Results to Local Machine

```bash
docker cp namenode:/tmp/final_results.txt .
```

---

# Final Output

```text
final_results.txt
```