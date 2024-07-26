#!/bin/bash

set -x

mode=$1

user=luoxh
kafka_dir=/proj/rasl-PG0/$user/kafka
log_dir=/data
text=/proj/rasl-PG0/$user/samza/input/pg2554.txt

start_kafka() {
    $kafka_dir/bin/zookeeper-server-start.sh    $kafka_dir/config/zookeeper.properties  > $log_dir/zk.log 2>&1 &
    sleep 2
    $kafka_dir/bin/kafka-server-start.sh        $kafka_dir/config/server.properties      > $log_dir/kafka.log 2>&1 &
}

stop_kafka() {
    $kafka_dir/bin/kafka-server-stop.sh
    $kafka_dir/bin/zookeeper-server-stop.sh
}

create_topic() {

    $kafka_dir/bin/kafka-topics.sh --create --topic sample-text --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    sleep 2
    $kafka_dir/bin/kafka-console-producer.sh --topic sample-text --bootstrap-server localhost:9092 < $text
}

clean() {
    sudo rm -rf /data/zookeeper
    sudo rm -rf /data/kafka-logs
    sudo rm -f $log_dir/zk.log
    sudo rm -f $log_dir/kafka.log
}

read_topic() {
    $kafka_dir/bin/kafka-console-consumer.sh --topic word-count-output --bootstrap-server localhost:9092 --from-beginning
}

if [ $mode == "start" ]; then
    start_kafka
    sleep 5
    create_topic
elif [ $mode == "read" ]; then
    read_topic
else
    stop_kafka
    clean
fi
