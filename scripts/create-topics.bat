@echo off

CALL kafka-topics.bat --create ^
  --bootstrap-server 10.10.51.1:9092,10.10.52.1:9092,10.10.53.1:9092,10.10.54.1:9092 ^
  --topic stress ^
  --partitions 3 ^
  --replication-factor 3 ^
  --config min.insync.replicas=2

echo Topic 'stress' created.
echo Starting producer performance test...
kafka-producer-perf-test.bat ^
  --topic stress ^
  --num-records 5000000 ^
  --record-size 100 ^
  --throughput 50000 ^
  --producer-props bootstrap.servers=10.10.51.1:9092,10.10.52.1:9092,10.10.53.1:9092,10.10.54.1:9092 acks=all linger.ms=5 batch.size=65536 compression.type=lz4