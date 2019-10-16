./bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic transaction-topic --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic category-topic --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server broker:29092 --create --topic enhanced-transaction-topic --partitions 3 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server broker:29092 --create \
            --topic customer-total-topic \
            --partitions 3 \
            --replication-factor 1 \
            --config cleanup.policy=compact \
            --config min.cleanable.dirty.ratio=0.01 \
            --config segment.ms=100
./bin/kafka-topics.sh --bootstrap-server broker:29092 --create \
            --topic customer-rolling-total-topic \
            --partitions 3 \
            --replication-factor 1 \
            --config cleanup.policy=compact \
            --config min.cleanable.dirty.ratio=0.01 \
            --config segment.ms=100
./bin/kafka-topics.sh --bootstrap-server broker:29092 --list

# View the raw transactions
 ./bin/kafka-console-consumer.sh --bootstrap-server broker:29092 \
             --topic transaction-topic \
             --from-beginning \
             --formatter kafka.tools.DefaultMessageFormatter \
             --property print.key=true \
             --property print.value=true \

# Consume total topic in console
 ./bin/kafka-console-consumer.sh --bootstrap-server broker:29092 \
             --topic customer-total-topic \
             --from-beginning \
             --formatter kafka.tools.DefaultMessageFormatter \
             --property print.key=true \
             --property print.value=true \
             --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
             --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# Consume rolling total topic in console
./bin/kafka-console-consumer.sh --bootstrap-server broker:29092 \
             --topic customer-rolling-total-topic \
             --from-beginning \
             --formatter kafka.tools.DefaultMessageFormatter \
             --property print.key=true \
             --property print.value=true \
             --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
             --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
