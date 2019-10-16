package com.digio.kafka.transactions

import com.digio.kafka.transactions.serdes.MessageTimeExtractor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import com.digio.kafka.transactions.serdes.TransactionSerde
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.ValueJoiner
import java.util.concurrent.TimeUnit


class StreamingTopology {
  companion object {
    private val logger: Logger = LoggerFactory.getLogger(StreamingTopology::class.java.name)
    fun config(): Properties {
      return Properties().apply {
        this[StreamsConfig.APPLICATION_ID_CONFIG] = "bank-starter-app"
        this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        this[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = "0"
        this[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java.name
        this[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = TransactionSerde::class.java.name
      }
    }

    fun createStream(builder: StreamsBuilder): KStream<String, Transaction>? {
      return null
    }

    fun computeTotals(kStream: KStream<String, Transaction>?): KStream<String, Long>? {
      return null
    }

    fun computeRunningTotal(kStream: KStream<String, Transaction>?): KStream<Windowed<String>, Long>? {
      return null
    }

    fun createCategoryLookupTable(builder: StreamsBuilder): GlobalKTable<String, String>? {
      return null
    }

    fun categorisedStream(kStream: KStream<String, Transaction>, kTable: GlobalKTable<String, String>?)
        : KStream<String, Transaction>? {
      return null
    }

    fun topology(builder: StreamsBuilder) {
      val stringLongProduced = Produced.with(Serdes.StringSerde(), Serdes.LongSerde())
      val stream = createStream(builder)
      val totalStream = computeTotals(stream)
      val windowedLongKStream = computeRunningTotal(stream)
      val enhancedTransactions = categorisedStream(stream!!,
          createCategoryLookupTable(builder))
    }
  }

  fun main(args: Array<String>) {
    val builder = StreamsBuilder()
    topology(builder)

    // Start and start streaming
    val topology: Topology = builder.build()
    logger.info(topology.describe().toString())
    val streams = KafkaStreams(topology, config())
    streams.cleanUp() // only do this in dev - not in prod
    streams.start()
    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
  }
}
