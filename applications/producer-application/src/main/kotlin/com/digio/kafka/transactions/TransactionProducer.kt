package com.digio.kafka.transactions

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ser.std.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Timer
import java.util.TimerTask
import java.util.stream.IntStream

class TransactionProducer {
  companion object {
    private val logger: Logger = LoggerFactory.getLogger(TransactionProducer::class.java.name)
  }

  fun main(args: Array<String>) {
    logger.info("I'm a producer!")
  }
}
