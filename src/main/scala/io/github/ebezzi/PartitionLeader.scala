package io.github.ebezzi

import java.io.File
import java.nio.ByteBuffer

import akka.Done
import akka.actor.{Actor, ActorLogging}
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingReceive}
import akka.io.Tcp.{PeerClosed, Received, Write}

case class LeaderResponse(data: Any)

class PartitionLeader extends Actor with ActorLogging {

  import Protocol._
  import context.{system, dispatcher}

  //  val cluster = Cluster(context.system)

  private def fileFor(topic: String) =
    new File(s"./${system.settings.config.getString("server.data-dir")}/$topic.dat")

  private def writerFor(topic: String) =
    new LogWriter(fileFor(topic))

  private def readerFor(topic: String) =
    new LogReader(fileFor(topic))

  var reader: LogReader = _
  var writer: LogWriter = _

  // This is for writing offsets
  // TODO: we need a more complex structure
  var producer: Producer = _

  // Will be fetched when a consumer is registered
  // TODO: consumer group (to allow multiple, independent consumers)
  var lastCommittedOffset: Long = _

  log.info("Partition leader started")

  def receive = LoggingReceive(Logging.InfoLevel) {

    case RegisterConsumer(topic, consumerId) =>
      log.info("{} registered for topic {}", consumerId, topic)
      reader = readerFor(topic)
      writer = writerFor(topic)
      producer = new Producer
      if (topic != "_offset") {
        val consumer = new Consumer("_offset")
        // TODO: this is a hack
        val lv = ByteBuffer.wrap(consumer.lastValue().data).getLong
        log.info("Recovering from offset {}", lv)
        lastCommittedOffset = lv
      }

    case CommitOffset(consumerId, offset) =>
      log.info("Received commit from consumerId {}", consumerId)
      val requestor = sender
      producer.produce("_offset", 1 + offset).map { _ =>
        log.info("Offset committed...")
        lastCommittedOffset = 1 + offset
        requestor ! Write(ProtocolFraming.encode(ServerProtocol.commitAck))
      }

    case Poll(consumerId) =>
      log.info("Received poll from consumerId {}", consumerId)
      val toSend = reader.fromOffset(lastCommittedOffset)
      log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), lastCommittedOffset)
      sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))

    case GetAllElements =>
      val last = reader.readAll().last
      // TODO: this could use a different object
      sender ! Write(ProtocolFraming.encode(ServerProtocol.record(-1, last)))

    case PublishData(topic, data) =>
      log.info("Publishing to topic {}: {}", topic, data.length)
      writerFor(topic).append(data)
      sender ! LeaderResponse(Done)
  }

}