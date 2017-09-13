package io.github.ebezzi

import java.io.File
import java.nio.ByteBuffer

import akka.Done
import akka.actor.{Actor, ActorLogging}
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingReceive}
import akka.io.Tcp.{PeerClosed, Received, Write}
import akka.util.ByteString

case class LeaderResponse(data: ByteString)
case class LeaderPoll(offset: Long)
case object LeaderGetAllElements

class PartitionLeader(topic: String) extends Actor with ActorLogging {

  import ClientProtocol._
  import context.{system, dispatcher}

  //  val cluster = Cluster(context.system)

  private def fileFor(topic: String) =
    new File(s"./${system.settings.config.getString("server.data-dir")}/$topic.dat")

  private def writerFor(topic: String) =
    new LogWriter(fileFor(topic))

  private def readerFor(topic: String) =
    new LogReader(fileFor(topic))

  var reader: LogReader = readerFor(topic)
  var writer: LogWriter = writerFor(topic)

  // This is for writing offsets
  // TODO: we need a more complex structure
  var producer: Producer = new Producer

  log.info("Partition leader started")

  def receive = LoggingReceive(Logging.InfoLevel) {

    case LeaderPoll(offset) =>
      val toSend = reader.fromOffset(offset)
      log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), offset)
      sender ! LeaderResponse(ProtocolFraming.encode(ServerProtocol.record(offset, toSend)))

    case LeaderGetAllElements =>
      val last = reader.readAll().last
      // TODO: this could use a different object
      sender ! LeaderResponse(ProtocolFraming.encode(ServerProtocol.record(-1, last)))

    case PublishData(topic, data) =>
      log.info("Publishing to topic {}: {}", topic, data.length)
      writerFor(topic).append(data)
      sender ! LeaderResponse(ProtocolFraming.encode(ServerProtocol.writeAck))
  }

}