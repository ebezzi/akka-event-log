package io.github.ebezzi

import java.io.File
import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.io.{IO, Tcp}
import akka.io.Tcp._
import akka.util.{ByteString, ByteStringBuilder}

class ActorServer extends Actor with ActorLogging {

  import Tcp._
  import context.system

  private val port =
    context.system.settings.config.getInt("server.tcp.port")

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b@Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case c@Connected(remote, local) =>
      log.info("Received connection from: {}", remote)
      val handler = context.actorOf(Props(new Handler))
      val connection = sender()
      connection ! Register(handler)
  }

}

class Handler extends Actor with ActorLogging {

  import Tcp._
  import Protocol._
  import context.{system, dispatcher}

  //  val cluster = Cluster(context.system)

  private def fileFor(topic: String) =
    new File(s"$topic.dat")

  private def writerFor(topic: String) =
    new LogWriter(fileFor(topic))

  private def readerFor(topic: String) =
    new LogReader(fileFor(topic))

  var reader: LogReader = _
  var writer: LogWriter = _

  // This is for writing offsets
  // TODO: we need a more complex structure
  var producer: Producer = _

  // TODO: this should be persisted to a database
  var lastCommittedOffset = 0L

  def receive = {
    case r@Received(data) =>
      Protocol.decode(data) match {

        case Some(RegisterConsumer(topic, consumerId)) =>
          println("### " + r.data)
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


        case Some(CommitOffset(consumerId, offset)) =>
          log.info("Received commit from consumerId {}", consumerId)
          val requestor = sender
          producer.produce("_offset", 1 + offset).map { _ =>
            log.info("Offset committed...")
            lastCommittedOffset = 1 + offset
            requestor ! Write(ProtocolFraming.encode(ServerProtocol.commitAck))
          }

        case Some(Poll(consumerId)) =>
          log.info("Received poll from consumerId {}", consumerId)
          val toSend = reader.fromOffset(lastCommittedOffset)
          log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), lastCommittedOffset)
          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))

        case Some(GetAllElements) =>
          val last = reader.readAll().last
          // TODO: this could use a different object
          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(-1, last)))

        case Some(PublishData(topic, data)) =>
          log.info("Publishing to topic {}: {}", topic, data.length)
          writerFor(topic).append(data)
          sender ! Write(ProtocolFraming.encode(ServerProtocol.writeAck))
      }

    case PeerClosed =>
      context stop self

  }

}

//class ReplicaHandler(path: String) extends Actor with ActorLogging {
//
//  val file = new File(path)
//  val reader = new LogReader(file)
//  val writer = new LogWriter(file)
//
//  override def receive: Receive = {
//
//  }
//
//}

object ActorServer extends App {

  val system = ActorSystem("test-system")
  system.actorOf(Props(new ActorServer), "server")
  //  system.actorOf(Props(new SimpleClusterListener))

}


object ActorTest extends App {
  val system = ActorSystem("test-system")
}