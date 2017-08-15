package io.github.ebezzi

import java.io.File
import java.net.InetSocketAddress
import java.nio.ByteOrder

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

  val file = "00000.dat"

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b @ Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      log.info("Received connection from: {}", remote)
      val handler = context.actorOf(Props(new Handler(file)))
      val connection = sender()
      connection ! Register(handler)
  }

}

class Handler(path: String) extends Actor with ActorLogging {
  import Tcp._
  import Protocol._

//  val cluster = Cluster(context.system)

  val file = new File(path)
  val reader = new LogReader(file)
  val writer = new LogWriter(file)

  val producer = new Producer

  // TODO: this should be persisted to a database
  var lastCommittedOffset = 0L

  def receive = {
    case Received(data) =>
      Protocol.decode(data) match {

        case Some(RegisterConsumer(consumerId)) =>
          log.info("{} registered", consumerId)
          // TODO: match the consumerId and get the right partition (for now, assume offset is 0)

        case Some(CommitOffset(consumerId, offset)) =>
          log.info("Received commit from consumerId {}", consumerId)
          lastCommittedOffset = 1 + offset
          sender ! Write(ProtocolFraming.encode(ServerProtocol.commitAck))

        case Some(Poll(consumerId)) =>
          log.info("Received poll from consumerId {}", consumerId)
          val toSend = reader.fromOffset(lastCommittedOffset)
          log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), lastCommittedOffset)
          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))

        case Some(PublishData(topic, data)) =>
          log.info("Publishing to topic {}: {}", topic, data.length)
          writer.append(data)
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
  system.actorOf(Props(new ActorServer))
//  system.actorOf(Props(new SimpleClusterListener))

}


object ActorTest extends App {
  val system = ActorSystem("test-system")
}