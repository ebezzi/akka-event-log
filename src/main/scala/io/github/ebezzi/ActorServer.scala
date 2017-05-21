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

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b @ Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      log.info("Received connection from: {}", remote)
      val handler = context.actorOf(Props[Handler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class Handler extends Actor with ActorLogging {
  import Tcp._
  import Protocol._

  val cluster = Cluster(context.system)

  val file = new File("00000.dat")
  val reader = new LogReader(file)
  val writer = new LogWriter(file)

//  val offsetStorage = new File("offsets.dat")
//  val offsetManagerReader = new LogReader(offsetStorage)
//  val offsetManagerWriter = new LogWriter(offsetStorage)

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
          log.warning("Polled record: {}", new String(toSend, "utf-8"))
          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))

        case Some(PublishData(data)) =>
          log.info("Publishing: {}", data.length)
          writer.append(data)
          sender ! Write(ProtocolFraming.encode(ServerProtocol.writeAck))
      }

    case PeerClosed =>
      context stop self

  }

}


object ActorServer extends App {

  val system = ActorSystem("test-system")
//  system.actorOf(Props(new ActorServer))
  system.actorOf(Props(new SimpleClusterListener))

}


object ActorTest extends App {
  val system = ActorSystem("test-system")
}