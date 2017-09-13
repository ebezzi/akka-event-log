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

  import context.system

  private val port =
    context.system.settings.config.getInt("server.tcp.port")

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  // A single instance for each node, please
  val manager = new TopicManager(context.system)

  // Start a partition leader for each topic(partition) found on this node
  for (topic <- manager.topics) {
    context.actorOf(Props(new PartitionLeader(topic)), topic)
  }

  // Listens on incoming connections. Might want to delay this unless the node is ready (e.g. has topics data, etc.)
  // by using a state machine
  def receive = {

    case b@Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case Connected(remote, local) =>
      log.info("Received connection from: {}", remote)
      val coordinator = context.actorOf(Props(new Coordinator(manager)))
      val connection = sender()
      connection ! Register(coordinator)
  }

}

// Application entry point
object ActorServer extends App {

  val system = ActorSystem("test-system")
  system.actorOf(Props(new ActorServer), "server")

}