package io.github.ebezzi

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import java.net.InetSocketAddress

class Server2 extends Actor with ActorLogging {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 7777))

  def receive = {
    case b @ Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      val handler = context.actorOf(Props[SimplisticHandler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class SimplisticHandler extends Actor {
  import Tcp._
  def receive = {
    case Received(data) => sender() ! Write(data)
    case PeerClosed     => context stop self
  }
}

object TcpManager extends App {

  val system = ActorSystem("tcp-manager")
  system.actorOf(Props(new Server2))

}