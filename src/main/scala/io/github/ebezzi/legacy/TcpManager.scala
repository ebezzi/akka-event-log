package io.github.ebezzi.legacy

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.io.{IO, Tcp}

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
      val handler = context.actorOf(Props[Handler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class Handler extends Actor {
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