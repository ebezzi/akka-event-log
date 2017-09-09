package io.github.ebezzi

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.io.{IO, Tcp}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.{ByteString, ByteStringBuilder}

// TODO: producer do not need a topic, so this can be omitted. Maybe can be done better
class ActorClientProducer extends Actor with ActorLogging with Stash {

  val remote = new InetSocketAddress("localhost", 7001)

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      context stop self

    case Connected(remote, local) =>
      val connection = sender()
      connection ! Register(self)
      context become connected(connection)
      unstashAll()

    case other =>
      log.info("Stashing {}", other)
      stash()
  }

  def connected(connection: ActorRef): Receive = {

    case Publish(topic, data) =>
      log.info("Publishing {} bytes to topic {}", data.length, topic)
      connection ! Write(ProtocolFraming.encode(Protocol.publishData(topic, data)))
      context become waitingForResponse(connection, sender)

    case CommandFailed(w: Write) =>
      println("whatever")
      // TODO: analyze this case according to the docs

    case _: ConnectionClosed =>
      //          listener ! "connection closed"
      context stop self

    case other =>
      log.warning(s"Received unexpected $other")
  }

  def waitingForResponse(connection: ActorRef, requestor: ActorRef): Receive = {
    case Received(data) =>
      log.debug("Received {}", data.decodeString("utf-8"))
      ServerProtocol.decode(data) match {
        case Some(msg) =>
          requestor ! msg
        // TODO: what if we cannot decode any data? probably we need to do some buffering
      }
      context become connected(connection)
  }

}

/*
  Flow of commands:
  - Poll -> Record
  - Publish -> PublishAck
  - Commit -> CommitAck
 */


object ActorClientProducer extends App {
  def props = Props(new ActorClientProducer)
}
