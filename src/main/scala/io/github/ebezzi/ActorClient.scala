package io.github.ebezzi

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.event.{Logging, LoggingReceive}
import akka.io.{IO, Tcp}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.{ByteString, ByteStringBuilder}

// These messages are supposed to be mapped to function calls (to hide the actor behind it)
sealed trait Message
case class Commit(offset: Long) extends Message
case class Publish(topic: String, data: Array[Byte]) extends Message
case object Poll extends Message
case object GetAll extends Message

class ActorClient(topic: String) extends Actor with ActorLogging with Stash {

  // TODO: configuration
  val remote = new InetSocketAddress("localhost", 7001)

  // TODO: include this in the initial message
  val consumerId = 1337

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      context stop self

    case Connected(remote, local) =>
      val connection = sender()
      connection ! Register(self)

      log.info("Registering consumer {} for topic {}", consumerId, topic)

      // TODO: Add a timeout for failed consumer registration
      connection ! Write(ProtocolFraming.encode(ClientProtocol.registerConsumer(topic, consumerId)))
      context.become({
        case Received(_) =>
          log.info("Consumer successfully registered, unstashing messages")
          unstashAll()
          context become connected(connection)
      })

    case other =>
      log.info("Stashing {}", other)
      stash()
  }

  def connected(connection: ActorRef): Receive = {

    case Commit(offset) =>
      connection ! Write(ProtocolFraming.encode(ClientProtocol.commitOffset(consumerId, offset)))
      context become waitingForResponse(connection, sender)

    case Poll =>
      connection ! Write(ProtocolFraming.encode(ClientProtocol.poll(consumerId)))
      context become waitingForResponse(connection, sender)

    case GetAll =>
      connection ! Write(ProtocolFraming.encode(ClientProtocol.getAll))
      context become waitingForResponse(connection, sender)

    case Publish(topic, data) =>
      log.info("Publishing {} bytes to topic {}", data.length, topic)
      connection ! Write(ProtocolFraming.encode(ClientProtocol.publishData(topic, data)))
      context become waitingForResponse(connection, sender)

    case CommandFailed(w: Write) =>
      println("whatever")
      // TODO: analyze this case according to the docs

    case _: ConnectionClosed =>
      context stop self

    case other =>
      log.warning(s"Received unexpected $other")
  }

  private def waitingForResponse(connection: ActorRef, requestor: ActorRef) = LoggingReceive(Logging.DebugLevel) {
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

object ActorClient extends App {
  def props(topic: String) = Props(new ActorClient(topic))
}
