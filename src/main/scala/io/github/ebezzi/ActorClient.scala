package io.github.ebezzi

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.io.{IO, Tcp}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.{ByteString, ByteStringBuilder}

// These messages are supposed to be mapped to function calls (to hide the actor behind it)
sealed trait Message
case class Commit(offset: Long) extends Message
case class Publish(topic: String, data: Array[Byte]) extends Message
case object Poll extends Message

// TODO: producer do not need a topic, so this can be omitted. Maybe can be done better
class ActorClient(topic: String = "") extends Actor with ActorLogging with Stash {

  val remote = new InetSocketAddress("localhost", 7000)

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

      log.info("Registering consumer {}", consumerId)

      connection ! Write(ProtocolFraming.encode(Protocol.registerConsumer(topic, consumerId)))

      log.info("Unstashing all")
      unstashAll()

      context become connected(connection)

    case other =>
      log.info("Stashing {}", other)
      stash()
  }

  def connected(connection: ActorRef): Receive = {

    case Commit(offset) =>
      connection ! Write(ProtocolFraming.encode(Protocol.commitOffset(consumerId, offset)))
      context become waitingForResponse(connection, sender)

    case Poll =>
      connection ! Write(ProtocolFraming.encode(Protocol.poll(consumerId)))
      context become waitingForResponse(connection, sender)

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


object Protocol {

  sealed trait Protocol

  // Registers a consumer
  case class RegisterConsumer(topic: String, consumerId: Int) extends Protocol

  // Commits the offset for consumerId `consumerId`. Will not retrieve new data
  case class CommitOffset(consumerId: Int, offset: Long) extends Protocol

  // Polls for the next record(s). TODO: for now we use only one record at a time
  case class Poll(consumerId: Int) extends Protocol

  case class PublishData(topic: String, data: Array[Byte]) extends Protocol

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val RegisterConsumerMagic = 0.toByte
  val CommitOffsetMagic = 1.toByte
  val PollMagic = 2.toByte
  val PublishDataMagic = 3.toByte

  def registerConsumer(topic: String, consumerId: Int) =
    new ByteStringBuilder()
      .putByte(RegisterConsumerMagic)
      .putInt(topic.length)
      .putBytes(topic.getBytes)
      .putInt(consumerId)
      .result()

  def commitOffset(consumerId: Int, offset: Long) =
    new ByteStringBuilder()
      .putByte(CommitOffsetMagic)
      .putInt(consumerId)
      .putLong(offset)
      .result()

  def poll(consumerId: Int) =
    new ByteStringBuilder()
      .putByte(PollMagic)
      .putInt(consumerId)
      .result()

  def publishData(topic: String, data: Array[Byte]) =
    new ByteStringBuilder()
      .putByte(PublishDataMagic)
      .putInt(topic.length)
      .putBytes(topic.getBytes)
      .putInt(data.length)
      .putBytes(data)
      .result()

  def decode(bs: ByteString): Option[Protocol] = {
    val buffer = bs.toByteBuffer
    val magic = buffer.get()
    magic match {
      case `RegisterConsumerMagic` =>
        val topicSize = buffer.getInt
        val topicDst = Array.ofDim[Byte](topicSize)
        buffer.get(topicDst)
        Some(RegisterConsumer(new String(topicDst), buffer.getInt()))
      case `CommitOffsetMagic` =>
        Some(CommitOffset(buffer.getInt(), buffer.getLong()))
      case `PollMagic` =>
        Some(Poll(buffer.getInt()))
      case `PublishDataMagic` =>
        val topicSize = buffer.getInt
        val topicDst = Array.ofDim[Byte](topicSize)
        buffer.get(topicDst)
        val size = buffer.getInt()
        val dst = Array.ofDim[Byte](size)
        buffer.get(dst)
        Some(PublishData(new String(topicDst), dst))
      case otherwise => None
    }
  }

}

object ProtocolFraming {

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  def encode(i: Int): ByteString =
    encode(ByteString.fromInts(i))

  def encode(s: String): ByteString =
    encode(ByteString.fromString(s))

  def encode(ba: Array[Byte]): ByteString =
    encode(ByteString.fromArray(ba))

  def encode(bs: ByteString): ByteString =
    bs

  def decode(bs: ByteString) =
    bs

}

class JustLogActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case msg => log.info("Received: {}", msg)
  }
}



object ActorClient extends App {
  def props(topic: String = "") = Props(new ActorClient(topic))
}
