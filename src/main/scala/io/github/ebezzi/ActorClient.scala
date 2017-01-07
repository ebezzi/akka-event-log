package io.github.ebezzi

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.{ByteString, ByteStringBuilder}

sealed trait Message
case class Commit(offset: Long) extends Message

class ActorClient(listener: ActorRef) extends Actor with ActorLogging {

  val remote = new InetSocketAddress("localhost", 7777)

  val consumerId = 1337

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      //      listener ! "connect failed"
      context stop self

    case c@Connected(remote, local) =>
      //      listener ! c
      val connection = sender()
      connection ! Register(self)

      log.info("Registering consumer {}", consumerId)

      connection ! Write(ProtocolFraming.encode(Protocol.registerConsumer(consumerId)))

      context become connected(connection)
  }

  def connected(connection: ActorRef): Receive = {

    //    case data: ByteString =>
    //      connection ! Write(data)

    case Commit(offset) =>
      connection ! Write(ProtocolFraming.encode(Protocol.commitOffset(offset)))

    case CommandFailed(w: Write) =>
    // O/S buffer was full
    //          listener ! "write failed"

    case Received(data) =>
      //      listener ! data
      //      log.info("Received {}", data.decodeString("utf-8"))
      ServerProtocol.decode(data) match {
        case Some(r: Record) => listener ! r
      }

    case "close" =>
      connection ! Close

    case _: ConnectionClosed =>
      //          listener ! "connection closed"
      context stop self
  }

}

sealed trait Protocol

case class RegisterConsumer(consumerId: Int) extends Protocol

case class CommitOffset(offset: Long) extends Protocol

object Protocol {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val RegisterConsumerMagic = 0.toByte
  val CommitOffsetMagic = 1.toByte

  def registerConsumer(consumerId: Int) =
    new ByteStringBuilder()
      .putByte(RegisterConsumerMagic)
      .putInt(consumerId)
      .result()

  def commitOffset(offset: Long) =
    new ByteStringBuilder()
      .putByte(CommitOffsetMagic)
      .putLong(offset)
      .result()

  def decode(bs: ByteString): Option[Protocol] = {
    val buffer = bs.toByteBuffer
    val magic = buffer.get()
    magic match {
      case `RegisterConsumerMagic` => Some(RegisterConsumer(buffer.getInt()))
      case `CommitOffsetMagic` => Some(CommitOffset(buffer.getLong()))
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

//object ProtocolFraming {
//
//  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
//
//  def encode(i: Int): ByteString =
//    encode(ByteString.fromInts(i))
//
//  def encode(s: String): ByteString =
//    encode(ByteString.fromString(s))
//
//  def encode(ba: Array[Byte]): ByteString =
//    encode(ByteString.fromArray(ba))
//
//  def encode(bs: ByteString): ByteString =
//    new ByteStringBuilder()
//      .putInt(bs.length)
//      .append(bs)
//      .result()
//
//  def decode(bs: ByteString) =
//    bs.drop(4)
//
//}

class JustLogActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case msg => log.info("Received: {}", msg)
  }
}

class ConsumerActor extends Actor with ActorLogging {

  val client = context.actorOf(Props(new ActorClient(self)))

  override def receive: Receive = {
    case record@Record(offset, data) =>
      log.info("Consumed: {}", record)
      // TODO: we need a poll command, because we need to commit the offset as soon as possible, but wait to poll new data
      Thread.sleep(1000)
      client ! Commit(offset)
  }

}

object ActorClient extends App {

  import scala.concurrent.duration._

  val system = ActorSystem("consumer")
  import system.dispatcher

//  val logger = system.actorOf(Props(new JustLogActor))
//  val client = system.actorOf(Props(new ActorClient(logger)))

  val actor = system.actorOf(Props(new ConsumerActor))


}
