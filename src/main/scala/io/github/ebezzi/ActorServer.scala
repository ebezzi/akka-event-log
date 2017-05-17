package io.github.ebezzi

import java.io.File
import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.io.Tcp._
import akka.util.{ByteString, ByteStringBuilder}

/**
  * Created by emanuele on 06/01/17.
  */
class ActorServer extends Actor with ActorLogging {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 7777))

  def receive = {
    case b @ Bound(localAddress) =>
      log.info("Bound to address: {}", b)

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      log.info("Received connection from: {}", remote)
      val handler = context.actorOf(Props[ConsumerHandler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class ConsumerHandler extends Actor with ActorLogging {
  import Tcp._
  import Protocol._

  val file = new File("00000.dat")
  val reader = new LogReader(file)
  val writer = new LogWriter(file)

  val offsetStorage = new File("offsets.dat")
  val offsetManagerReader = new LogReader(offsetStorage)
  val offsetManagerWriter = new LogWriter(offsetStorage)

  // TODO: this should be saved to a database
  var lastCommittedOffset = 0L

  def receive = {
    case Received(data) =>
      Protocol.decode(data) match {

        case Some(RegisterConsumer(consumerId)) =>
          log.info("{} registered", consumerId)
          // TODO: match the consumerId and get the right partition (for now, assume offset is 0)

        case Some(CommitOffset(consumerId, offset)) =>
          log.info("Received commit from consumerId {}", consumerId)
          lastCommittedOffset = 1+offset
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

sealed trait ServerProtocol
case class Record(offset: Long, data: Array[Byte]) extends ServerProtocol {
  override def toString: String = s"Record($offset, ${new String(data)})"
}
case object CommitAck extends ServerProtocol
case object WriteAck extends ServerProtocol

object ServerProtocol {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val SendDataMagic = 0.toByte
  val CommitAckMagic = 1.toByte
  val WriteAckMagic = 2.toByte

  def record(offset: Long, data: Array[Byte]) =
    new ByteStringBuilder()
      .putByte(SendDataMagic)
      .putLong(offset)
      .putBytes(data)
      .result()

  def commitAck =
    new ByteStringBuilder()
      .putByte(CommitAckMagic)
      .result()

  def writeAck =
    new ByteStringBuilder()
      .putByte(WriteAckMagic)
      .result()

  def decode(bs: ByteString): Option[ServerProtocol] = {
    val buffer = bs.toByteBuffer
    val magic = buffer.get()

    magic match {
      case `SendDataMagic` =>
        val offset = buffer.getLong()
        val data: Array[Byte] = new Array(buffer.remaining())
        buffer.get(data)
        Some(Record(offset, data))
      case `CommitAckMagic` =>
        Some(CommitAck)
      case `WriteAckMagic` =>
        Some(WriteAck)

      case otherwise =>
        None
    }
  }

}

object ActorServer extends App {

  val system = ActorSystem("tcp-manager")
  system.actorOf(Props(new ActorServer))

}
