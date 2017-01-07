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
      val handler = context.actorOf(Props[SimplisticHandler])
      val connection = sender()
      connection ! Register(handler)
  }

}

class SimplisticHandler extends Actor with ActorLogging {
  import Tcp._

  val file = new File("00000.dat")
  val reader = new LogReader(file)
//  val writer = new LogWriter(file)

  var lastCommittedOffset = -1L

  def receive = {
    case Received(data) =>

      log.info("Received data: {}", data)

      Protocol.decode(data) match {
        case Some(RegisterConsumer(consumerId)) =>
          log.info("{} registered", consumerId)
          // TODO: match the consumerId and get the right partition (for now, assume offset is 0)
          val toSend = reader.next()
          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(0L, toSend)))
          log.info("Consumer initializing")

        case Some(CommitOffset(offset)) =>
          lastCommittedOffset = offset
          val toSend = reader.next()
          if (toSend.nonEmpty){
            sender ! Write(ProtocolFraming.encode(ServerProtocol.record(1 + lastCommittedOffset, toSend)))
            log.info("Sending {} bytes to the consumer with offset {}" , toSend.length, 1 + lastCommittedOffset)
          } else {
            sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))
            log.info("Sending empty response to the consumer")
          }
      }

    case PeerClosed =>
      context stop self

  }

}

sealed trait ServerProtocol
case class Record(offset: Long, data: Array[Byte]) extends ServerProtocol {
  override def toString: String = s"Record($offset, ${new String(data)})"
}

object ServerProtocol {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val SendDataMagic = 0.toByte

  def record(offset: Long, data: Array[Byte]) =
    new ByteStringBuilder()
      .putByte(SendDataMagic)
      .putLong(offset)
      .putBytes(data)
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

      case otherwise =>
        None
    }
  }

}

object ActorServer extends App {

  val system = ActorSystem("tcp-manager")
  system.actorOf(Props(new ActorServer))

}
