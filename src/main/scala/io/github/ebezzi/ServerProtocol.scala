package io.github.ebezzi

import java.nio.ByteOrder

import akka.util.{ByteString, ByteStringBuilder}


sealed trait ServerProtocol
case class Record(offset: Long, data: Array[Byte]) extends ServerProtocol {
  override def toString: String = s"Record($offset, ${new String(data)})"
}
case object CommitAck extends ServerProtocol
case object WriteAck extends ServerProtocol
case object RegisterAck extends ServerProtocol

object ServerProtocol {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val SendDataMagic = 0.toByte
  val CommitAckMagic = 1.toByte
  val WriteAckMagic = 2.toByte
  val RegisterAckMagic = 3.toByte

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

  def registerAck =
    new ByteStringBuilder()
      .putByte(RegisterAckMagic)
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
      case `RegisterAckMagic` =>
        Some(RegisterAck)
      case otherwise =>
        None
    }
  }

}