package io.github.ebezzi

import java.nio.ByteOrder

import akka.util.{ByteString, ByteStringBuilder}


object ClientProtocol {

  sealed trait Protocol

  // Registers a consumer
  case class RegisterConsumer(topic: String, consumerId: Int) extends Protocol

  // Commits the offset for consumerId `consumerId`. Will not retrieve new data
  case class CommitOffset(consumerId: Int, offset: Long) extends Protocol

  // Polls for the next record(s). TODO: for now we use only one record at a time
  case class Poll(consumerId: Int) extends Protocol

  // Publishes some data
  case class PublishData(topic: String, data: Array[Byte]) extends Protocol

  case object GetAllElements extends Protocol

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val RegisterConsumerMagic = 0.toByte
  val CommitOffsetMagic = 1.toByte
  val PollMagic = 2.toByte
  val PublishDataMagic = 3.toByte
  val GetAllMagic = 4.toByte

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

  def getAll =
    new ByteStringBuilder()
      .putByte(GetAllMagic)
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
        val topicSize = buffer.getInt()
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
      case `GetAllMagic` =>
        Some(GetAllElements)
      case otherwise =>
        None
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
