package io.github.ebezzi

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import io.github.ebezzi.LogReader.reader

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.Random

class LogWriter(file: File) {

  file.createNewFile()

  var currentOffset = 1 + scanLastOffset()

  val stream = new FileOutputStream(file, true)
  val channel = stream.getChannel

  def scanLastOffset(): Long = {
    val reader = new LogReader(file)
    val last = reader.lastRecord
    reader.close()
    last.offset
  }

  def encode(bytes: Array[Byte]) = {
    // Message format:
    // - Offset (8 bytes)
    // - Length (4 bytes)
    // - Payload (N bytes)

    val buffer = ByteBuffer.allocate(8 + 4 + bytes.length)
    buffer.putLong(currentOffset)
    buffer.putInt(bytes.length)
    buffer.put(bytes)
    buffer.rewind()
    buffer
  }

  def append(bytes: Array[Byte]) = {
    channel.write(encode(bytes))
    currentOffset += 1
  }

  def close() = {
    channel.close()
    stream.close()
  }

}

final class LogReader(file: File) {

  private val raf = new RandomAccessFile(file, "r")
  private val channel: FileChannel = raf.getChannel

  private var position = 0L

  case class Record(offset: Long, size: Int)

  private def read() = {
    val buffer = ByteBuffer.allocate(12)
    val remaining = channel.read(buffer, position)
    buffer.rewind()
    if (remaining > -1) {
      val offset = buffer.getLong()
      val size = buffer.getInt()
      Record(offset, size)
    } else {
      Record(-1, 0)
    }
  }

  private def advance(record: Record): Unit =
    position = 12 + position + record.size

  // TODO: does not go backwards
  @tailrec def moveTo(offset: Long): Unit = {
    val record = read()
    if (record.offset == -1) return
    if (record.offset == offset) return
    advance(record)
    moveTo(offset)
  }

  def lastRecord: Record = {
    @tailrec def move(nextRecord: Record): Record = {
      advance(nextRecord)
      val n = read()
      if (n.offset == -1) return nextRecord
      move(n)
    }
    move(read())
  }

  def next(): Array[Byte] = {
    val record = read()
    if (record.offset == -1) return Array.empty[Byte]
    val buffer = ByteBuffer.allocate(record.size)
    channel.read(buffer, 12 + position)
    advance(record)
    buffer.array()
  }

  private def readBytes(record: Record): Array[Byte] = {
    val buffer = ByteBuffer.allocate(record.size)
    channel.read(buffer, 12 + position)
    buffer.array()
  }

  def fromOffset(offset: Long): Array[Byte] = {
    println(s"Position $position, requested $offset")
    val record = read()
    record match {
      case record @ Record(`offset`, _) =>
        readBytes(record)
      case Record(-1, _) =>
        Array.empty[Byte]
      case record =>
        println(s"Moving to $offset")
        moveTo(offset)
        readBytes(read())
    }

  }

  // To be used on compacted records
  def readAll(): Iterable[Array[Byte]] =
    Stream.continually(next()).takeWhile(_.nonEmpty).toIterable

  def close() = {
    channel.close()
    raf.close()
  }

}

object LogWriter extends App {

  val file = new File("00000.dat")
  val writer = new LogWriter(file)

//  file.delete()
//  file.createNewFile()

  val phrases = Seq("ciao a tutti", "da emanuele bezzi", "nato a milano nel 1985", "lavora in DATABIZ")

//  writer.append("ciao a tutti".getBytes)
//  writer.append("da emanuele bezzi".getBytes)
//  writer.append("nato a milano nel 1985".getBytes)
//  writer.append("lavora in DATABIZ".getBytes)

  writer.append(Random.shuffle(phrases).head.getBytes)

  writer.close()


}


object LogReader extends App {

  val file = new File("00000.dat")
  val reader = new LogReader(file)

  def printByteArray(ba: Array[Byte]) =
    println(s"ByteArray size:${ba.length} content: ${new String(ba)}")


  Stream.continually(reader.next()).takeWhile(_.nonEmpty).toList.map(printByteArray)


}
