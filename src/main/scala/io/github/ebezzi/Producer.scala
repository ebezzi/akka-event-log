package io.github.ebezzi

import java.nio.ByteOrder

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source, Tcp}
import akka.util.{ByteString, ByteStringBuilder}

import scala.concurrent.Future


class Producer(implicit val system: ActorSystem, mat: ActorMaterializer) {

  val connection = Tcp().outgoingConnection("127.0.0.1", 7777)

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  def encode(ba: Array[Byte]) =
    new ByteStringBuilder()
      .putInt(ba.length)
      .putBytes(ba)
      .result()

  def send(ba: Array[Byte]): Future[ByteString] =
    Source.single(ba)
      .map(encode)
      .via(connection)
      .runWith(Sink.head)

}


object Producer extends App {

  implicit val system = ActorSystem("producer")
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  val producer = new Producer

  producer.send("ciao a tutti".getBytes).map(_.decodeString("utf-8")).foreach { resp =>
    println(resp)
    system.terminate()
  }



}
