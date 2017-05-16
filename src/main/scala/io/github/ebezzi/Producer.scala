package io.github.ebezzi

import java.nio.ByteOrder
import java.nio.charset.Charset

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source, Tcp}
import akka.util.{ByteString, ByteStringBuilder}
import io.github.ebezzi.Producer.system
import scala.concurrent.duration._

import scala.concurrent.Future


class Producer(implicit val system: ActorSystem, mat: ActorMaterializer) {

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  val client = system.actorOf(ActorClient.props)

  def produce(data: String) =
    client ! Publish(data.getBytes(Charset.defaultCharset()))

}


object Producer extends App {

  implicit val system = ActorSystem("producer")
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  val producer = new Producer

  system.scheduler.schedule(0.seconds, 1.seconds) {
    producer.produce(s"Message produced at ${System.currentTimeMillis}")
  }
//
//  producer.send("ciao a tutti".getBytes).map(_.decodeString("utf-8")).foreach { resp =>
//    println(resp)
//    system.terminate()
//  }




}
