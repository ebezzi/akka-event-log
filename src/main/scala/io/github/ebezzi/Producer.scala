package io.github.ebezzi

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.Charset

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source, Tcp}
import akka.util.{ByteString, ByteStringBuilder, Timeout}
import io.github.ebezzi.Producer.system

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.pattern.ask
import com.typesafe.config.ConfigFactory


class Producer(implicit val system: ActorSystem) {

  implicit val timeout = Timeout(10.seconds)
  import system.dispatcher

  private val client = system.actorOf(ActorClientProducer.props)

  def produce(topic: String, data: String) =
    client ? Publish(topic, data.getBytes(Charset.defaultCharset()))

  def produce(topic: String, data: Long) =
    client ? Publish(topic, ByteBuffer.allocate(8).putLong(data).array())

}


object Producer extends App {

  implicit val system = ActorSystem("producer", ConfigFactory.load().getConfig("clients"))
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  val producer = new Producer

//  system.scheduler.schedule(0.seconds, 1.seconds) {
  producer.produce("00000", s"Message produced at ${System.currentTimeMillis}")
//  }
//
//  producer.send("ciao a tutti".getBytes).map(_.decodeString("utf-8")).foreach { resp =>
//    println(resp)
//    system.terminate()
//  }




}
