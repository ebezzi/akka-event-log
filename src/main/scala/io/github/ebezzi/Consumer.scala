package io.github.ebezzi

import java.io.File
import java.nio.ByteOrder
import java.nio.charset.Charset

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.{Flow, Framing, Sink, Source, Tcp}
import akka.util.{ByteString, ByteStringBuilder, Timeout}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.pattern.ask
import com.typesafe.config.ConfigFactory

object Consumer extends App {

  implicit val system = ActorSystem("consumer", ConfigFactory.load().getConfig("clients"))
  val consumer = new Consumer("00000")
  import system.log

  while (true) {
    val record = consumer.poll()
    if (record.data.nonEmpty)
      log.info("Consumed record: {}", record)
    consumer.commit(record)
    Thread.sleep(100)
  }


}

class Consumer(topic: String)(implicit val system: ActorSystem) {

  implicit val timeout = Timeout(10.seconds)
  import system.dispatcher

  private val client = system.actorOf(ActorClient.props(topic))

  def pollAsync(): Future[Record] =
    (client ? Poll).mapTo[Record]

  def poll(): Record =
    Await.result((client ? Poll).mapTo[Record], 10.seconds)

  // TODO: we need a more powerful method to get the offset
  def lastValue(): Record =
    Await.result((client ? GetAll).mapTo[Record], 10.seconds)


  def commit(record: Record): Unit =
    if (record.data.isEmpty) () else Await.result(client ? Commit(record.offset), 10.seconds)

}