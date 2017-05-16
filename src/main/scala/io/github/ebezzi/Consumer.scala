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

object Consumer extends App {

  implicit val system = ActorSystem("consumer")
  val consumer = new Consumer
  import system.log

  while (true) {
    Thread.sleep(500)
    val record = consumer.poll()
    log.info("Consumed record: {}", record)
    consumer.commit(record)
    log.info("Committed record: {}", record)
  }

}

class Consumer(implicit val system: ActorSystem) {

  implicit val timeout = Timeout(10.seconds)
  import system.dispatcher

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  val client = system.actorOf(ActorClient.props)

  def pollAsync(): Future[Record] =
    (client ? Poll).mapTo[Record]

  def poll(): Record =
    Await.result((client ? Poll).mapTo[Record], 10.seconds)

  def commit(record: Record): Unit =
    Await.result(client ? Commit(record.offset), 10.seconds)

}