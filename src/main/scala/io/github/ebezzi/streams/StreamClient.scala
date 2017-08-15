package io.github.ebezzi.streams

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import scala.concurrent.duration._

import scala.concurrent.Future


/**
  * Created by ebezzi on 15/07/2017.
  */
object StreamClient extends App {

  implicit val system = ActorSystem()

  import system.dispatcher

  implicit val mat = ActorMaterializer()

  val connection = Tcp().outgoingConnection("localhost", 8000)

    val replParser =
      Flow[String].takeWhile(_ != "q")
        .concat(Source.single("BYE"))
        .map(elem => ByteString(s"$elem\n"))

    val repl = Flow[ByteString]
      .via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 256,
        allowTruncation = true))
      .map(_.utf8String)
      .map(text => println("Server: " + text))
      .map(_ => readLine("> "))
      .via(replParser)

    connection.join(repl).run().onComplete(println)

  //  Source.single("stocazzo").map(ByteString(_)).via(connection).map(_.utf8String).runWith(Sink.foreach(println))

  //  connection.join()

//  Source.single("foo")
//    .map(ByteString(_))
//    .via(connection)
//    .flatMapConcat { recv =>
//      Source.single()
//    }
//    .map(_.utf8String)
//    .runWith(Sink.foreach(println))
//    .onComplete { r =>
//      println(r)
//      system.terminate()
//    }


  //  Source.single("stocazzo")
  //    .map(ByteString(_))
  //    .via(connection)
  //    .flatMapConcat { recv =>
  //      Source.
  //    }
  //    .runWith(Sink.foreach(println))


}
