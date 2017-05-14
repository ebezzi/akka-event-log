//package io.github.ebezzi
//
//import java.io.File
//import java.net.InetSocketAddress
//
//import akka.actor.{Actor, ActorLogging, Props}
//import akka.io.{IO, Tcp}
//import akka.io.Tcp._
//
//// TODO: merge this with the main server
//class ProducerServer extends Actor with ActorLogging {
//
//  import Tcp._
//  import context.system
//
//  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 7778))
//
//  def receive = {
//    case b @ Bound(localAddress) =>
//      log.info("Bound to address: {}", b)
//
//    case CommandFailed(_: Bind) =>
//      context stop self
//
//    case c @ Connected(remote, local) =>
//      log.info("Received connection from: {}", remote)
//      val handler = context.actorOf(Props[ProducerHandler])
//      val connection = sender()
//      connection ! Register(handler)
//  }
//
//}
//
//
//class ProducerHandler extends Actor with ActorLogging {
//  import Tcp._
//
//  val file = new File("00000.dat")
//    val writer = new LogWriter(file)
//
//
//  def receive = {
//    case Received(data) =>
//
//      log.info("Received data: {}", data)
//
//      Protocol.decode(data) match {
//        case Some(RegisterConsumer(consumerId)) =>
//          log.info("{} registered", consumerId)
//          // TODO: match the consumerId and get the right partition (for now, assume offset is 0)
//          val toSend = reader.next()
//          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(0L, toSend)))
//          log.info("Consumer initializing")
//
//        case Some(CommitOffset(offset)) =>
//          lastCommittedOffset = offset
//          val toSend = reader.next()
//          if (toSend.nonEmpty){
//            sender ! Write(ProtocolFraming.encode(ServerProtocol.record(1 + lastCommittedOffset, toSend)))
//            log.info("Sending {} bytes to the consumer with offset {}" , toSend.length, 1 + lastCommittedOffset)
//          } else {
//            sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))
//            log.info("Sending empty response to the consumer")
//          }
//      }
//
//    case PeerClosed =>
//      context stop self
//
//  }
//
//}