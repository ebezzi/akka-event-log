package io.github.ebezzi

import java.io.File
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.io.Tcp._

import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.util.Timeout

// The connection handler handles incoming connections. It needs to know the cluster topology, in particular
// which node contains a determined topic/partition. If a new topic/partition is encountered, we send a request
// to create one on a random node.
// The connection handler needs to keep track of the offset for each consumerId (i.e. each connection).
class Coordinator(topicManager: TopicManager) extends Actor with ActorLogging {

  import Protocol._
  import context.{system, dispatcher}

  val cluster = Cluster(context.system)
  implicit val timeout = Timeout(10.seconds)

  // This is for writing offsets
  // TODO: we need a more complex structure
  var producer: Producer = _

  var topic: String = _

  // Will be fetched when a consumer is registered
  var lastCommittedOffset: Long = _

  def receive = {
    case Received(data) =>
      Protocol.decode(data) match {

        // If a consumer is registered, we need to find the location of the topic
        case Some(rc @ RegisterConsumer(requestedTopic, consumerId)) =>
          log.info("{} registered for topic {}", consumerId, requestedTopic)
//          producer = new Producer
          // send the registration message to the leader
          // TODO: if you can't register a consumer, you should nack this
          topic = requestedTopic
          topicManager.leaderForTopic(topic) forward rc

        // We have two possibilities:
        // (1) produce the commit here and propagate the lastCommittedOffset along with the Poll message
        // (2) Send the commit to the partition leader and let him manage the lastCommittedOffset
        // We will try with (2) initially
        case Some(co @ CommitOffset(consumerId, offset)) =>
          log.info("Received commit from consumerId {}", consumerId)
//          val requestor = sender
//          producer.produce("_offset", 1 + offset).map { _ =>
//            log.info("Offset committed...")
//            lastCommittedOffset = 1 + offset
//            requestor ! Write(ProtocolFraming.encode(ServerProtocol.commitAck))
//          }
          topicManager.leaderForTopic(topic) forward co

        // Poll requires the location of the consumer
        case Some(poll @ Poll(consumerId)) =>
          log.info("Received poll from consumerId {}", consumerId)
//          val toSend = reader.fromOffset(lastCommittedOffset)
//          log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), lastCommittedOffset)
//          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))
          topicManager.leaderForTopic(topic) forward poll

        // TODO: not sure on how to do this
        case Some(GetAllElements) =>
//          val last = reader.readAll().last
//           TODO: this could use a different object
//          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(-1, last)))
          topicManager.leaderForTopic(topic) forward GetAllElements

        // Find the location of the topic, write to it
        case Some(pd @ PublishData(topic, _)) =>
          log.info("Publishing to topic {} (address {}): {}", topic,
            topicManager.leaderForTopic(topic), data.length)
//          writerFor(topic).append(data)
//          sender ! Write(ProtocolFraming.encode(ServerProtocol.writeAck))
          topicManager.leaderForTopic(topic) ? pd map { case LeaderResponse(m) => m } pipeTo sender
      }

    case PeerClosed =>
      context stop self

  }

}