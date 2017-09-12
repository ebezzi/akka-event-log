package io.github.ebezzi

import java.io.File
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.io.Tcp._

import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.Await

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

        // This is just used to store the offset for this coordinator
        // This is not necessarily the best choice, but it's ok for now
        // Note: one might want to cache the topic location for speed here.
        case Some(rc@RegisterConsumer(requestedTopic, consumerId)) =>
          log.info("{} registered for topic {}", consumerId, requestedTopic)
          producer = new Producer
          // send the registration message to the leader
          // TODO: if you can't register a consumer, you should nack this
          topic = requestedTopic
          if (topic != "_offset") {
            val consumer = new Consumer("_offset")
            // TODO: this is a hack
            val lv = ByteBuffer.wrap(consumer.lastValue().data).getLong
            log.info("Recovering from offset {}", lv)
            lastCommittedOffset = lv
          }
          sender ! Write(ProtocolFraming.encode(ServerProtocol.registerAck))

        // Poll requires the location of the consumer
        case Some(poll@Poll(consumerId)) =>
          log.info("Received poll from consumerId {}", consumerId)
          //          val toSend = reader.fromOffset(lastCommittedOffset)
          //          log.warning("Polled record: {} at offset {}", new String(toSend, "utf-8"), lastCommittedOffset)
          //          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(lastCommittedOffset, toSend)))
          topicManager.leaderForTopic(topic)
            .ask(LeaderPoll(lastCommittedOffset))
            .map { case LeaderResponse(m) => Write(m) }
            .pipeTo(sender)

        // We have two possibilities:
        // (1) produce the commit here and propagate the lastCommittedOffset along with the Poll message
        // (2) Send the commit to the partition leader and let him manage the lastCommittedOffset
        // We will try with (2) initially
        case Some(co@CommitOffset(consumerId, offset)) =>
          log.info("Received commit from consumerId {}", consumerId)
          val requestor = sender
          producer.produce("_offset", 1 + offset).map { _ =>
            log.info("Committing offset {}", 1 + offset)
            lastCommittedOffset = 1 + offset
            requestor ! Write(ProtocolFraming.encode(ServerProtocol.commitAck))
          }
        //          topicManager.leaderForTopic(topic) forward co

        // TODO: not sure on how to do this
        case Some(GetAllElements) =>
          log.info("GetAllElements for topic {}", topic)
          //          val last = reader.readAll().last
          //           TODO: this could use a different object
          //          sender ! Write(ProtocolFraming.encode(ServerProtocol.record(-1, last)))
          topicManager.leaderForTopic(topic)
            .ask(LeaderGetAllElements)
            .map { case LeaderResponse(m) => Write(m) }
            .pipeTo(sender)

        // Find the location of the topic, write to it
        case Some(pd@PublishData(topic, _)) =>
          log.info("Publishing to topic {} (address {}): {}", topic,
            topicManager.leaderForTopic(topic), data.length)
          //          writerFor(topic).append(data)
          //          sender ! Write(ProtocolFraming.encode(ServerProtocol.writeAck))
          topicManager.leaderForTopic(topic) ? pd map { case LeaderResponse(m) => Write(m) } pipeTo sender
      }

    case PeerClosed =>
      context stop self

  }

}