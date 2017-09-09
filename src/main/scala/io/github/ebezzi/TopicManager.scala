package io.github.ebezzi

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSelection, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._
import akka.cluster.pubsub.DistributedPubSub
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/*

The topic manager is responsible for informing handlers about topic information
(if they exist, where they're located, what's their partitioning)
It needs to do 3 things:
1) Given a topic/partition, return the location
2) Add a location for a certain topic/partition

In the future, it might be useful if we do a subscription mechanism so handlers can register to updates (similar
to how Akka cluster works), but for now it's enough.

Design choice: I expect the best solution would be to use a special topic.
For now let's stick to a memory solution

 */

case class RegisterTopic(topic: String, address: String)
case class GetLeaderAddress(topic: String)

class TopicManager(system: ActorSystem) {

  implicit val timeout = Timeout(10.seconds)

  private val actor = system.actorOf(Props(new TopicManagerActor))

  def leaderForTopic(topic: String): ActorSelection = {
    // TODO: is there a way to fix this hack? Maybe retuning a future could be ok...
    val maybeAddress = Await.result(actor ? GetLeaderAddress(topic), 10.seconds)
    maybeAddress match {
      case Some(address) =>
        system.actorSelection(s"$address/user/partitions/$topic")
      case None =>
        val members = Cluster(system).state.members
        val chosenMember = members.toIndexedSeq(topic.hashCode % members.size)
        system.actorSelection(s"$chosenMember/user/partitions/$topic")
    }
  }

  def registerTopic(topic: String, location: ActorRef) =
    actor ! RegisterTopic(topic, Cluster(system).selfAddress.toString)

  def bootstrap() = {

    val topics = new File(system.settings.config.getString("server.data-dir") + "/.")
      .listFiles
      .filter(_.getName.endsWith(".dat"))
      .map(_.getName)
      .map(_.dropRight(4))
      .toList

    println(topics)

    for (topic <- topics)
      actor ! RegisterTopic(topic, Cluster(system).selfAddress.toString)

  }

  bootstrap()

}

class TopicManagerActor extends Actor with ActorLogging {

  val replicator = DistributedData(context.system).replicator
  implicit val node = Cluster(context.system)

  val key = LWWMapKey[String, String]("topics")
  replicator ! Subscribe(key, self)

  var map: Map[String, String] = _

  self ! Get(key, ReadLocal)

  log.info("Topic manager started")

  override def receive: Receive = {

    case RegisterTopic(topic, address) =>
      log.info("Registering topic {} for address {}", topic, address)
      replicator ! Update(key, LWWMap.empty[String, String], WriteLocal) { _ + (topic -> address) }

    // TODO: this message should be handled only when we're sure the topic manager is fully up-to-date!
    case GetLeaderAddress(topic) =>
      sender ! map.get(topic)

    case c @ Changed(`key`) =>
      val data = c.get(key)
      log.info("Current elements: {}", data.entries)
      map = data.entries

    case c @ GetSuccess(`key`, None) =>
      val data = c.get(key)
      log.info("Current elements: {}", data.entries)
      map = data.entries
  }


}