package io.github.ebezzi

import akka.actor.ActorRef
import akka.cluster.Cluster

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
class TopicManager(cluster: Cluster) {

  def leaderForTopic(topic: String): ActorRef = ???

  def registerTopic(topic: String, location: ActorRef) = ???

}
