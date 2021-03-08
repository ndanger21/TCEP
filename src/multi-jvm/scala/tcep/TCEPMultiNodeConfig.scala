package tcep

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

/**
  * executes a multi-node akka cluster test by running one JVM for each node
  * run using 'sbt multi-jvm:test'
  * see https://doc.akka.io/docs/akka/2.5/multi-node-testing.html
  */
// config for test nodes of the cluster
object TCEPMultiNodeConfig extends MultiNodeConfig {

  // 'role' does not mean member role, but cluster node
  val client = role("client")
  val publisher1 = role("publisher1")
  val publisher2 = role("publisher2")
  val node1 = role("node1")
  val node2 = role("node2")

  // enable the test transport that allows to do fancy things such as blackhole, throttle, etc.
  testTransport(on = false)

  // configuration for client
  nodeConfig(client)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Subscriber, Candidate]
    """.stripMargin))

  nodeConfig(publisher1)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Publisher, Candidate]
    """.stripMargin))

  nodeConfig(publisher2)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Publisher, Candidate]
    """.stripMargin))

  nodeConfig(node1)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Candidate]
    """.stripMargin
  ))

  nodeConfig(node2)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Candidate]
    """.stripMargin
  ))
  //
  //nodeConfig(node3)(ConfigFactory.parseString(
  //  """
  //    |akka.remote.netty.tcp.port=2505
  //    |akka.cluster.roles=[Candidate]
  //  """.stripMargin
  //))
  // common configuration for all nodes
  commonConfig(ConfigFactory.parseString(
    """
      |blocking-io-dispatcher = "akka.actor.default-blocking-io-dispatcher"
      |akka.loglevel=WARNING
      |akka.log-dead-letters = off
      |akka.log-dead-letters-during-shutdown = off
      |akka.actor.provider = cluster
      |clustering.cluster.name = tcep
      |akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      |akka.coordinated-shutdown.terminate-actor-system = off
      |akka.cluster.run-coordinated-shutdown-when-down = off
      |prio-dispatcher { mailbox-type = "tcep.akkamailbox.TCEPPriorityMailbox" }
      |prio-mailbox { mailbox-type = "tcep.akkamailbox.TCEPPriorityMailbox" }
      |akka.actor.deployment { /PrioActor { mailbox = prio-mailbox } }
      |akka.actor.serializers {
      |      jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
      |      jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
      |      kryo = "com.twitter.chill.akka.AkkaSerializer"
      |      java = "akka.serialization.JavaSerializer"
      |      proto = "akka.remote.serialization.ProtobufSerializer"
      |    }
      |akka.actor.serialization-bindings {
      |      #"tcep.machinenodes.helper.actors.MySerializable" = jackson-cbor
      |      #"tcep.data.Queries$Stream1" = proto
      |      "java.io.Serializable" = kryo
      |    }
      |akka.actor.allow-java-serialization = off
      |akka.actor.serialize-creators = off // always serialize Props
      |akka.actor.serialize-messages = off // always serialize messages
    """.stripMargin))
}
