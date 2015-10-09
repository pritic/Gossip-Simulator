
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.math._
import scala.util.Random

/**
 * Created by PRITI on 10/7/15.
 */
/**
 *
 */

object Project2Bonus {

  //  val MasterInvalidNodeMap = new ArrayBuffer[ActorRef] with mutable
  //  .SynchronizedBuffer[ActorRef]

  def main(args: Array[String]) {

    var numNodes: Int = args(0).toInt
    val topology: String = args(1)
    val algorithm: String = args(2)
    val percentFailure: Double = args(3).toDouble

    if (topology.equals("3D") || topology.equals("imp3D")) {
      numNodes = ceil(cbrt(numNodes)).toInt
      numNodes = numNodes * numNodes * numNodes
      println("Number of nodes: " + numNodes)
    }

    val failureCount = percentFailure / 100.0 * numNodes

    val system = ActorSystem("GossipSimulator")
    val MasterBonus: ActorRef = system.actorOf(Props(new MasterBonus
    (numNodes, topology, algorithm, ceil(failureCount).toInt)), name =
      "MasterBonus")

    MasterBonus ! STARTBonus
  }

  /**
   *
   * @param numNodes : Number of nodes in the network
   * @param topology : Type of network (full, line, 3D, imp3D)
   * @param algorithm : Type of algorithm (gossip, push-sum)
   * @param failureCount : Number of nodes failing
   */
  class MasterBonus(
                     numNodes: Int, topology: String, algorithm: String,
                     failureCount: Int) extends Actor {

    var NodeActors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]()
    var NodeActorsCompleted: ArrayBuffer[ActorRef] = new
        ArrayBuffer[ActorRef]()
    var counter: Int = 0
    var failureList: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    for (i <- 0 until numNodes) {
      NodeActors += context.actorOf(Props(new GossipActorBonus(i, self)),
        name = "NodeActor" + i)
    }

    var k: Int = 0
    var m: Int = 0

    while (k < failureCount) {
      m = Random.nextInt(NodeActors.length)
      failureList += m
      k += 1
    }
    //println(failureList)
    var Neighbors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]()

    if (topology.equals("full")) {
      for (i <- 0 until NodeActors.length) {
        for (j <- 0 until NodeActors.length)
          if (i != j)
            Neighbors += NodeActors(j)

        NodeActors(i) ! SetNeighborListBonus(Neighbors)
        NodeActors(i) ! SetFailureList(failureList)
        Neighbors = new ArrayBuffer[ActorRef]()
      }
    }

    if (topology.equals("line")) {
      Neighbors += NodeActors(1)
      NodeActors(0) ! SetNeighborListBonus(Neighbors)

      Neighbors = new ArrayBuffer[ActorRef]()

      Neighbors += NodeActors(NodeActors.length - 2)
      NodeActors(NodeActors.length - 1) ! SetNeighborListBonus(Neighbors)
      Neighbors = new ArrayBuffer[ActorRef]()

      for (i <- 1 until NodeActors.length - 1) {
        Neighbors +=(NodeActors(i - 1), NodeActors(i + 1))
        NodeActors(i) ! SetNeighborListBonus(Neighbors)
        NodeActors(i) ! SetFailureList(failureList)
        Neighbors = new ArrayBuffer[ActorRef]()
      }
    }

    if (topology.equals("3D")) {
      val sideIndex: Int = cbrt(NodeActors.length).toInt

      val nodeArray = Array.ofDim[ActorRef](sideIndex, sideIndex, sideIndex)
      var len: Int = 0

      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            nodeArray(i)(j)(k) = NodeActors(len)
            len += 1
          }

      len = 0
      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            //top,bottom,left,right,front,back

            if (!(i - 1 < 0))
              Neighbors += nodeArray(i - 1)(j)(k)

            if (!(i + 1 >= sideIndex))
              Neighbors += nodeArray(i + 1)(j)(k)

            if (!(j - 1 < 0))
              Neighbors += nodeArray(i)(j - 1)(k)

            if (!(j + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j + 1)(k)

            if (!(k - 1 < 0))
              Neighbors += nodeArray(i)(j)(k - 1)

            if (!(k + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j)(k + 1)


            //          println(Neighbors)
            NodeActors(len) ! SetNeighborListBonus(Neighbors)
            NodeActors(len) ! SetFailureList(failureList)
            len += 1
            Neighbors = new ArrayBuffer[ActorRef]()
          }
    }

    if (topology.equals("imp3D")) {
      val sideIndex: Int = cbrt(NodeActors.length).toInt

      val nodeArray = Array.ofDim[ActorRef](sideIndex, sideIndex, sideIndex)
      var len: Int = 0

      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            nodeArray(i)(j)(k) = NodeActors(len)
            len += 1
          }

      len = 0
      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            //top,bottom,left,right,front,back

            if (!(i - 1 < 0))
              Neighbors += nodeArray(i - 1)(j)(k)

            if (!(i + 1 >= sideIndex))
              Neighbors += nodeArray(i + 1)(j)(k)

            if (!(j - 1 < 0))
              Neighbors += nodeArray(i)(j - 1)(k)

            if (!(j + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j + 1)(k)

            if (!(k - 1 < 0))
              Neighbors += nodeArray(i)(j)(k - 1)

            if (!(k + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j)(k + 1)

            var m: Int = Random.nextInt(NodeActors.length)

            while (Neighbors.contains(NodeActors(m)) || m == len) {
              m = Random.nextInt(NodeActors.length)
            }
            Neighbors += NodeActors(m)
            NodeActors(len) ! SetNeighborListBonus(Neighbors)
            NodeActors(len) ! SetFailureList(failureList)
            len += 1
            Neighbors = new ArrayBuffer[ActorRef]()
          }
    }
    val b = System.currentTimeMillis;

    override def receive: Receive = {

      case STARTBonus => {
        if (algorithm == "gossip") {
          var i: Int = Random.nextInt(NodeActors.length)
          while (failureList.contains(i))
            i = Random.nextInt(NodeActors.length)
          NodeActors(i) ! KeepGossipingBonus
          println("Gossip started")
        }
        else {
          val i: Int = Random.nextInt(NodeActors.length)
          NodeActors(i) ! StartPushSumBonus(0, 1)
          println("Push-Sum started")
        }
      }

      case KillNodeBonus =>
        counter += 1
        if (counter == NodeActors.length - failureList.length) {
          println("Time in milliseconds: " + (System.currentTimeMillis - b))
          context.system.shutdown()
        }

      case KillItBonus =>
        println("Time in milliseconds: " + (System.currentTimeMillis - b))
        context.system.shutdown()
    }
  }

  /**
   *
   * @param id : Unique ID for each Actor
   * @param MasterBonus : Master Actor
   */
  class GossipActorBonus(id: Int, MasterBonus: ActorRef) extends Actor {

    var neighborList = new ArrayBuffer[ActorRef]()
    val ID: Int = id
    //    var disqualifiedNeighbors = new ArrayBuffer[Int]()
    var rumorCount: Int = 0
    var maxRumorCount = 10
    var s: Double = ID
    var w: Double = 0.0
    var count: Int = 0
    var naiveCounter: Int = 0
    var previousSW: Double = s / w
    var currentSW: Double = s / w
    var failureList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    //    var flag: Int = 0
    var done: Boolean = false
    var calledAtLeastOnce: Boolean = false

    override def receive: Receive = {

      case SetNeighborListBonus(neighborList: ArrayBuffer[ActorRef]) =>
        this.neighborList = neighborList

      case SetFailureList(failureList: ArrayBuffer[Int]) =>
        this.failureList = failureList

      case KeepGossipingBonus =>
        if (rumorCount < maxRumorCount && !done && (neighborList.length !=
          0) && !(failureList.contains(ID))) {
          rumorCount += 1
          val i: Int = Random.nextInt(neighborList.length)
          neighborList(i) ! KeepGossipingBonus
          context.system.scheduler.scheduleOnce(10 milliseconds, self,
            KeepGossipingBonus)
        }
        else if (!calledAtLeastOnce) {

          done = true
          calledAtLeastOnce = true

          if (neighborList.length != 0)
            for (eachActor <- neighborList)
              eachActor ! Remove

          if (!(failureList.contains(ID))) {
            //              println("killing: " + ID)
            MasterBonus ! KillNodeBonus
          }

        }
      case RemoveBonus =>
        neighborList = neighborList - sender
        if (neighborList.length <= 0) {
          done = true
        }

      case StartPushSumBonus(s1: Double, w1: Double) =>

        naiveCounter += 1
        previousSW = s / w

        s += s1
        w += w1

        currentSW = s / w

        s = s / 2
        w = w / 2

        if (naiveCounter == 1 || Math.abs(currentSW - previousSW) > math.pow
        (10, -10)) {
          count = 0
          val i = Random.nextInt(neighborList.length);
          neighborList(i) ! StartPushSumBonus(s, w)
        }
        else {
          count += 1
          if (count >= 3) {
            MasterBonus ! KillItBonus
          }
          else {
            val i = Random.nextInt(neighborList.length);
            neighborList(i) ! StartPushSumBonus(s, w)
          }
        }
    }
  }

}

case class STARTBonus()

case class KillNodeBonus()

case class KillItBonus()

case class SetNeighborListBonus(neighborList: ArrayBuffer[ActorRef])

case class KeepGossipingBonus()

case class StartPushSumBonus(s: Double, w: Double)

case class SetFailureList(failureList: ArrayBuffer[Int])

case class RemoveBonus()

