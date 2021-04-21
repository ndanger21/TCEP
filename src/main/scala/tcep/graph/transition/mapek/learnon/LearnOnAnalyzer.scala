package tcep.graph.transition.mapek.learnon

import tcep.graph.transition.mapek.contrast.{ContrastAnalyzer}

import scala.concurrent.duration._

class LearnOnAnalyzer(mapek: LearnOnMAPEK) extends ContrastAnalyzer(mapek, 30 seconds, 30 seconds) {
  /**
    * identical to Contrast implementation, except for delay and interval (3m, 2m)
    */
}
