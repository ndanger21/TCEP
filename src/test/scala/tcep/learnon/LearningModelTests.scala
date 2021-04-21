package tcep.learnon

import org.scalatest.FunSuite
import breeze.linalg._
import tcep.graph.transition.mapek.learnon.{LearningModel, Lightweight}

class LearningModelTests extends FunSuite{

  test("OLS computations #1") {
    val learnModel = new Lightweight()
    val X = DenseMatrix(
      (1.0,0.5),
      (1.0,1.5),
      (1.0,1.5),
      (1.0,2.5),
      (1.0,3.5),
      (1.0,4.0)
    )
    val y = DenseVector(1.5,0.5,1.5,2.0,2.5,2).t
    //val theta = learnModel.computeOLSParameters(X, y)
    //assert(theta == DenseVector(193.0/213.0,24.0/71.0), s"Computed theta is: ${theta.data}")
  }
}
