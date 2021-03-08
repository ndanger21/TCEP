package tcep.graph.transition.mapek.contrast

import org.cardygan.fm.Real

import scala.util.Random

/**
  * Created by Niels on 24.05.2018.
  */
object TestUtils {

  def generateMaxedContextData(cfm: CFM): Map[String, AnyVal] = {

    var res = Map[String, AnyVal]()
    val contextAttributes = cfm.getMandatoryAttributes
    for(a <- contextAttributes)
      a.getDomain match {
        case d: Real => res += (a.getName -> d.getBounds.getUb)
        case d: org.cardygan.fm.Int => res += (a.getName -> d.getBounds.getUb)
      }
    res
  }


  def generateMinContextData(cfm: CFM): Map[String, AnyVal] = {

    var res = Map[String, AnyVal]()
    val contextAttributes = cfm.getMandatoryAttributes
    for(a <- contextAttributes)
      a.getDomain match {
        case d: Real => res += (a.getName -> d.getBounds.getLb)
        case d: org.cardygan.fm.Int => res += (a.getName -> d.getBounds.getLb)
      }
    res
  }

  def generateRandomContextData(cfm: CFM): Map[String, AnyVal] = {

    var res = Map[String, AnyVal]()
    val contextAttributes = cfm.getMandatoryAttributes
    for(a <- contextAttributes)
      a.getDomain match {
        case d: Real => res += (a.getName -> getRandomValue(d.getBounds.getLb.toInt, d.getBounds.getUb.toInt))
        case d: org.cardygan.fm.Int => res += (a.getName -> getRandomValue(d.getBounds.getLb, d.getBounds.getUb).round.toInt)
      }

    def getRandomValue(lb: Int, ub: Int): Double = (ub - lb) * Random.nextDouble() + lb

    res
  }
}
