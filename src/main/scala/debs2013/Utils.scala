package debs2013

object Utils {

  def isGameInterruption(id: Long): Boolean = {
    id == 0 || id == 1
  }

  def isBall(id: Long): Boolean = {
    id == 4 || id == 8 || id == 10 || id == 12
  }

  def isInTheField(x: Float, y: Float): Boolean = {
    x > 0 && x < 52483 && y > -33960 && y < 33965
  }

  def distance(x1: Float, y1: Float, z1: Float, x2: Float, y2: Float, z2: Float): Double = {
    Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2) + Math.pow(z1 - z2, 2))
  }
}
