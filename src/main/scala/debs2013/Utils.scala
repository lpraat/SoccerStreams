package debs2013

object Utils {

  def getHourMinuteSeconds(seconds: Double): String = {
    val justSeconds: Int = seconds.toInt
    f"${justSeconds / 3600}%02d:${(justSeconds % 3600) / 60}%02d:${justSeconds % 60}%02d:${(seconds - justSeconds)*1e3}%.0f"
  }

  def isGameInterruption(id: Long): Boolean = {
    id == 0 || id == 1
  }

  def isBall(id: Long): Boolean = {
    id == 4 || id == 8 || id == 10 || id == 12
  }

  def isInTheField(x: Float, y: Float): Boolean = {
    x > 0 && x < 52.483 && y > -33.960 && y < 33.965
  }

  def distance(x1: Float, y1: Float, z1: Float, x2: Float, y2: Float, z2: Float): Double = {
    Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2) + Math.pow(z1 - z2, 2))
  }


  case class Area(xMin: Float, xMax: Float, yMin: Float, yMax: Float, zMax: Float)

  val deltaY: Float = 2.0f/3 * 2.440f
  val deltaZ: Float = 2.440f


  val deltaX1: Float = (29.8985f - 22.5785f) / 2.0f

  val Area1: Area = Area(22.5785f - deltaX1, 29.8985f + deltaX1, 33.941f, 33.941f + deltaY, 2.440f + deltaZ)
  def inGoalAreaOfTeam1(x: Float, y: Float, z: Float): Boolean = {
    x >= Area1.xMin && x <= Area1.xMax && y >= Area1.yMin && y <= Area1.yMax && z < Area1.zMax
  }

  val deltaX2: Float = (29.880f - 22.560f) / 2.0f
  val Area2: Area = Area(22.560f - deltaX2, 29.880f + deltaX2, -33.968f - deltaY, -33.968f, 2.440f + deltaZ)
  def inGoalAreaOfTeam2(x: Float, y: Float, z: Float): Boolean = {
    x >= Area2.xMin && x <= Area2.xMax && y >= Area2.yMin && y <= Area2.yMax && z < Area2.zMax
  }


}
