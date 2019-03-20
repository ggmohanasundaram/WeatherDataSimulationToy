package au.com.cba.common.domain

case class Location(name: String, latitude: Double, longitude: Double, elevation: Double) {
  override def toString = s"${name}|%.2f,%.2f,%.2f".format(latitude, longitude, elevation)
}
