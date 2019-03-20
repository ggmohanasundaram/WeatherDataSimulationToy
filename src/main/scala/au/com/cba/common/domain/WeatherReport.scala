package au.com.cba.common.domain

case class WeatherReport(number: Int,location: Location,localTime: String,condition:String, temperature: Double ,pressure: Double, humidity: Double){

  override def toString = {
    "%d|%s|%s|%s|%+.2f|%.2f|%.2f".format(number, location.toString, localTime, condition , temperature , pressure , humidity )
  }
}