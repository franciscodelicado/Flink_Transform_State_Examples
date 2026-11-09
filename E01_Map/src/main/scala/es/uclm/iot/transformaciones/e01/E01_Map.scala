package es.uclm.iot.transformaciones.e01

import org.apache.flink.streaming.api.scala._

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** map: cada lectura en Celsius produce exactamente una en Fahrenheit. */
object E01_Map {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val lecturas: DataStream[Lectura] = env.fromElements(
      Lectura("s1", 21.5), Lectura("s2", 30.0), Lectura("s3", -4.0))

    val fahrenheit: DataStream[(String, Double)] =
      lecturas.map(l => (l.sensor, l.celsius * 9 / 5 + 32))

    fahrenheit.print()
    env.execute("E01 map")
  }
}
