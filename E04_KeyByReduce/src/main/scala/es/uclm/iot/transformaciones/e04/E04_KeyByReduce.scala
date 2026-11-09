package es.uclm.iot.transformaciones.e04

import org.apache.flink.streaming.api.scala._

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** Consumo eléctrico (kWh) que envía un contador cada cierto tiempo. */
case class Consumo(contador: String, kwh: Double)

/** keyBy + reduce: máximo acumulado por sensor y consumo total por contador. */
object E04_KeyByReduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1) Temperatura máxima vista hasta ahora en cada sensor
    val lecturas = env.fromElements(
      Lectura("s1", 20.0), Lectura("s2", 25.0), Lectura("s1", 23.0),
      Lectura("s2", 21.0), Lectura("s1", 19.0))

    val maximos: DataStream[Lectura] = lecturas
      .keyBy(_.sensor)
      .reduce((acum, nueva) => if (nueva.celsius > acum.celsius) nueva else acum)
    maximos.print("MAX")

    // 2) Consumo acumulado de cada contador
    val consumos = env.fromElements(
      Consumo("c1", 1.5), Consumo("c2", 0.4), Consumo("c1", 2.0), Consumo("c1", 0.5), Consumo("c2", 1.1))

    val totales: DataStream[Consumo] = consumos
      .keyBy(_.contador)
      .reduce((a, b) => Consumo(a.contador, a.kwh + b.kwh))
    totales.print("TOTAL")

    env.execute("E04 keyBy + reduce")
  }
}
