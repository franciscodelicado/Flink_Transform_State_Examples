package es.uclm.iot.transformaciones.e03

import org.apache.flink.streaming.api.scala._

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** filter: solo pasan las lecturas que cumplen la condición. */
object E03_Filter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val lecturas = env.fromElements(
      Lectura("s1", 21.5), Lectura("s2", 30.1), Lectura("s3", 22.0), Lectura("s1", 31.4))

    val calientes: DataStream[Lectura] = lecturas.filter(l => l.celsius > 25.0)

    calientes.print()
    env.execute("E03 filter")
  }
}
