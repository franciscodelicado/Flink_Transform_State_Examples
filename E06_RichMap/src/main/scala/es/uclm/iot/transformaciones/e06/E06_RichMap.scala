package es.uclm.iot.transformaciones.e06

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** RichMapFunction con estado: numera las lecturas de cada sensor y calcula su media acumulada. */
class NumerarYMedia extends RichMapFunction[Lectura, String] {
  // (número de lecturas, suma de temperaturas) de la clave actual
  private var acumulado: ValueState[(Long, Double)] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[(Long, Double)]("acumulado", createTypeInformation[(Long, Double)])
    acumulado = getRuntimeContext.getState(descriptor)
  }

  override def map(l: Lectura): String = {
    val (n, suma) = Option(acumulado.value()).getOrElse((0L, 0.0))   // null la primera vez
    val nuevo = (n + 1, suma + l.celsius)
    acumulado.update(nuevo)
    f"${l.sensor} lectura #${nuevo._1}: ${l.celsius}%.1f C, media ${nuevo._2 / nuevo._1}%.2f C"
  }
}

object E06_RichMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(Lectura("s1", 20.0), Lectura("s2", 25.0), Lectura("s1", 22.0),
                     Lectura("s1", 24.0), Lectura("s2", 27.0))
      .keyBy(_.sensor)              // el estado es por clave: hace falta un KeyedStream
      .map(new NumerarYMedia)
      .print()

    env.execute("E06 RichMapFunction")
  }
}
