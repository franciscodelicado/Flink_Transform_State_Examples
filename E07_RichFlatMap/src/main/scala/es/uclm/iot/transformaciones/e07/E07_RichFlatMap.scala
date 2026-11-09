package es.uclm.iot.transformaciones.e07

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** Emite un aviso solo cuando la temperatura de un sensor cambia más de `salto` grados respecto a su lectura anterior. */
class SaltoBrusco(salto: Double) extends RichFlatMapFunction[Lectura, String] {
  private var anterior: ValueState[java.lang.Double] = _

  override def open(parameters: Configuration): Unit =
    anterior = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Double]("anterior", classOf[java.lang.Double]))

  override def flatMap(l: Lectura, out: Collector[String]): Unit = {
    val previa = anterior.value()                     // null si es la primera lectura de este sensor
    if (previa != null && math.abs(l.celsius - previa) > salto)
      out.collect(f"SALTO en ${l.sensor}: de $previa%.1f a ${l.celsius}%.1f C")
    anterior.update(l.celsius)
  }
}

object E07_RichFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(Lectura("s1", 21.0), Lectura("s2", 22.0), Lectura("s1", 22.5),
                     Lectura("s2", 29.5), Lectura("s1", 21.0), Lectura("s1", 14.0))
      .keyBy(_.sensor)
      .flatMap(new SaltoBrusco(5.0))
      .print()

    env.execute("E07 RichFlatMapFunction")
  }
}
