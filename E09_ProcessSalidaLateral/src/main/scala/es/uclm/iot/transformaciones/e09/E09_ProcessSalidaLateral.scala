package es.uclm.iot.transformaciones.e09

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/**
 * KeyedProcessFunction con estado y dos salidas laterales:
 *  - lecturas fuera del rango físico del sensor (-40..85 C) -> salida "errores"
 *  - tres errores seguidos del mismo sensor                 -> salida "averias"
 * Las lecturas válidas siguen por la salida principal.
 */
class ValidarLecturas extends KeyedProcessFunction[String, Lectura, Lectura] {
  private var erroresSeguidos: ValueState[java.lang.Integer] = _

  override def open(p: Configuration): Unit =
    erroresSeguidos = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Integer]("erroresSeguidos", classOf[java.lang.Integer]))

  override def processElement(l: Lectura,
                              ctx: KeyedProcessFunction[String, Lectura, Lectura]#Context,
                              out: Collector[Lectura]): Unit = {
    if (l.celsius >= -40 && l.celsius <= 85) {
      erroresSeguidos.clear()                                      // vuelve a 0
      out.collect(l)                                               // salida principal
    } else {
      val n = Option(erroresSeguidos.value()).map(_.intValue).getOrElse(0) + 1
      erroresSeguidos.update(n)
      ctx.output(ValidarLecturas.Errores, s"${ctx.getCurrentKey}: valor imposible ${l.celsius}")
      if (n == 3) ctx.output(ValidarLecturas.Averias, s"${ctx.getCurrentKey}: 3 errores seguidos, revisar sensor")
    }
  }
}

object ValidarLecturas {
  val Errores = OutputTag[String]("errores")
  val Averias = OutputTag[String]("averias")
}

object E09_ProcessSalidaLateral {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val validas: DataStream[Lectura] = env
      .fromElements(Lectura("s1", 21.0), Lectura("s2", 999.0), Lectura("s1", 22.0), Lectura("s2", 999.0),
                    Lectura("s2", -273.0), Lectura("s2", 23.5))
      .keyBy(_.sensor)
      .process(new ValidarLecturas)

    validas.print("VALIDA")
    validas.getSideOutput(ValidarLecturas.Errores).print("ERROR ")
    validas.getSideOutput(ValidarLecturas.Averias).print("AVERIA")

    env.execute("E09 process + salidas laterales")
  }
}
