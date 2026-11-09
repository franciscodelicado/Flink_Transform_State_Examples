package es.uclm.iot.transformaciones.e02

import org.apache.flink.streaming.api.scala._
import scala.util.Try

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** flatMap: un mensaje con varias lecturas produce 0, 1 o varias lecturas. */
object E02_FlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val mensajes: DataStream[String] = env.fromElements(
      "s1:21.5;s2:30.1", "s3:22.0", "s4:xx;s5:19.5", "")

    val lecturas: DataStream[Lectura] = mensajes.flatMap { msg =>
      msg.split(";").toSeq.flatMap { parte =>
        parte.split(":") match {
          case Array(s, t) => Try(Lectura(s, t.toDouble)).toOption   // None si t no es un número
          case _           => None                                    // trozo mal formado o vacío
        }
      }
    }

    lecturas.print()
    env.execute("E02 flatMap")
  }
}
