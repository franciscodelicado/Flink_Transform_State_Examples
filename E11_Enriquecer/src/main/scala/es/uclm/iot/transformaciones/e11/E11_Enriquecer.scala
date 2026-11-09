package es.uclm.iot.transformaciones.e11

import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/** Lectura de un sensor de temperatura (grados Celsius). */
case class Lectura(sensor: String, celsius: Double)

/** Alta de un sensor: dónde está instalado. */
case class Ubicacion(sensor: String, sala: String)

/**
 * Enriquecer lecturas con la ubicación del sensor, que llega por otro flujo.
 * Si una lectura llega antes que la ubicación, se guarda en estado y se emite al llegar esta.
 */
class EnriquecerConUbicacion extends KeyedCoProcessFunction[String, Lectura, Ubicacion, String] {
  private var sala: ValueState[String] = _
  private var pendientes: ListState[Lectura] = _

  override def open(p: Configuration): Unit = {
    sala = getRuntimeContext.getState(new ValueStateDescriptor[String]("sala", classOf[String]))
    pendientes = getRuntimeContext.getListState(new ListStateDescriptor[Lectura]("pendientes", createTypeInformation[Lectura]))
  }

  override def processElement1(l: Lectura, ctx: KeyedCoProcessFunction[String, Lectura, Ubicacion, String]#Context,
                               out: Collector[String]): Unit =
    Option(sala.value()) match {
      case Some(s) => out.collect(f"${l.sensor} (${s}): ${l.celsius}%.1f C")
      case None    => pendientes.add(l)                                   // aún no sabemos dónde está
    }

  override def processElement2(u: Ubicacion, ctx: KeyedCoProcessFunction[String, Lectura, Ubicacion, String]#Context,
                               out: Collector[String]): Unit = {
    sala.update(u.sala)
    for (l <- pendientes.get().asScala) out.collect(f"${l.sensor} (${u.sala}): ${l.celsius}%.1f C   [retenida]")
    pendientes.clear()
  }
}

object E11_Enriquecer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val lecturas = env.addSource(new FuenteConPausas(Seq(
      0L -> Lectura("s1", 21.0), 100L -> Lectura("s2", 23.0), 600L -> Lectura("s2", 23.4), 900L -> Lectura("s1", 21.2))))
    val altas = env.addSource(new FuenteConPausas(Seq(300L -> Ubicacion("s2", "aula 1.05"), 700L -> Ubicacion("s1", "laboratorio"))))

    lecturas.connect(altas)
      .keyBy(_.sensor, _.sensor)
      .process(new EnriquecerConUbicacion)
      .print()

    env.execute("E11 connect + CoProcessFunction")
  }
}
