
import better.files._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

case class G9Data(
                   idKey: String,
                   contentId: String,
                   epgId: String,
                   startDate: DateTime,
                   endDate: DateTime
                 )

case class G9Gap(idKeyStart: String, idKeyEnd: String)

object Main extends App {

  implicit def dateTimeOrdering: Ordering[G9Data] = Ordering.fromLessThan(_.startDate isBefore _.startDate)

  val dtFormatTimeZoneDate: DateTimeFormatter = ISODateTimeFormat.dateTime()
  val now = DateTime.now()

  val start = now.minusDays(1).withHourOfDay(6).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  val end = now.plusDays(8).withHourOfDay(6).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

  println(s"coucou Coco & Cocotte")

  //val fileToOpen = File("/Users/mchaouchi/workspace/utils/scripts/reload/broadcasts_a_sorted")
  val fileToOpen = File("/tmp/broadcast.txt")
  val data = fileToOpen.lineIterator

  /*
  val fileEpgIds = File("")
  val epgIds = Json.parse(fileEpgIds.contentAsString).as[Seq[JsString]].map(_.toString())
  */

  val mapEpgIdData: Map[String, Seq[G9Data]] = data.map{ line =>
    val infos = line.split(";")
    G9Data(infos.head, infos(1), infos(2), dtFormatTimeZoneDate.parseDateTime(infos(3)), dtFormatTimeZoneDate.parseDateTime(infos.last))
  }.toSeq.groupBy(_.epgId).mapValues(d => d.sorted)

  var nbTrous = 0

  val listeEpgIdsTrou = mapEpgIdData.flatMap{ case (epgId, g9datas) =>
    val sizeOfG9Datas = g9datas.size

    var x = 0
    val etatNbTrous = nbTrous
    while (x < (sizeOfG9Datas - 1)) {
      val g9Data1 = g9datas(x)
      val g9Data2 = g9datas(x + 1)

      if(g9Data1.endDate != g9Data2.startDate){
        println(s"Trou dans la grille sur l'epgId $epgId entre ${g9Data1.idKey} (${g9Data1.endDate}) & ${g9Data2.idKey} (${g9Data2.startDate})")
        nbTrous += 1
      }
      x += 1
    }

    if(etatNbTrous != nbTrous) Some(epgId)
    else None
  }.toSeq.distinct

  println(s"nombre de trous: $nbTrous ## liste de chaines avec des trous: $listeEpgIdsTrou")

}
