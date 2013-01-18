import com.nicta.scoobi.Scoobi._
import app.Histogram._
import scalaz.Scalaz._

object Tweets extends ScoobiApp {

  def run {
    fromTextFile("tweets.txt").
      map(t => (t.size, 1)).
      groupByKey.
      combine((_:Int)+(_:Int)).
      run.toHistogram("Tweet length", "Tweets number")
  }

}

