package reactive

import java.io.File

class FilePoller(path: Signal[String], delay: Long = 500) extends Trigger {

  private var previousLastModified = new File(path.now).lastModified()

  val timer = new Timer(0, delay, {t =>  false}) foreach { tick =>
    def newLastModified = new File(path.now).lastModified()
    if (newLastModified > previousLastModified || newLastModified == 0) {
      previousLastModified = newLastModified
      source.fire(())
    }
  }

}

