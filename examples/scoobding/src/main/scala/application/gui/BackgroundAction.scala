package gui

import reactive._
import swing.SwingWorker

class BackgroundAction[T](action: =>T) {

  def inBackground(implicit progress: EventSource[ActionInProgress] = new EventSource[ActionInProgress]()): EventStream[T] = {
    val result: EventSource[T] = new EventSource
    val worker = new SwingWorker {
      def act() {
        try {
          progress.fire(Started())
          result.fire(action)
        } finally { progress.fire(Finished()) }
      }
    }
    worker.start()
    result
  }
}

object BackgroundAction {
  implicit def actionToBackgroundAction[T](action: =>T) = new BackgroundAction(action)
}

sealed trait ActionInProgress
case class Started() extends ActionInProgress
case class Finished() extends ActionInProgress
