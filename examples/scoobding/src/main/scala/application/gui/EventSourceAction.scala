package gui

import swing._
import reactive.Trigger

abstract class EventSourceAction(title: String) extends Action(title) with Trigger {
  def apply { trigger }
}