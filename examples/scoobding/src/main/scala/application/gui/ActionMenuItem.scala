package gui

import swing.MenuItem
import reactive._
import java.awt.Component

case class ActionMenuItem(a: EventSourceAction) extends MenuItem(a.title) with Trigger {
  override def self: Component with EventStream[Unit] = this.asInstanceOf[Component with EventStream[Unit]]

  action = a
  a foreach { doIt => trigger }
}