package gui

import swing.UIElement
import java.awt.{Cursor, Point, Toolkit}
import Images._
import reactive.{Observing, EventStream}

case class WaitCursor(parent: UIElement)(implicit actionInProgress: EventStream[ActionInProgress]) extends Observing {
  private lazy val waitCursor = Toolkit.getDefaultToolkit.createCustomCursor(getImage("clock-icon.png"), new Point(0,0), "cursor")

  actionInProgress.foreach { action => action match {
      case Started()  => parent.cursor = waitCursor
      case Finished() => parent.cursor = new Cursor(Cursor.DEFAULT_CURSOR)
    }
  }
}