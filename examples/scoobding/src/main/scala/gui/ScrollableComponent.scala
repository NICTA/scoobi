package gui

import swing.{Component, ScrollPane}

object ScrollableComponent {
  implicit def toScrollable(component: Component) = new ToScrollable(component)
  class ToScrollable(component: Component) {
    def scrollable = new ScrollPane(component)
  }
}