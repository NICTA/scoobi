package gui

import swing.{Component, BorderPanel}

case class PositionedBorderPanel(center: Component = NoComponent,
                                 north:  Component = NoComponent,
                                 south:  Component = NoComponent,
                                 east:   Component = NoComponent,
                                 west:   Component = NoComponent) extends BorderPanel {
  import BorderPanel._

  if (center != NoComponent) layout(center) = Position.Center
  if (north  != NoComponent) layout(north)  = Position.North
  if (south  != NoComponent) layout(south)  = Position.South
  if (east   != NoComponent) layout(east)   = Position.East
  if (west   != NoComponent) layout(west)   = Position.West
}

object NoComponent extends Component