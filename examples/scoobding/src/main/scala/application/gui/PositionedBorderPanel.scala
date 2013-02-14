/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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