package reactive


object Signalx {
  implicit def toVar[T](v: =>T): Var[T] = Var(v)
  implicit def toSignalx[T](s: Signal[T]) = new Signalx[T](s)
}

case class Signalx[T](s: Signal[T]) extends Observing {
  def changeIf[S](ev: EventStream[S]) = {
    val changed = Var(s.now)
    ev.foreach { b =>
      changed.update(s.now)
    }
    changed.change | s.change
  }
}