package reactive

/**
 * This event source triggers empty values just to say that "something" happens
 */
trait Trigger extends EventStreamSourceProxy[Unit] {
  def trigger = source.fire(())
}