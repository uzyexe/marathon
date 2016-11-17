package mesosphere.marathon
package raml

/**
  * Helpers for quickly constructing port definitions
  */
object PortDefinitions {

  def apply(ports: Int*): Seq[PortDefinition] =
    ports.map(p => PortDefinition(port = Some(p)))(collection.breakOut)
}
