package mesosphere.marathon
package raml

import mesosphere.marathon.test.MarathonSpec

class ReadinessConversionsTest extends MarathonSpec {

  test("A readiness check can be converted") {
    Given("A readiness check")
    val check = core.readiness.ReadinessCheck()

    When("The check is converted")
    val raml = check.toRaml[ReadinessCheck]

    Then("The check is converted correctly")
    raml.httpStatusCodesForReady should contain theSameElementsInOrderAs check.httpStatusCodesForReady
    raml.intervalSeconds should be(check.interval.toSeconds)
    raml.name should be(check.name)
    raml.path should be(check.path)
    raml.portName should be(check.portName)
    raml.preserveLastResponse should be(Some(check.preserveLastResponse))
    raml.timeoutSeconds should be(check.timeout.toSeconds)
  }
}
