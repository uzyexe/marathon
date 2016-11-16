package mesosphere.marathon
package raml

import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.launcher.OfferMatchResult
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfoWithStatistics
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp }
import mesosphere.marathon.test.{ MarathonSpec, MarathonTestHelper }
import mesosphere.mesos.NoOfferMatchReason
import mesosphere.mesos.protos.{ ScalarResource, TextAttribute }
import org.apache.mesos.{ Protos => Mesos }

class QueueInfoConversionTest extends MarathonSpec {

  test("A reject reason is converted correctly") {
    Given("A reject reason")
    val reason = NoOfferMatchReason.InsufficientCpus

    When("The value is converted to raml")
    val raml = reason.toRaml[String]

    Then("The value is converted correctly")
    raml should be (reason.toString)
  }

  test("A scala value is converted correctly") {
    Given("A scalar value")
    val scalar = Mesos.Value.Scalar.newBuilder().setValue(123L).build()

    When("The value is converted to raml")
    val raml = scalar.toRaml[Option[Double]]

    Then("The value is converted correctly")
    raml should be (defined)
    raml should be (Some(123L))
  }

  test("A Range value is converted correctly") {
    Given("A range value")
    val range = Mesos.Value.Range.newBuilder().setBegin(0).setEnd(5).build()

    When("The value is converted to raml")
    val raml = range.toRaml[NumberRange]

    Then("The value is converted correctly")
    raml should be (NumberRange(0L, 5L))
  }

  test("An OfferResource is converted correctly") {
    Given("An offer resource")
    import mesosphere.mesos.protos.Implicits._
    val resource: Mesos.Resource = ScalarResource("cpus", 34L)

    When("The value is converted to raml")
    val raml = resource.toRaml[OfferResource]

    Then("The value is converted correctly")
    raml.name should be ("cpus")
    raml.role should be ("*")
    raml.scalar should be (Some(34L))
    raml.ranges should be (empty)
    raml.set should be (empty)
  }

  test("An Offer Attribute is converted correctly") {
    Given("An offer attribute")
    import mesosphere.mesos.protos.Implicits._
    val attribute: Mesos.Attribute = TextAttribute("key", "value")

    When("The value is converted to raml")
    val raml = attribute.toRaml[AgentAttribute]

    Then("The value is converted correctly")
    raml.name should be ("key")
    raml.scalar should be (empty)
    raml.text should be (Some("value"))
    raml.ranges should be (empty)
    raml.set should be (empty)
  }

  test("An Offer is converted correctly") {
    Given("An offer")
    val offer = MarathonTestHelper.makeBasicOffer().build()

    When("The value is converted to raml")
    val raml = offer.toRaml[Offer]

    Then("The value is converted correctly")
    raml.agentId should be (offer.getSlaveId.getValue)
    raml.attributes should have size 0
    raml.hostname should be (offer.getHostname)
    raml.id should be (offer.getId.getValue)
    raml.resources should have size 5
  }

  test("A NoMatch is converted correctly") {
    Given("A NoMatch")
    val app = AppDefinition(PathId("/test"))
    val offer = MarathonTestHelper.makeBasicOffer().build()
    val noMatch = OfferMatchResult.NoMatch(app, offer, Seq(NoOfferMatchReason.InsufficientCpus), Timestamp.now())

    When("The value is converted to raml")
    val raml = noMatch.toRaml[UnusedOffer]

    Then("The value is converted correctly")
    raml.offer should be (offer.toRaml[Offer])
    raml.reason should be(noMatch.reasons.toRaml[Seq[String]])
    raml.timestamp should be (noMatch.timestamp.toOffsetDateTime)
  }

  test("A QueueInfoWithStatistics is converted correctly") {
    Given("A QueueInfoWithStatistics")
    val clock = ConstantClock()
    val now = clock.now()
    val app = AppDefinition(PathId("/test"))
    val offer = MarathonTestHelper.makeBasicOffer().build()
    val noMatch = OfferMatchResult.NoMatch(app, offer, Seq(NoOfferMatchReason.InsufficientCpus), now)
    val summary: Map[NoOfferMatchReason, Int] = Map(NoOfferMatchReason.InsufficientCpus -> 100)
    val info = QueuedInstanceInfoWithStatistics(app, inProgress = true,
      instancesLeftToLaunch = 23,
      finalInstanceCount = 23,
      unreachableInstances = 12,
      backOffUntil = now,
      startedAt = now,
      rejectSummary = summary,
      processedOfferCount = 123,
      unusedOfferCount = 123,
      lastMatch = None,
      lastNoMatch = Some(noMatch),
      lastNoMatches = Seq(noMatch))

    When("The value is converted to raml")
    val raml = (Seq(info), true, clock).toRaml[Queue]

    Then("The value is converted correctly")
    raml.queue should have size 1
    raml.queue.head shouldBe a[QueueApp]
    val item = raml.queue.head.asInstanceOf[QueueApp]
    item.app.id should be (app.id.toString)
    item.count should be(23)
    item.processedOffersSummary.processedOffersCount should be(info.processedOfferCount)
    item.processedOffersSummary.unusedOffersCount should be(info.unusedOfferCount)
    item.processedOffersSummary.lastUnusedOfferAt should be(Some(now.toOffsetDateTime))
    item.processedOffersSummary.lastUsedOfferAt should be(None)
    item.processedOffersSummary.rejectReason should be(summary.toRaml[Map[String, Int]])
    item.lastUnusedOffers should be (defined)
    item.since should be(now.toOffsetDateTime)
  }
}
