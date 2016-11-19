package mesosphere.marathon
package api.v2.json

import mesosphere.marathon.state.{ AppDefinition, Group, PathId, Timestamp }
import com.wix.accord.validate
import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }
import PathId._
import mesosphere.marathon.api.v2.AppNormalization
import mesosphere.marathon.raml.{ App, GroupConversion, GroupUpdate, Raml }

class GroupUpdateTest extends FunSuite with Matchers with GivenWhenThen {

  val noEnabledFeatures = Set.empty[String]
  val groupConversionContext: GroupConversion.Context = new GroupConversion.Context {
    override def preprocess(app: App): AppDefinition = {
      // assume canonical form and that the app is valid
      Raml.fromRaml(AppNormalization.apply(app, AppNormalization.Config(None)))
    }
  }

  test("A group update can be applied to an empty group") {
    Given("An empty group with updates")
    val group = Group.empty
    val update = GroupUpdate(
      Some(PathId.empty.toString),
      Some(Set.empty[App]),
      Some(Set(
        GroupUpdate(
          Some("test"), Some(Set.empty[App]), Some(Set(GroupUpdateHelper.empty("foo".toPath)))),
        GroupUpdate(
          Some("apps"), Some(Set(
            App("app1", cmd = Some("foo"),
              dependencies = Set("d1", "../test/foo", "/test")))))
      ))
    )
    val timestamp = Timestamp.now()

    When("The update is performed")
    val result = Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, group, timestamp), groupConversionContext))

    validate(result)(Group.validRootGroup(maxApps = None, noEnabledFeatures)).isSuccess should be(true)

    Then("The update is applied correctly")
    result.id should be(PathId.empty)
    result.groups should have size 2
    val test = result.group("test".toRootPath)
    test should be('defined)
    test.get.groups should have size 1
    val apps = result.group("apps".toRootPath)
    apps should be('defined)
    apps.get.apps should have size 1
    val app = apps.get.apps.head
    app._1.toString should be ("/apps/app1")
    app._2.dependencies should be (Set("/apps/d1".toPath, "/test/foo".toPath, "/test".toPath))
  }

  test("A group update can be applied to existing entries") {
    Given("A group with updates of existing nodes")
    val blaApp = AppDefinition("/test/bla".toPath, Some("foo"))
    val actual: Group = Group(PathId.empty, groups = Set(
      Group("/test".toPath, apps = Map(blaApp.id -> blaApp)),
      Group("/apps".toPath, groups = Set(Group("/apps/foo".toPath)))
    ))
    val update = GroupUpdate(
      Some(PathId.empty.toString),
      Some(Set.empty[App]),
      Some(Set(
        GroupUpdate(
          Some("test"),
          None,
          Some(Set(GroupUpdateHelper.empty("foo".toPath)))
        ),
        GroupUpdate(
          Some("apps"),
          Some(Set(App("app1", cmd = Some("foo"),
            dependencies = Set("d1", "../test/foo", "/test"))))
        )
      ))
    )
    val timestamp = Timestamp.now()

    When("The update is performed")
    val result = Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, actual, timestamp), groupConversionContext))

    validate(result)(Group.validRootGroup(maxApps = None, Set())).isSuccess should be(true)

    Then("The update is applied correctly")
    result.id should be(PathId.empty)
    result.groupsById should have size 2
    val test = result.group("test".toRootPath)
    test should be('defined)
    test.get.groupsById should have size 1
    test.get.apps should have size 1
    val apps = result.group("apps".toRootPath)
    apps should be('defined)
    apps.get.groupsById should have size 1
    apps.get.apps should have size 1
    val app = apps.get.apps.head
    app._1.toString should be ("/apps/app1")
    app._2.dependencies should be (Set("/apps/d1".toPath, "/test/foo".toPath, "/test".toPath))
  }

  test("GroupUpdate will update a Group correctly") {
    Given("An existing group with two subgroups")
    val app1 = AppDefinition("/test/group1/app1".toPath, Some("foo"))
    val app2 = AppDefinition("/test/group2/app2".toPath, Some("foo"))
    val current = Group(
      "/test".toPath,
      groups = Set(
        Group("/test/group1".toPath, Map(app1.id -> app1)),
        Group("/test/group2".toPath, Map(app2.id -> app2))
      )
    )

    When("A group update is applied")
    val update = GroupUpdate(
      Some("/test"),
      Some(Set.empty[App]),
      Some(Set(
        GroupUpdate(Some("/test/group1"), Some(Set(App("/test/group1/app3", cmd = Some("foo"))))),
        GroupUpdate(
          Some("/test/group3"),
          Some(Set.empty[App]),
          Some(Set(GroupUpdate(Some("/test/group3/sub1"), Some(Set(App("/test/group3/sub1/app4", cmd = Some("foo")))))))
        )
      ))
    )

    val timestamp = Timestamp.now()
    val result = Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, current, timestamp), groupConversionContext))

    validate(result)(Group.validRootGroup(maxApps = None, Set())).isSuccess should be(true)

    Then("The update is reflected in the current group")
    result.id.toString should be("/test")
    result.apps should be('empty)
    result.groupsById should have size 2
    val group1 = result.group("/test/group1".toPath).get
    val group3 = result.group("/test/group3".toPath).get
    group1.id should be("/test/group1".toPath)
    group1.apps.head._1 should be("/test/group1/app3".toPath)
    group3.id should be("/test/group3".toPath)
    group3.apps should be('empty)
  }

  test("A group update should not contain a version") {
    val update = GroupUpdate(None, version = Some(Timestamp.now().toOffsetDateTime))
    intercept[IllegalArgumentException] {
      Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, Group.empty, Timestamp.now()), groupConversionContext))
    }
  }

  test("A group update should not contain a scaleBy") {
    val update = GroupUpdate(None, scaleBy = Some(3))
    intercept[IllegalArgumentException] {
      Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, Group.empty, Timestamp.now()), groupConversionContext))
    }
  }

  test("Relative path of a dependency, should be relative to group and not to the app") {
    Given("A group with two apps. Second app is dependend of first.")
    val update = GroupUpdate(Some(PathId.empty.toString), Some(Set.empty[App]), Some(Set(
      GroupUpdate(
        Some("test-group"),
        Some(Set(
          App("test-app1", cmd = Some("foo")),
          App("test-app2", cmd = Some("foo"), dependencies = Set("test-app1"))))
      )
    )))

    When("The update is performed")
    val result = Raml.fromRaml((GroupConversion.UpdateGroupStructureOp(update, Group.empty, Timestamp.now()), groupConversionContext))

    validate(result)(Group.validRootGroup(maxApps = None, Set())).isSuccess should be(true)

    Then("The update is applied correctly")
    val group = result.group("test-group".toRootPath)
    group should be('defined)
    group.get.apps should have size 2
    val dependentApp = group.get.app("/test-group/test-app2".toPath).get
    dependentApp.dependencies should be (Set("/test-group/test-app1".toPath))
  }
}
