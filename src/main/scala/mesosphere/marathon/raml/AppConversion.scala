package mesosphere.marathon
package raml

import mesosphere.marathon.state.{ AppDefinition, PathId }

trait AppConversion {

  // FIXME: implement complete conversion for all app fields
  // See https://mesosphere.atlassian.net/browse/MARATHON-1291
  implicit val appWriter: Writes[AppDefinition, App] = Writes { appDef =>
    App(id = appDef.id.toString)
  }

  implicit val appReader: Reads[App, AppDefinition] = Reads { app =>
    AppDefinition(id = PathId(app.id))
  }
}
