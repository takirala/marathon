package mesosphere.marathon
package api.v3

import akka.Done
import akka.stream.ActorMaterializer
import mesosphere.AkkaUnitTest
import mesosphere.marathon.api._
import mesosphere.marathon.api.v2.{AppHelpers, AppNormalization}
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.experimental.repository.SyncTemplateRepository
import mesosphere.marathon.plugin.auth.{Authenticator, Authorizer}
import mesosphere.marathon.raml.{App, Raml}
import mesosphere.marathon.state.VersionInfo.OnlyVersion
import mesosphere.marathon.state._
import mesosphere.marathon.test.{GroupCreation, JerseyTest}
import org.apache.zookeeper.KeeperException.NoNodeException
import org.mockito.Matchers
import play.api.libs.json._

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class TemplatesResourceTest extends AkkaUnitTest with GroupCreation with JerseyTest {

  case class Fixture(
      auth: TestAuthFixture = new TestAuthFixture,
      repository: SyncTemplateRepository = mock[SyncTemplateRepository]) {
    val config: AllConf = AllConf.withTestConfig()
    implicit val mat = ActorMaterializer()
    val templatesResource: TemplatesResource = new TemplatesResource(
      repository,
      system.eventStream,
      config,
      PluginManager.None
    )(auth.auth, auth.auth, ctx, mat)

    implicit val authenticator: Authenticator = auth.auth
    implicit val authorizer: Authorizer = auth.auth

    val normalizationConfig = AppNormalization.Configuration(config.defaultNetworkName.toOption, config.mesosBridgeName())
    implicit lazy val appDefinitionValidator = AppDefinition.validAppDefinition(config.availableFeatures)(PluginManager.None)

    implicit val validateAndNormalizeApp: Normalization[raml.App] =
      AppHelpers.appNormalization(config.availableFeatures, normalizationConfig)(AppNormalization.withCanonizedIds())

    def normalize(app: App): App = {
      val migrated = AppNormalization.forDeprecated(normalizationConfig).normalized(app)
      AppNormalization(normalizationConfig).normalized(migrated)
    }

    def normalizeAndConvert(app: App): AppDefinition = {
      val normalized = normalize(app)
      Raml.fromRaml(normalized)
    }

    def appToBytes(app: App) = {
      val normed = normalize(app)
      val body = Json.stringify(Json.toJson(normed)).getBytes("UTF-8")
      body
    }
  }

  "Templates resource" should {
    "create a new template successfully" in new Fixture {
      Given("a template")
      val app = App(id = "/app", cmd = Some("cmd"))
      val body = appToBytes(app)
      val template = normalizeAndConvert(app)
      val version = "1"

      repository.create(any) returns Future.successful(version)

      When("it is created")
      val response = asyncRequest { r =>
        templatesResource.create(body, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(201)

      And("version of created template is returned")
      JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(JsObject(List("version" -> JsString(version))))
    }

    "find the latest template version" in new Fixture {
      Given("two templates with different app version timestamps")
      val app = App(id = "/app", cmd = Some("cmd"))
      val normalized = normalize(app)

      val templateOne = Raml.fromRaml(normalized)
      val templateTwo = templateOne.copy(versionInfo = OnlyVersion(templateOne.version + 1.minute))

      And(s"repository returns both saved templates for ${app.id}")
      val versions = Seq("1", "2")

      repository.contentsSync(templateOne.id) returns Success(versions)
      repository.readSync[AppDefinition](any, Matchers.eq(versions(0))) returns Success(templateOne)
      repository.readSync[AppDefinition](any, Matchers.eq(versions(1))) returns Success(templateTwo)

      When("latest template version is requested")
      val response = asyncRequest { r =>
        templatesResource.latest(templateOne.id.toString, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)

      And("returned JSON contains the the latest template with the biggest timestamp")
      val parsed = Json.parse(response.getEntity.toString)
      (parsed \ "template" \ "id").as[String] shouldBe app.id
      (parsed \ "template" \ "version").as[String] shouldBe templateTwo.version.toString
    }

    "find a template with provided version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)
      val version = "1"

      repository.readSync[AppDefinition](any, any) returns Success(template)

      When("template version is requested")
      val response = asyncRequest { r =>
        templatesResource.version(template.id.toString, version, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)

      And("template version is returned")
      val parsed = Json.parse(response.getEntity.toString)
      (parsed \ "template" \ "id").as[String] shouldBe app.id
    }

    "fail to get a template with a non-existing version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)
      val version = "1"

      repository.readSync(any, any) returns Failure(new NoNodeException(s"/templates/app/${version}"))

      When("a non-existing template version is requested")
      val response = asyncRequest { r =>
        templatesResource.version(template.id.toString, version, auth.request, r)
      }

      Then("request should fail")
      response.getStatus should be(500)

      And("response should contain information about missing template and version")
      response.getEntity.toString should include(new TemplateNotFoundException(template.id, Some(version)).getMessage)
    }

    "list all versions of the template" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)
      val versions = Seq("1", "2", "3")

      repository.contentsSync(any) returns Success(versions)

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)

      And("an array with three versions is expected")

      JsonTestHelper
        .assertThatJsonString(response.getEntity.asInstanceOf[String])
        .correspondsToJsonOf(JsObject(List("versions" -> JsArray(versions.map(JsString(_))))))
    }

    "list versions of the template where none exist" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.contentsSync(any) returns Success(Seq.empty)

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)

      And("empty version array is expected")

      JsonTestHelper
        .assertThatJsonString(response.getEntity.asInstanceOf[String])
        .correspondsToJsonOf(JsObject(List("versions" -> JsArray())))
    }

    "list versions of a non-existing template" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.contentsSync(any) returns Failure(new NoNodeException("/templates/app"))

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("request should fail")
      response.getStatus should be(500)

      And("contain the information about missing template")
      response.getEntity.toString should include(new TemplateNotFoundException(template.id).getMessage)
    }

    "delete a template with a provided version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)
      val version = "1"

      repository.delete(any, any) returns Future.successful(Done)

      val response = asyncRequest { r =>
        templatesResource.delete(template.id.toString, version, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)
    }

    "delete template and all versions" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.delete(equalTo(template.id)) returns Future.successful(Done)
      val response = asyncRequest { r =>
        templatesResource.delete(template.id.toString, auth.request, r)
      }

      Then("request is successful")
      response.getStatus should be(200)
    }

    "access without authentication is denied" in new Fixture() {
      Given("an unauthenticated request")
      auth.authenticated = false
      val req = auth.request
      val app = """{"id":"/a/b/c","cmd":"foo","ports":[]}"""

      When("we try to add a template")
      val create = asyncRequest { r =>
        templatesResource.create(app.getBytes("UTF-8"), req, r)
      }
      Then("we receive a NotAuthenticated response")
      create.getStatus should be(auth.NotAuthenticatedStatus)

      When("we try to fetch a template")
      val show = asyncRequest { r =>
        templatesResource.latest("", req, r)
      }
      Then("we receive a NotAuthenticated response")
      show.getStatus should be(auth.NotAuthenticatedStatus)

      When("we try to delete a template")
      val delete = asyncRequest { r =>
        templatesResource.delete("", req, r)
      }

      Then("we receive a NotAuthenticated response")
      delete.getStatus should be(auth.NotAuthenticatedStatus)
    }
  }
}
