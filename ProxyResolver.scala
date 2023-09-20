//> using scala 2.13
//> using lib "com.github.seratch::awscala:0.9.2"
//> using lib "dev.zio::zio-http:3.0.0-RC2"

import java.io.File
import java.nio.charset.CodingErrorAction
import java.nio.file.Paths

import scala.annotation.tailrec
import scala.io.{Codec, Source}
import scala.util.{Properties, Try, Using}

import awscala.Credentials
import awscala.s3.{Bucket, S3}
import zio._
import zio.http._
import zio.stream.ZStream

object ProxyResolver extends ZIOAppDefault  {

  private val DefaultRegion = awscala.Region.EU_CENTRAL_1

  private object Env {
    val AwsAccessKeyId   = "AWS_ACCESS_KEY_ID"
    val AwsSecretKeyId   = "AWS_SECRET_ACCESS_KEY"
    val AwsDefaultRegion = "AWS_DEFAULT_REGION"
    val ProxyServerPort  = "PROXY_SERVER_PORT"
  }

  private object PropertyKeys {
    val AccessKey = "accessKey"
    val SecretKey = "secretKey"
    val Region    = "region"
  }

  private val app: App[Any] = {
    Http.collect[Request] {
      case Method.GET -> "" /: bucketName /: keyPath =>

        getClient(bucketName).flatMap { s3 =>
          val bucket = Bucket(bucketName)
          val key = keyPath.toString

          s3.getObject(bucket, key)
        }.map(_.content)
        .map(is => Response(body = Body.fromStream(ZStream.fromInputStream(is))))
        .getOrElse(Response.status(Status.NotFound))
    }
  }

  private def getClient(bucketName: String): Option[S3] = {

    val fileProducers = List(Some(bucketName), None)
      .map(_.map(bn => s".${bn}_s3credentials").getOrElse(".s3credentials"))
      .flatMap(fileName =>
        List(
          Paths.get("").toAbsolutePath,
          Paths.get(Properties.userHome),
          Paths.get(Properties.userHome).resolve(".sbt"),
          Paths.get(Properties.userHome).resolve(".coursier")
        ).map(_.resolve(fileName).toFile)
          .map(path => () => readFromFile(path))
      )

    @tailrec
    def loop(curr: Option[S3], producers: List[() => Option[S3]] ): Option[S3] = {
      (curr, producers) match {
        case (Some(s3), _)                  => Some(s3)
        case (_, producer :: nextProducers) => loop(producer(), nextProducers)
        case (_, Nil)                       => None
      }
    }

    loop(readFromEnv, fileProducers)
  }

  private def readFromEnv: Option[S3] = {
    for {
      accessKey <- sys.env.get(Env.AwsAccessKeyId)
      secretKey <- sys.env.get(Env.AwsSecretKeyId)
    } yield {
      val region = sys.env.get(Env.AwsDefaultRegion)
        .map(awscala.Region.apply)
        .getOrElse(DefaultRegion)

      S3(Credentials(accessKey, secretKey))(region)
    }
  }

  private def readFromFile(file: File): Option[S3] = {

    implicit val codec: Codec = Codec("UTF-8")
       .onMalformedInput(CodingErrorAction.REPLACE)
       .onUnmappableCharacter(CodingErrorAction.REPLACE)

    Using(Source.fromFile(file)){ f =>
      val credentials = f.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .map(_.split("=").toList.map(_.trim))
        .collect { case key :: value :: Nil => (key, value) }
        .toMap

      for {
        accessKey <- credentials.get(PropertyKeys.AccessKey)
        secretKey <- credentials.get(PropertyKeys.SecretKey)
      } yield {
        val region = credentials.get(PropertyKeys.Region)
          .orElse(sys.env.get(Env.AwsDefaultRegion))
          .map(awscala.Region.apply)
          .getOrElse(DefaultRegion)

        S3(Credentials(accessKey, secretKey))(region)
      }
    }.toOption.flatten
  }

  private val serverPort = sys.env.get(Env.ProxyServerPort).flatMap(raw => Try(raw.toInt).toOption).getOrElse(8080)

  override val run: ZIO[Any, Throwable, Unit] = Server.serve(app).provide(Server.defaultWithPort(serverPort))

} 
