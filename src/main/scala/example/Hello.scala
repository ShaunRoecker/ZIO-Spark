package example

import zio._
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._
import org.apache.spark.sql.types._

final case class Person(name: String, age: Int)

final case class Car(
  name: String, 
  mpg: Double, 
  cylinders: Double, 
  displacement: Double, 
  hp: Double, 
  weight: Double, 
  acceleration: Double, 
  year: String, 
  origin: String
)

final case class Number(value: Int)

object SimpleApp extends ZIOAppDefault {
  //force throwing exeptions from analysis, like missing columns, invalid sql, ...
  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException


  def readData: SIO[DataFrame] =
    SparkSession.read.schema[Person]
      .withHeader
      .withDelimiter(";")
      .csv("./src/main/resources/data.csv")

  def job(inputDs: DataFrame) = 
    inputDs.as[Person].headOption


  def firstDF: SIO[DataFrame] =
    SparkSession.read
      .withHeader
      .inferSchema
      .json("src/main/resources/cars.json")

  def numbersDF: SIO[DataFrame] =
    SparkSession.read
      .withHeader
      .schema(numberSchema)
      .csv("src/main/resources/numbers.csv")

  def showNumbers(df: DataFrame) =
    df.as[Number].show(10)

  val numberSchema = 
    StructType(
      Seq(
        StructField("number", IntegerType, nullable = false)
        )
    )
  
  

  val app: ZIO[SparkSession, Throwable, Unit] =
    for {
      _ <- Console.printLine("Hello, ZIO!")
      input <- readData
      maybePeople <- job(input)
      _ <- maybePeople match {
          case None => Console.printLine("There is nobody :(.")
          case Some(p) => Console.printLine(s"The first person's name is ${p.name}.")
        }
      
    } yield ()

  private val session = 
    SparkSession
      .builder
      .master(localAllNodes)
      .appName("app")
      .asLayer

  override def run: ZIO[ZIOAppArgs, Any, Any] = app.provide(session)
}

