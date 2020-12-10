package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickUpInfo")
  val freqQuery = "select x,y,z, count(*) as freq from pickupInfo where x between %f and %f and y between %f and %f and z between %d and %d group by x, y, z"
  val pickUpPointsFreqDF = spark.sql(freqQuery.format(minX, maxX, minY, maxY,minZ, maxZ))
  pickUpPointsFreqDF.createOrReplaceTempView("pickUpPointsFreq")

  //Calculate sigma Xj and sigma (Xj)^2
  val totalFreqDF = spark.sql("select sum(freq), sum(freq*freq) from pickUpPointsFreq")

  //Calculate X bar
  val X = totalFreqDF.first().getLong(0).toDouble/(numCells.toDouble)

  //Calculate S
  val S = Math.sqrt(totalFreqDF.first().getLong(1).toDouble/numCells.toDouble - X*X)
  spark.udf.register("findNeighbors", (X: Double, Y: Double, Z: Int, minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Int, maxZ: Int)=>((
    HotcellUtils.findNeighbours(X, Y, Z, minX, maxX, minY, maxY, minZ, maxZ)
    )))

  val WijXj_query = "select findNeighbors( p.x, p.y, p.z, %f, %f, %f, %f, %d, %d) as Wij, sum(q.freq) as Xj, p.x as X, p.y as Y, p.z as Z from pickupPointsFreq p, pickupPointsFreq q where" +
    " (q.x = p.x or q.x = p.x + 1 or q.x = p.x - 1) and (q.y = p.y or q.y = p.y + 1 or q.y = p.y - 1) and (q.z = p.z or q.z = p.z + 1 or q.z = p.z - 1) group by p.x,p.y,p.z"

  //Calculate Wij & Xj
  val WijXjDF = spark.sql(WijXj_query.format(minX, maxX, minY, maxY, minZ, maxZ))
  WijXjDF.createOrReplaceTempView("WijXjValues")

  spark.udf.register("calcG", (w: Double, sumX: Double, s: Double, nCells: Double, xBar: Double)=>((
    HotcellUtils.calcG(w, sumX, s, nCells, xBar)
    )))

  val gScore_query = "select x, y, z, calcG(Wij, Xj, %f, %f, %f) as GScore from WijXjValues"

  val gScoreDF = spark.sql(gScore_query.format(S, numCells.toDouble, X))
  gScoreDF.createOrReplaceTempView("GettisOrd")

  val result = spark.sql("select x, y, z from GettisOrd order by GScore DESC limit 50")


  return result // YOU NEED TO CHANGE THIS PART
}
}
