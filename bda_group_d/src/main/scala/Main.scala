//@main def hello(): Unit =
//  println("Hello world!")
//  println(msg)
//  println(data)

//def msg = "I was compiled by Scala 3. :)"


// load data from the data folder and print it
//val data = os.read(os.Path("C:\\4_MSE\\Project_BDA_GroupeD\\bda_group_d\\data\\open-data\\data\\matches\\2\\27.json"))
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  // Initialize SparkSession with GCS support
  val spark = SparkSession.builder()
    .appName("Football Data Analysis")
    .config("spark.master", "local")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()

  // Your code
  val matchId = 3890561  // Example match ID, replace with the actual match ID you want to load
  val filePath = s"gs://2024-bda-projet-group-d/open-data/data/events/$matchId.json"

  // Read the JSON file into a DataFrame
  val df_events = spark.read.option("multiline", "true").json(filePath)

  // Show the DataFrame to verify the content
  val df_tmp = df_events.select("index", "timestamp", "type", "possession_team").show()

  // Filter events to include only passes
  val passEvents = df_events.filter(col("type.name").contains("Pass"))

  // Group by 'possession_team' and count the passes
  val passCountByTeam = passEvents.groupBy("possession_team.name")  // Assuming possession_team is a struct with a name field
    .count()
    .withColumnRenamed("count", "pass_count")

  // Order by the number of passes in descending order to get the team with the most passes
  val teamWithMostPasses = passCountByTeam.orderBy(desc("pass_count"))

  // Show the result
  teamWithMostPasses.show()

  spark.stop()  // Stop the Spark session
}

