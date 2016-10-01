import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._
import java.text.SimpleDateFormat

/**
 * This class aggregates the unique visits for each URL in a session (Goal 3 in the task list)
 * Assume:
 *   - session time window is fixed, 15 minutes
 *   - this session time window starts at 2015-07-22T09:00:00.000Z
 *   - this session time windows ends at 2015-07-22T09:15:00.000Z
 * It runs as a mapreduce job on a hadoop cluster.
 * Mapper prepares the data as (url, ip)
 * Reducer removes the duplicated ips and count the unique visits.
 * @author asha
 *
 */
object UserInfo {

	def main(args: Array[String]) : Unit = {
		val filename = "sessionTime.out"
		val line = null
		var count = 0
		var timeDiff = 0L
		var mostEngagedUser = ""
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
		try 
		{
			val startTime = sdf.parse("2015-07-22T09:00:00.000Z")
			var maxTime = sdf.parse("2015-07-22T09:00:00.000Z")
			for (line <- Source.fromFile(filename).getLines()) 
			{
  				count += 1
				val results = line.split(",")
				val currentTime = sdf.parse(results(1).substring(0, results(1).size - 1))
				timeDiff += (currentTime.getTime() - startTime.getTime())
				if (currentTime.after(maxTime)) {
					mostEngagedUser = results(0).substring(1, results(0).size - 1)
					maxTime = currentTime
				}
			}
			val diffSeconds = timeDiff / count / 1000.0
			System.out.println("Avg session time: " + diffSeconds + "seconds")
			System.out.println("The most engaged user ip is:" + mostEngagedUser)
		} 
		catch 
		{
  			case ex: FileNotFoundException => println("Couldn't find that file.")
  			case ex: IOException => println("Had an IOException trying to read that file")
		}
	}
}
