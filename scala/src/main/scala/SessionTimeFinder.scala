import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io._
import java.lang.Math
import java.text.SimpleDateFormat

/**
 * This class aggregates the number of the visits for each user. (goal 1 in the task list)
 * Assume:
 *   - session time window is fixed, 15 minutes
 *   - this session time window starts at 2015-07-22T09:00:00.000Z
 *   - this session time windows ends at 2015-07-22T09:15:00.000Z
 *   - same visitorip but different port considered to be different visitor
 * It runs as a mapreduce job on a hadoop cluster.
 * Mapper prepares the data as (IP, 1)
 * Reducer adds the number up for each IP.
 * @author asha
 *
 */
object AverageSessionTime {

	def withinTime(line: String) : Boolean = {
		//do something
		val words = line.split("\\s")
		val visitTime = words(0)
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
		val startTime = sdf.parse("2015-07-22T09:00:00.000Z")
		val endTime = sdf.parse("2015-07-22T09:15:00.000Z")
		val currentTimeStr = visitTime.substring(0, 20) + String.valueOf(Integer.parseInt(visitTime.substring(20, 26)) / 1000) + "Z"
		val currentTime = sdf.parse(currentTimeStr)

		if (currentTime.after(startTime) && currentTime.before(endTime)) {
			return true
		}

		return false
	}

	def getVisitorIpAndTime(line: String) : (String, String) = {
		val words = line.split("\\s")
		val visitTime = words(0)
		val visitorIp = words(2)
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
		val currentTimeStr = visitTime.substring(0, 20) + String.valueOf(Integer.parseInt(visitTime.substring(20, 26)) / 1000) + "Z";

		return (visitorIp, currentTimeStr)
	}

	def getMaxTime(timeA:String, timeB:String) : String = {
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
		val time1 = sdf.parse(timeA)
		val time2 = sdf.parse(timeB)

		if (time1.after(time2)) return timeA
		
		return timeB
	}

	def main(args: Array[String]) : Unit = {

		val conf = new SparkConf().setAppName("SessionTimeFinder")
		val sc = new SparkContext(conf)

		val inputLog = sc.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")

		val hitsCounts = inputLog.filter(line => withinTime(line)).map(line => (getVisitorIpAndTime(line))).reduceByKey((a, b) => getMaxTime(a, b))

		hitsCounts.saveAsTextFile("avgSessionTime")
		
		val output = new PrintWriter(new File("sessionTime.out"))
		hitsCounts.collect().foreach(line => output.println(line)) 
		output.close()
	}
}
