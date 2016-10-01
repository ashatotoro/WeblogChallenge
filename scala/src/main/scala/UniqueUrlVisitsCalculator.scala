import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

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
object UniqueUrlVisitsCalculator {

	def withinTime(line: String) : Boolean = {
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

	def getUrlAndVisitorIp(line: String) : (String, String) = {
		val words = line.split("\\s")
        	val url = words(12)//.split("\\?")(0)
		val visitorIp = words(2)//.split(":")(0)

		return (url, visitorIp)
	}

	def main(args: Array[String]) : Unit = {

        val conf = new SparkConf().setAppName("UniqueUrlVisitsCalculator")
		val sc = new SparkContext(conf)

		val inputLog = sc.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")

		val urlCounts = inputLog.filter(line => withinTime(line))
                            .map(line => getUrlAndVisitorIp(line))
                            .distinct()
			    .map{case (url, visitorIp) => (url, 1)}
                            .reduceByKey((a, b) => (a + b))

		urlCounts.saveAsTextFile("uniqueUrlCount")
		
		val output = new PrintWriter(new File("uniqueUrlCount.out"))
		urlCounts.collect().foreach(line => output.println(line)) 
		output.close()
	}
}
