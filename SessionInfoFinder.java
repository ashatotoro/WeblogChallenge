package com.asha.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class finds the average session time and the most engaged user. (Goal 2 and Goal 4 in the task list).
 * this class uses the output of a mapreduce job (GetSessionTime: finds the latest visiting time for each user in a session, and generate a small output file), 
 * then runs on local computer.
 * Average session time is calculated by aggregating the time difference between the latest visiting time and session starting time.
 * Most engaged user is calculated by finding the user who has the longest session time.
 * @author asha
 *
 */
public class SessionInfoFinder {
	public static void main(String[] args) {

		String fileName = "/home/asha/hadoop-2.6.4/session_data/session_time";
		String line = null;
		int count = 0;
		long timeDiff = 0;
		String mostEngagedUser = "";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");

		try {
			Date startTime = sdf.parse("2015-07-22T09:00:00.000Z");
			Date maxTime = sdf.parse("2015-07-22T09:00:00.000Z");
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				count++;
				String[] results = line.split("\\s");
				Date currentTime = sdf.parse(results[1]);
				timeDiff += (currentTime.getTime() - startTime.getTime());
				if (currentTime.after(maxTime)) {
					mostEngagedUser = results[0];
					maxTime = currentTime;
				}
			}
			double diffSeconds = timeDiff / count / 1000.0;

			System.out.println("Avg session time: " + diffSeconds + "seconds");
			System.out.println("The most engaged user ip is:" + mostEngagedUser);
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
		} catch (ParseException e) {
			System.out.println("Error parsing file '" + fileName + "'");
		}
	}

}
