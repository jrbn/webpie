package jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Task;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractHostnameFromJobTracker {

	protected static Logger log = LoggerFactory
			.getLogger(ExtractHostnameFromJobTracker.class);

	public static void main(String[] args) throws IOException {
		FileWriter writer = null;
		try {
			writer = new FileWriter("hostname");
			FileReader reader = new FileReader(args[0]);
			BufferedReader br = new BufferedReader(reader);
			Set<String> alreadyProcessed = new HashSet<String>();

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] files = fs.listStatus(new Path(
					"/var/scratch/jurbani/cluster/logs/history/"));

			while (true) {
				String jobId = br.readLine();
				if (jobId == null)
					break;
				if (!alreadyProcessed.contains(jobId)) {
					alreadyProcessed.add(jobId);
					// log.info(jobId);

					for (FileStatus status : files) {
						if (status.getPath().getName().indexOf(jobId) != -1
								&& !status.getPath().getName().endsWith(".xml")) {
							JobInfo job = new JobHistory.JobInfo(jobId);
							DefaultJobHistoryParser.parseJobTasks(status
									.getPath().toString(), job, fs);

							Map<String, Task> allTasks = job.getAllTasks();
							for (Map.Entry<String, Task> entry : allTasks
									.entrySet()) {
								Map<String, TaskAttempt> allTasksAttempts = entry
										.getValue().getTaskAttempts();
								for (Map.Entry<String, TaskAttempt> entryAttemp : allTasksAttempts
										.entrySet()) {
									if (Values.SUCCESS.name().equals(
											entryAttemp.getValue().get(
													Keys.TASK_STATUS))
											&& Values.MAP.name().equals(
													entryAttemp.getValue().get(
															Keys.TASK_TYPE))) {
										writer.write(entryAttemp.getValue()
												.get(Keys.TASK_ATTEMPT_ID)
												+ " \t"
												+ entryAttemp.getValue().get(
														Keys.HOSTNAME) + "\n");
									}
								}
							}
						}
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
		}
	}
}
