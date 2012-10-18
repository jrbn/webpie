package jobs;

import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyzeJobs {

	protected static Logger log = LoggerFactory.getLogger(AnalyzeJobs.class);

	public static void main(String[] args) {

		try {
			JobClient client = null;
			InetSocketAddress trackerAddress = JobTracker
					.getAddress(new Configuration());
			client = new JobClient(trackerAddress, new Configuration());

			FileWriter jobWriter = new FileWriter("job_stats");
			boolean jobFirstLine = true;
			FileWriter mapWriter = new FileWriter("map_stats");
			boolean mapFirstLine = true;
			FileWriter reduceWriter = new FileWriter("reduce_stats");
			boolean reduceFirstLine = true;

			JobStatus[] jobs = client.getAllJobs();
			String[] headers = null;
			String[] headersMap = null;
			String[] headersReduce = null;
			for (JobStatus jobstatus : jobs) {

				if (jobstatus.getRunState() == JobStatus.SUCCEEDED) {

					// Print all counters of the job
					RunningJob job = client.getJob(jobstatus.getJobID());
					log.info(job.getJobName());

					if (jobFirstLine) {
						// Get headers
						ArrayList<String> lHeaders = new ArrayList<String>();
						lHeaders.add("Job Name");
						for (Group group : job.getCounters()) {
							for (Counter counter : group) {
								lHeaders.add(counter.getDisplayName());
							}
						}
						headers = lHeaders.toArray(new String[lHeaders.size()]);
						String line = "";
						for (String header : headers)
							line += header + "\t";
						jobFirstLine = false;
						jobWriter.write(line + "\n");
					}

					String[] values = new String[headers.length];
					values[0] = job.getJobName();
					for (Group group : job.getCounters()) {
						for (Counter counter : group) {
							String displayName = counter.getDisplayName();
							int i = 0;
							boolean found = false;
							for (String header : headers) {
								if (header.equals(displayName)) {
									found = true;
									break;
								} else {
									i++;
								}
							}
							if (found) {
								values[i] = Long.toString(counter.getValue());
							}
						}
					}

					String line = "";
					for (String value : values) {
						if (value != null) {
							line += value + "\t";
						} else {
							line += "0\t";
						}
					}
					jobWriter.write(line + "\n");

					// Map
					TaskReport[] tasks = client.getMapTaskReports(jobstatus
							.getJobID());
					boolean firstJob = true;
					for (TaskReport task : tasks) {

						if (mapFirstLine) {
							// Print headers
							ArrayList<String> lh = new ArrayList<String>();
							lh.add("Job ID");
							lh.add("Job name");
							lh.add("Task name");
							for (Group group : task.getCounters()) {
								for (Counter counter : group) {
									lh.add(counter.getDisplayName());
								}
							}
							headersMap = lh.toArray(new String[lh.size()]);
							line = "";
							for (String header : headersMap) {
								line += header + "\t";
							}
							mapWriter.write(line + "\n");
							mapFirstLine = false;
						}

						values = new String[headersMap.length];
						values[0] = jobstatus.getJobID().toString();
						values[1] = job.getJobName();
						values[2] = task.getTaskID().toString();
						for (Group group : task.getCounters()) {
							for (Counter counter : group) {
								int i = 0;
								boolean found = false;
								for (String header : headersMap) {
									if (header.equals(counter.getDisplayName())) {
										found = true;
										break;
									} else {
										i++;
									}
								}

								if (found) {
									values[i] = Long.toString(counter
											.getValue());
								}
							}
						}

						line = "";
						for (String value : values) {
							if (value != null) {
								line += value + "\t";
							} else {
								line += "0\t";
							}
						}
						mapWriter.write(line + "\n");
					}

					// Reduce
					tasks = client.getReduceTaskReports(jobstatus.getJobID());
					firstJob = true;
					for (TaskReport task : tasks) {
						if (reduceFirstLine) {
							// Print headers
							ArrayList<String> lh = new ArrayList<String>();
							lh.add("Job ID");
							lh.add("Job name");
							lh.add("Task name");
							for (Group group : task.getCounters()) {
								for (Counter counter : group) {
									lh.add(counter.getDisplayName());
								}
							}
							headersReduce = lh.toArray(new String[lh.size()]);
							line = "";
							for (String header : headersReduce) {
								line += header + "\t";
							}
							reduceWriter.write(line + "\n");
							reduceFirstLine = false;
						}

						values = new String[headersReduce.length];

						values[0] = jobstatus.getJobID().toString();
						values[1] = job.getJobName();
						values[2] = task.getTaskID().toString();

						for (Group group : task.getCounters()) {
							for (Counter counter : group) {
								int i = 0;
								boolean found = false;
								for (String header : headersReduce) {
									if (header.equals(counter.getDisplayName())) {
										found = true;
										break;
									} else {
										i++;
									}
								}

								if (found) {
									values[i] = Long.toString(counter
											.getValue());
								}
							}
						}

						line = "";
						for (String value : values) {
							if (value != null) {
								line += value + "\t";
							} else {
								line += "0\t";
							}
						}
						reduceWriter.write(line + "\n");
					}

					/*
					 * for(TaskReport task : tasks) {
					 * //log.info(task.getCounters
					 * ().getGroupNames().toString()); //for(Counter counter :
					 * task.getCounters().getGroup(
					 * "org.apache.hadoop.mapred.Task$Counter")) { //
					 * log.info(counter.getName()); //}
					 * 
					 * long inputRecords = task.getCounters().findCounter(
					 * "org.apache.hadoop.mapred.Task$Counter",
					 * "REDUCE_INPUT_RECORDS").getCounter(); long outputRecords
					 * = task.getCounters().findCounter(
					 * "org.apache.hadoop.mapred.Task$Counter",
					 * "REDUCE_OUTPUT_RECORDS").getCounter(); //log.info("input"
					 * + inputRecords + " output " + outputRecords); n ++; load
					 * += (double)outputRecords / inputRecords; }
					 * log.info("Job " + jobstatus.getJobID() +
					 * " - average load = " + (load / n));
					 */
				}
			}

			jobWriter.close();
			mapWriter.close();
			reduceWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
