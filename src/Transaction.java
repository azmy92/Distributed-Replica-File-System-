import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

public class Transaction {

	long Tid;
	long MesgSeq; // expected write message sequence number
	String[] buffer;
	Semaphore semaphore;// for ensuring sequential consistency
	String file; // file associated with transaction
	PrintWriter logWriter;// log file for every transaction
	int BUFFER_SIZE = 100;
	Timer timer;
	TimerTask task;

	public Transaction(long transactionId, String fileName) throws IOException {
		this.Tid = transactionId;
		this.file = fileName;
		buffer = new String[BUFFER_SIZE];
		semaphore = new Semaphore(1);
		logWriter = new PrintWriter(new BufferedWriter(new FileWriter(
				"transactions/" + transactionId, true)));
		initLog();
		timer = new Timer();

	}

	public void scheduleJob(TimerTask task, long delay) {
		this.task = task;
		timer.schedule(task, delay);
	}

	public void cancelJob() {
		task.cancel();
	}

	private void initLog() {
		logWriter.println("transaction id: " + Tid);
		logWriter.println("transaction file: " + file);
		logWriter.flush();
	}

	public void appendLog(String tag, String msg) {
		logWriter.println(tag + ": " + msg);
		logWriter.flush();
	}
	
	public void flushFile() throws IOException{
		PrintWriter transFileWriter;
		
			transFileWriter = new PrintWriter(new BufferedWriter(
					new FileWriter(file, true)));
		

		for (int i = 0; i < buffer.length; i++) {
			if (buffer[i] == "" || buffer[i] == null)
				break;
			transFileWriter.println(buffer[i]);
		}
		transFileWriter.flush();
		transFileWriter.close();
	}
}
