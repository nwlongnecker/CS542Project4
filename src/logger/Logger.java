package logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for logging changes to the database. Used to restore to a consistent state on startup.
 * @author Nathan
 */
public class Logger {

	static final String START = "<";
	static final String START_TRANSACTION = "START";
	static final String COMMIT_TRANSACTION = "COMMIT";
	static final String COMMA = ",";
	static final String COLON = ":";
	static final String END = ">";
	
	static final String LOG_FILE = "db.log";
	static final String LINE_SEPARATOR = "\n";
	
	private static Logger logger;
	
	private Logger() {}
	
	public static Logger getLogger() {
		if (logger == null) {
			logger = new Logger();
		}
		return logger;
	}
	
	public void writeMessage(String message) {
		synchronized(this) {
			try (FileWriter fw = new FileWriter(new File(LOG_FILE),true)) {
				fw.append(message + LINE_SEPARATOR);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Collection<Transaction> recoverLog() {
		String[] contents = readFile(LOG_FILE).split(LINE_SEPARATOR);
		Map<Integer,Transaction> transactions = new HashMap<Integer,Transaction>();
		for(String line : contents) {
			if (!line.isEmpty()) {
				switch (line.charAt(1)) {
				case 'S': // Start transaction
					int transactionId = Integer.parseInt(line.replace(START,"").replace(END,"").split(COMMA)[1]);
					transactions.put(transactionId, new Transaction(transactionId));
					break;
				case 'C': // Commit transaction
					int transId = Integer.parseInt(line.replace(START,"").replace(END,"").split(COMMA)[1]);
					transactions.get(transId).commit();
					break;
				default:
					LogRecord lr = LogRecord.parseRecord(line);
					Transaction t = transactions.get(lr.getTransactionId());
					t.addLogRecord(lr);
				}
			}
		}
		return transactions.values();
	}
	
	/**
	 * Reads the specified file and returns the contents as a string
	 * @param path Path to the file
	 * @return The file contents
	 */
	public static String readFile(String path)	{
		String ret = null;
		try {
			ret = new String(Files.readAllBytes(Paths.get(path)));
		} catch (IOException e) {
			System.err.println("Error reading from file " + e.getMessage());
		}
		return ret;
	}
	
}
