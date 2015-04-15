package logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database transaction
 * @author Nathan
 */
public class Transaction implements Serializable {

	/**
	 * For serializing
	 */
	private static final long serialVersionUID = 4077074111410750234L;
	
	private final int transactionId;
	private final List<LogRecord> logRecords;
	private boolean isCommitted;
	
	/**
	 * Constructs a transaction
	 * @param transactionId 
	 * @param filename The file this transaction is associated with
	 */
	public Transaction(int transactionId) {
		this.transactionId = transactionId;
		this.logRecords = new ArrayList<LogRecord>();
		this.isCommitted = false;
		Logger.getLogger().writeMessage(Logger.START + Logger.START_TRANSACTION + Logger.COMMA + transactionId + Logger.END);
	}
	
	/**
	 * @return the transactionId
	 */
	public int getTransactionID() {
		return transactionId;
	}
	
	public List<LogRecord> getLogRecords() {
		return logRecords;
	}
	
	public boolean isCommitted() {
		return isCommitted;
	}
	
	public void commit() {
		isCommitted = true;
		Logger.getLogger().writeMessage(Logger.START + Logger.COMMIT_TRANSACTION + Logger.COMMA + transactionId + Logger.END);
	}
	
	public void addLogRecord(LogRecord logRecord) {
		logRecords.add(logRecord);
		Logger.getLogger().writeMessage(logRecord.toString());
	}
}
