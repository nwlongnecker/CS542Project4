package logger;

public class LogRecord {
	
	private final int transactionId;
	private final String relationName;
	private final int rowNumber;
	private final int columnNumber;
	private final String oldValue;
	private final String newValue;
	
	public LogRecord(int transactionId, String relationName, int rowNumber, int columnNumber, String oldValue, String newValue) {
		this.transactionId = transactionId;
		this.relationName = relationName;
		this.rowNumber = rowNumber;
		this.columnNumber = columnNumber;
		this.oldValue = oldValue;
		this.newValue = newValue;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Logger.START);
		sb.append(transactionId);
		sb.append(Logger.COMMA);
		sb.append(relationName);
		sb.append(Logger.COLON);
		sb.append(rowNumber);
		sb.append(Logger.COLON);
		sb.append(columnNumber);
		sb.append(Logger.COMMA);
		sb.append(oldValue);
		sb.append(Logger.COMMA);
		sb.append(newValue);
		sb.append(Logger.END);
		return sb.toString();
	}
	
	public static LogRecord parseRecord(String record) {
		String[] values = record.replace(Logger.START,"").replace(Logger.END,"").split(Logger.COMMA);
		String[] location = values[1].split(Logger.COLON);
		
		int transactionId = Integer.parseInt(values[0]);
		String relationName = location[0];
		int rowNumber = Integer.parseInt(location[1]);
		int columnNumber = Integer.parseInt(location[2]);
		String oldValue = values[2];
		String newValue = values[3];
		return new LogRecord(transactionId, relationName, rowNumber, columnNumber, oldValue, newValue);
	}

	/**
	 * @return the transactionId
	 */
	public int getTransactionId() {
		return transactionId;
	}

	/**
	 * @return the relationName
	 */
	public String getRelationName() {
		return relationName;
	}

	/**
	 * @return the rowNumber
	 */
	public int getRowNumber() {
		return rowNumber;
	}

	/**
	 * @return the columnNumber
	 */
	public int getColumnNumber() {
		return columnNumber;
	}

	/**
	 * @return the oldValue
	 */
	public String getOldValue() {
		return oldValue;
	}

	/**
	 * @return the newValue
	 */
	public String getNewValue() {
		return newValue;
	}
}
