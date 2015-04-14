package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;

import logger.Transaction;

/**
 * Represents an operation that can be run as part of a query by
 * the QueryPlanner class. Uses Open(), GetNext(), and Close().
 */
public abstract class Operation extends Thread {
	
	public static final String SEPARATOR = ",";
	
	protected Reader in;
	protected Writer out;
	protected Transaction transaction;

	public Operation(Reader in, Writer out) {
		this.in = in;
		this.out = out;
	}

	public void open() throws IOException { }
	
	public abstract void getNext() throws IOException;

	public void close() throws IOException {
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected String readNextLine() throws IOException {
		StringBuilder builder = new StringBuilder("");
		int character = 0;
		
		while ((character = in.read()) != '\n' && character > 0) {
			builder.append((char)character);
		}
		if (character <= 0 && builder.length() == 0) {
			return null;
		}
		return builder.toString().trim();
	}
	
	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}

	public void run() {
		try {
			open();
			getNext();
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
