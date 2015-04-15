package logger;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

public class TransactionLoggerTest {

	@Test
	public void logTransactionChanges() {
		Logger log = new Logger();
		Transaction t = new Transaction(2, log);
		t.addLogRecord(new LogRecord(11,"relationName",321,4,"old value","new value"));
		t.commit();
		
		String contents = Logger.readFile(Logger.LOG_FILE);
		new File(Logger.LOG_FILE).delete();
		assertEquals("<START,2>\n<11,relationName:321:4,old value,new value>\n<COMMIT,2>\n", contents);
	}
}
