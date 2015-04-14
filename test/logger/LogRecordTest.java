package logger;

import static org.junit.Assert.*;

import org.junit.Test;

public class LogRecordTest {

	@Test
	public void testLogRecordParse() throws Exception {
		LogRecord lr = LogRecord.parseRecord("<11,relationName:321:4,old value,new value>");
		assertEquals(11, lr.getTransactionId());
		assertEquals("relationName", lr.getRelationName());
		assertEquals(321, lr.getRowNumber());
		assertEquals(4, lr.getColumnNumber());
		assertEquals("old value", lr.getOldValue());
		assertEquals("new value", lr.getNewValue());
	}
}
