package relation;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import logger.LogRecord;

/**
 * Executes an update operation on a relation based on a specified conditional. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class UpdateOperation extends Operation {
	private List<Integer> compareOn;
	private Conditional<String, Boolean> conditional;
	private List<Integer> updateIndices;
	private Conditional<String, String> newValues;
	private String relationName;
	private int rowNumber;

	public UpdateOperation(Reader in, Writer out, String relationName, List<Integer> compareOn, Conditional<String, Boolean> conditional, List<Integer> updateIndices, Conditional<String, String> newValues) {
		super(in, out);
		this.compareOn = compareOn;
		this.conditional = conditional;
		this.updateIndices = updateIndices;
		this.newValues = newValues;
		this.relationName = relationName;
		rowNumber = 0;
	}

	@Override
	public void getNext() throws IOException {
		String line = null;

		while ((line = readNextLine()) != null) {
			// use comma as separator
			String[] tuple = line.split(SEPARATOR);
			
			List<String> attributes = new ArrayList<String>();
			for(Integer i : compareOn) {
				attributes.add(tuple[i]);
			}
			try {
				if (conditional.getResult(attributes)) {
					for (int i = 0; i < updateIndices.size(); i++) {
						String newValue = newValues.getResult(attributes);
						String oldValue = tuple[updateIndices.get(i)];
						tuple[updateIndices.get(i)] = newValue;
						transaction.addLogRecord(new LogRecord(transaction.getTransactionID(), relationName, rowNumber, updateIndices.get(i), oldValue, newValue));
					}
					StringBuilder builder = new StringBuilder("");
					for (int i = 0; i < tuple.length-1; i++) {
						builder.append(tuple[i]);
						builder.append(SEPARATOR);
					}
					builder.append(tuple[tuple.length-1]);
					builder.append('\n');
					out.write(builder.toString());
				} else {
					out.write(line+'\n');
				}
				rowNumber++;
			} catch (Exception e) {
				// do nothing, continue the update
			}
		}
	}
}
