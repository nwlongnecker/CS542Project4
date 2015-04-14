package relation;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes an update operation on a relation based on a specified conditional. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class UpdateOperation extends Operation {
	private List<Integer> compareOn;
	private Conditional<String> conditional;
	private List<Integer> updateIndices;
	private List<String> newValues;

	public UpdateOperation(Reader in, Writer out, List<Integer> compareOn, Conditional<String> conditional, List<Integer> updateIndices, List<String> newValues) {
		super(in, out);
		this.compareOn = compareOn;
		this.conditional = conditional;
		this.updateIndices = updateIndices;
		this.newValues = newValues;
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
				if (conditional.compare(attributes)) {
//					transaction.addUpdate();
					for (int i = 0; i < updateIndices.size(); i++) {
						tuple[updateIndices.get(i)] = newValues.get(i);
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
			} catch (Exception e) {}
		}
	}
}
