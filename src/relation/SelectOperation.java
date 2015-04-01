package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes a select operation on a relations based on a specified conditional. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class SelectOperation extends Operation {
	private List<Integer> compareOn;
	private Conditional<String> conditional;

	public SelectOperation(Reader in, Writer out, List<Integer> compareOn, Conditional<String> conditional) {
		super(in, out);
		this.compareOn = compareOn;
		this.conditional = conditional;
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
					out.write(line+'\n');
				}
			} catch (Exception e) {}
		}
	}
}
