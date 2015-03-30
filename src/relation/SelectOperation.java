package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.Comparator;

public class SelectOperation extends Operation {
	private int compareOn;
	private Conditional1<String> conditional;

	public SelectOperation(Reader in, Writer out, int compareOn, Conditional1<String> conditional) {
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
			
			if (conditional.compare(tuple[compareOn])) {
				out.write(line+'\n');
			}
		}
	}
}
