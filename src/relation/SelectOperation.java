package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.Comparator;

public class SelectOperation extends Operation {
	private int compareOne, compareTwo;
	private Conditional1<String> conditional1 = null;
	private Conditional2<String> conditional2 = null;

	public SelectOperation(Reader in, Writer out, int compareOne, Conditional1<String> conditional) {
		super(in, out);
		this.compareOne = compareOne;
		this.conditional1 = conditional;
	}
	
	public SelectOperation(Reader in, Writer out, int compareOne, int compareTwo, Conditional2<String> conditional) {
		super(in, out);
		this.compareOne = compareOne;
		this.compareTwo = compareTwo;
		this.conditional2 = conditional;
	}

	@Override
	public void getNext() throws IOException {
		String line = null;

		while ((line = readNextLine()) != null) {
			// use comma as separator
			String[] tuple = line.split(SEPARATOR);
			
			if (conditional1 != null) {
				try {
					if (conditional1.compare(tuple[compareOne])) {
						out.write(line+'\n');
					}
				} catch (Exception e) {}
			} else {
				try {
					if (conditional2.compare(tuple[compareOne], tuple[compareTwo])) {
						out.write(line+'\n');
					}
				} catch (Exception e) {}
			}
		}
	}
}
