package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Executes a join operation on two relations based on a specified conditional. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class JoinOperation extends Operation {
	private Reader in2;
	private int index1, index2;
	private Conditional2<String> conditional;
	private Collection<String[]> rows;
	
	public JoinOperation(Reader in, Reader in2, Writer out, int index1, int index2, Conditional2<String> conditional) {
		super(in, out);
		this.in2 = in2;
		this.index1 = index1;
		this.index2 = index2;
		this.conditional = conditional;
		this.rows = new HashSet<String[]>();
	}

	@Override
	public void open() throws IOException {
		String line = null;

		while ((line = readNextLine()) != null) {
			rows.add(line.split(SEPARATOR));
		}
	}

	@Override
	public void getNext() throws IOException {
		String line = null;

		while ((line = readNextLineFrom2()) != null) {
			String[] lineTwo = line.split(SEPARATOR);
			for(String[] lineOne : rows) {
				if (conditional.compare(lineOne[index1], lineTwo[index2])) {
					StringBuilder builder = new StringBuilder("");
					for(String element : lineOne) {
						builder.append(element + SEPARATOR);
					}
					int j = 0;
					if (index2 != 0) {
						builder.append(lineTwo[j]);
					} else {
						builder.append(lineTwo[(++j)]);
					}
					for (int i = j + 1; i < lineTwo.length; i++) {
						if(i != index2) {
							builder.append(SEPARATOR);
							builder.append(lineTwo[i]);
						}
					}
					builder.append('\n');
					out.write(builder.toString());
				}
			}
		}
	}
	
	protected String readNextLineFrom2() throws IOException {
		StringBuilder builder = new StringBuilder("");
		int character = 0;
		
		while ((character = in2.read()) != '\n' && character > 0) {
			builder.append((char)character);
		}
		if (character <= 0 && builder.length() == 0) {
			return null;
		}
		return builder.toString().trim();
	}
	
}
