package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Executes a join operation on two relations based on a specified conditional. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class JoinOperation extends Operation {
	private Reader in2;
	private List<Integer> indicesOne, indicesTwo;
	private JoinConditional<String> conditional;
	private Collection<String[]> rows;
	
	public JoinOperation(Reader in, Reader in2, Writer out, List<Integer> indicesOne, List<Integer> indicesTwo, JoinConditional<String> conditional) {
		super(in, out);
		this.in2 = in2;
		this.indicesOne = indicesOne;
		this.indicesTwo = indicesTwo;
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
				List<String> attributesOne = new ArrayList<String>();
				List<String> attributesTwo = new ArrayList<String>();
				
				for(Integer i : indicesOne) {
					attributesOne.add(lineOne[i]);
				}
				for(Integer i : indicesTwo) {
					attributesTwo.add(lineTwo[i]);
				}
				if (conditional.compare(attributesOne, attributesTwo)) {
					StringBuilder builder = new StringBuilder("");
					for(String element : lineOne) {
						builder.append(element + SEPARATOR);
					}
					int j = 0;
					while (indicesTwo.contains(j)) {
						j++;
					}
					if (j < lineTwo.length) {
						builder.append(lineTwo[j]);
					}
					for (int i = j + 1; i < lineTwo.length; i++) {
						if(!indicesTwo.contains(i)) {
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
