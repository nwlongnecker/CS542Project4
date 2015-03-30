package relation;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.List;

/**
 * Executes a project operation on a relation for one or more specified indexes. The output
 * is written to a buffer as GetNext() is called, eventually sending EOF with Close().
 */
public class ProjectOperation extends Operation {
	
	private List<Integer> indicesToKeep;

	public ProjectOperation(Reader in, Writer out, List<Integer> indicesToKeep) {
		super(in, out);
		this.indicesToKeep = indicesToKeep;
	}
	
	@Override
	public void getNext() throws IOException {
		String line = null;

		while ((line = readNextLine()) != null) {
			// use comma as separator
			String[] tuple = line.split(SEPARATOR);
			StringBuilder builder = new StringBuilder("");
			for (int i = 0; i < indicesToKeep.size()-1; i++) {
				builder.append(tuple[indicesToKeep.get(i)]);
				builder.append(SEPARATOR);
			}
			builder.append(tuple[indicesToKeep.get(indicesToKeep.size()-1)]);
			builder.append('\n');
			out.write(builder.toString());
		}
	}
}
