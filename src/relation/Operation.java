package relation;

import java.io.Reader;
import java.io.Writer;

public abstract class Operation {
	
	protected Reader in;
	protected Writer out;

	public Operation(Reader in, Writer out) {
		this.in = in;
		this.out = out;
	}

}
