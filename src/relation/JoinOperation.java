package relation;

import java.io.Reader;
import java.io.Writer;

public class JoinOperation extends Operation {
	private Reader in2;
	
	public JoinOperation(Reader in, Reader in2, Writer out) {
		super(in, out);
	}

	public void open() {
		
	}

	public void getNext() {
		
	}
	
	public void close() {
		
	}
}
