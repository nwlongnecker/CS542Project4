package relation;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {
	
	private List<Operation> operations = new ArrayList<Operation>();

	public void select(Reader in, Writer out, int compareOn, Conditional1<String> conditional) {
		Operation select = new SelectOperation(in, out, compareOn, conditional);
		operations.add(select);
	}
	
	public void select(Reader in, Writer out, int compareOne, int compareTwo, Conditional2<String> conditional) {
		Operation select = new SelectOperation(in, out, compareOne, compareTwo, conditional);
		operations.add(select);
	}

	public void project(Reader in, Writer out, List<Integer> keep) {
		Operation project = new ProjectOperation(in, out, keep);
		operations.add(project);
	}
	
	public void join(Reader in1, Reader in2, Writer out, int index1, int index2, Conditional2<String> condtional) {
		Operation join = new JoinOperation(in1, in2, out, index1, index2, condtional);
		operations.add(join);
	}
	
	public void executeQuery() {
		for(Operation op : operations) {
			op.start();
		}
	}
}
