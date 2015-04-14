package relation;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import logger.Transaction;

/**
 * Prepares a list of operations to perform on given relations and executes it all as a
 * whole (concurrently using multithreading) when the executeQuery() method is called.
 */
public class QueryPlanner {
	
	private List<Operation> operations = new ArrayList<Operation>();

	/**
	 * Performs a select operation on the data, where a single attribute is checked against a value to see if it matches.
	 * @param in A reader from the initial relation.
	 * @param out A writer to write the resulting relation to.
	 * @param compareOn The attributes (column indices) to pass to the conditional.
	 * @param conditional A lambda expression for comparing attributes to determine whether the tuple matches the select criteria.
	 */
	public void select(Reader in, Writer out, List<Integer> compareOn, Conditional<String> conditional) {
		Operation select = new SelectOperation(in, out, compareOn, conditional);
		operations.add(select);
	}

	/**
	 * Performs a project operation on the data.
	 * @param in A reader from the relation to start with.
	 * @param out A writer to write the resulting relation to.
	 * @param keep A list of attributes (column indices) to keep in the resulting relation.
	 */
	public void project(Reader in, Writer out, List<Integer> keep) {
		Operation project = new ProjectOperation(in, out, keep);
		operations.add(project);
	}
	
	/**
	 * Joins the two tables in1 and in2 and writes the result to out. Joins on the condition specified in the conditional argument.
	 * @param in1 A reader from the first relation to join on. This should be the smaller relation.
	 * @param in2 A reader from second relation to join on. This should be the larger relation.
	 * @param out A writer to write the resulting relation.
	 * @param indicesOne The attributes (column index) in the first relation to pass to the conditional.
	 * @param indicesTwo The attributes (column index) in the second relation to pass to the conditional.
	 * @param condtional A lambda expression for comparing the attributes and determining whether the tuple should be included in the result.
	 */
	public void join(Reader in1, Reader in2, Writer out, List<Integer> indicesOne, List<Integer> indicesTwo, JoinConditional<String> conditional) {
		Operation join = new JoinOperation(in1, in2, out, indicesOne, indicesTwo, conditional);
		operations.add(join);
	}
	
	/**
	 * Runs the query by starting a thread for each operation.
	 * @param transaction The transaction associated with this query
	 */
	public void executeQuery(Transaction transaction) {
		for(Operation op : operations) {
			op.setTransaction(transaction);
			op.start();
		}
	}
}
