package relation;

import java.util.List;

/**
 * Conditional on two tables.
 *
 * @param <T> Type of the argument to be compared.
 */
public interface JoinConditional<T> {
	
	public boolean compare(List<T> relationOneIndices, List<T> relationTwoIndices);
}
