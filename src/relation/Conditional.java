package relation;

import java.util.List;

/**
 * Conditional with one argument.
 *
 * @param <T> Type of the argument to be compared.
 */
public interface Conditional<T> {
	
	public boolean compare(List<T> list);
}
