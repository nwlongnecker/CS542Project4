package relation;

import java.util.List;

/**
 * Conditional with one argument.
 *
 * @param <T> Type of the argument to be compared.
 * @param <U> Type of the argument to be returned.
 */
public interface Conditional<T, U> {
	
	public U getResult(List<T> list);
}
