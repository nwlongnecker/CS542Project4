package relation;

/**
 * Conditional with one argument.
 *
 * @param <T> Type of the argument to be compared.
 */
public interface Conditional1<T> {
	
	public boolean compare(T o);
}
