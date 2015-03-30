package relation;

/**
 * Conditional with two arguments.
 *
 * @param <T> Type of the arguments to be compared.
 */
public interface Conditional2<T> {

	public boolean compare(T o1, T o2);
}
