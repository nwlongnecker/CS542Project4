package relation;

import java.util.Collection;
import java.util.List;

public class Relation {

	public Relation() {
		// TODO Auto-generated constructor stub
	}
	
	public Relation select(Collection<Condition> conditions) {
		return null;
	}

	public Relation project(List<String> attributes) {
		return null;
	}
	
	public Relation join(Relation other, Collection<Condition> conditions) {
		return null;
	}
	
	public boolean addTuple(List<String> values) {
		return false;
	}
	
	public boolean updateTuple(Collection<Condition> conditions, List<String> attributes, List<String> values) {
		return false;
	}
	
	public boolean deleteTuple(Collection<Condition> conditions) {
		return false;
	}

}
