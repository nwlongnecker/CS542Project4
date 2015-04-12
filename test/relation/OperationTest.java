package relation;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class OperationTest {

	public OperationTest() {
	}
	
	@Test
	public void testProject() throws Exception {
		String input = "US,Boston,MA,12345,1000000\nUS,Worcester,MA,01609,750000\n";
		Reader in = new InputStreamReader(new ByteArrayInputStream(input.getBytes()));
		OutputStream output = new ByteArrayOutputStream();
		Writer out = new OutputStreamWriter(output);
		List<Integer> keep = new ArrayList<Integer>();
		keep.add(1); keep.add(4);
		Operation project = new ProjectOperation(in, out, keep);
		
		project.open();
		project.getNext();
		project.close();
		
		assertEquals("Boston,1000000\nWorcester,750000\n", output.toString());
	}
	
	@Test
	public void testSelect() throws Exception {
		String input = "US,Boston,MA,12345,1000000\nUS,Lincoln,MA,01773,15000\nUS,Worcester,MA,01609,750000";
		Reader in = new InputStreamReader(new ByteArrayInputStream(input.getBytes()));
		OutputStream output = new ByteArrayOutputStream();
		Writer out = new OutputStreamWriter(output);
		
		List<Integer> compareOn = new ArrayList<Integer>();
		compareOn.add(4);
		Operation select = new SelectOperation(in, out, compareOn, values -> Integer.parseInt(values.get(0)) > 500000);
		
		select.open();
		select.getNext();
		select.close();
		
		assertEquals("US,Boston,MA,12345,1000000\nUS,Worcester,MA,01609,750000\n", output.toString());
	}
	
	@Test
	public void testJoin() throws Exception {
		String input = "MA,30000000\n";
		String input2 = "US,Boston,MA,12345,1000000\nUS,Providence,RI,12412,150000\nUS,Worcester,MA,01609,750000";
		Reader in = new InputStreamReader(new ByteArrayInputStream(input.getBytes()));
		Reader in2 = new InputStreamReader(new ByteArrayInputStream(input2.getBytes()));
		OutputStream output = new ByteArrayOutputStream();
		Writer out = new OutputStreamWriter(output);
		
		List<Integer> attributesOne = new ArrayList<Integer>();
		attributesOne.add(0);
		List<Integer> attributesTwo = new ArrayList<Integer>();
		attributesTwo.add(2);
		Operation join = new JoinOperation(in, in2, out, attributesOne, attributesTwo, (values1, values2) -> values1.get(0).equals(values2.get(0)));
		
		join.open();
		join.getNext();
		join.close();
		
		assertEquals("MA,30000000,US,Boston,12345,1000000\nMA,30000000,US,Worcester,01609,750000\n", output.toString());
	}
	
	@Test
	public void testUpdate() throws Exception {
		String input = "US,Boston,MA,12345,1000000\nUS,Lincoln,MA,01773,15000\nUS,Worcester,MA,01609,750000";
		Reader in = new InputStreamReader(new ByteArrayInputStream(input.getBytes()));
		OutputStream output = new ByteArrayOutputStream();
		Writer out = new OutputStreamWriter(output);
		
		List<Integer> compareOn = new ArrayList<Integer>();
		compareOn.add(1);
		List<Integer> updateIndices = new ArrayList<Integer>();
		updateIndices.add(4);
		List<String> newValues = new ArrayList<String>();
		newValues.add("755000");
		Operation update = new UpdateOperation(in, out, compareOn, values -> values.get(0).equals("Worcester"), updateIndices, newValues);
		
		update.open();
		update.getNext();
		update.close();
		
		assertEquals("US,Boston,MA,12345,1000000\nUS,Lincoln,MA,01773,15000\nUS,Worcester,MA,01609,755000\n", output.toString());
	}
}
