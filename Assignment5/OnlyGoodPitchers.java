package pitcher;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class OnlyGoodPitchers extends FilterFunc {

	/**
	 * Input is a tuple in the form of (player, positions, strikeout fraction, average).
	 */
	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return false;
		try {
			Object o1 = tuple.get(0);
			Object o2 = tuple.get(1);
			Object o3 = tuple.get(2);
			Object o4 = tuple.get(3);
			DataBag bag = null;
			if (o2 instanceof DataBag) {
				bag = (DataBag) o2;
			} else { 
				return false;
			}
			Iterator<Tuple> values = bag.iterator();
			boolean flag = false;
			while (values.hasNext()) {
				if (values.next().get(0).toString().equals("Pitcher")) {
					flag = true;
					break;
				}
			}
			if (flag) {
				if (o3 != null && o4 != null) {
					double strikeOutFraction = (double) o3;
					double avg = (double) o4;
					return strikeOutFraction > avg;
				} else {
					return false;
				}
			} else {
				return false;
			}
		} catch (ExecException e) {
			return false;
		}
	}
}
