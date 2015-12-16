package pitcher;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;


public class StrikeOutFraction extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return null;
		try {
			Object o1 = tuple.get(0);
			Object o2 = tuple.get(1);
			if (o1 == null || o2 == null) {
				return null;
			}
			DataBag bag = null;
			Map<String, Object> map = null;
			if (o1 instanceof DataBag) {
				bag = (DataBag) o1;
			} else { return null; }
			if (o2 instanceof Map) {
				map = (Map<String, Object>) o2;
			} else { return null; }
			
			Iterator<Tuple> values = bag.iterator();
			boolean flag = false;
			while (values.hasNext()) {
				if (values.next().get(0).toString().equals("Pitcher")) {
					flag = true;
					break;
				}
			}
			Object strikeouts = map.get("strikeouts");
			Object games = map.get("games");
			if (strikeouts == null || games == null) {
				return null;
			}
			if (flag) {
				return (double) (int) strikeouts / (int) games;
			}
			return null;
		}
		catch (ExecException e) {
			return null;
		}
	}
}
