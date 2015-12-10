package homerun;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


public class OnlyGoodPitchers extends FilterFunc {

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return false;
		try {
			Object o1 = tuple.get(0);
			Object o2 = tuple.get(1);
			if (o1 == null || o2 == null) {
				return 0.0;
			}
			int homeruns = (int) o1;
			int games = (int) o2;
			double hr = (double) homeruns;
			return hr / games * 100;
		}
		catch (ExecException e) {
			return 0.0;
		}
	}
}
