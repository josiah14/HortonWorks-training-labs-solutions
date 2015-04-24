package stockudfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class HighestClosingPriceWindow extends AccumulatorEvalFunc<String>  {

	private ArrayList<String> highDates;
	private ArrayList<Float> highCloses;
	private float highClose;
	private int windowSize;
	private int accumulatedPrices;
	
	public HighestClosingPriceWindow(String windowSize) {
		int size = Integer.parseInt(windowSize);
		if(size > 0) {
			this.windowSize = size;
		} else {
			this.windowSize = 1;
		}
		highDates = new ArrayList<String>(this.windowSize);
		highCloses = new ArrayList<Float>(this.windowSize);
	}
	
	@Override
	public void accumulate(Tuple b) throws IOException {
		DataBag values = (DataBag) b.get(0);
		Iterator<Tuple> it = values.iterator();
		while(it.hasNext()) {
			Tuple currentTuple = it.next();
			float currentClose = Float.parseFloat(currentTuple.get(2).toString());
			if(currentClose > highClose) {
				highClose = currentClose;
				highCloses.add(0, currentClose);
				highDates.add(0, currentTuple.get(1).toString());
				accumulatedPrices = 1;
			} else if(accumulatedPrices < windowSize) {
				//add the current Tuple to the collections
				highCloses.add(accumulatedPrices, currentClose);
				highDates.add(accumulatedPrices, currentTuple.get(1).toString());
				accumulatedPrices++;
			}
		}
	}

	@Override
	public String getValue() {
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < accumulatedPrices; i++) {
			result.append(highDates.get(i) + " " + highCloses.get(i) + "\n");
		}
		return result.toString();
	}

	@Override
	//Gets invoked between bags passed to accumulate
	public void cleanup() {
		highDates.clear();
		highCloses.clear();
		highClose = 0;
		accumulatedPrices = 0;
	}
}
