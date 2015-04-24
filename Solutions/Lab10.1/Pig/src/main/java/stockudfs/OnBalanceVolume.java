package stockudfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class OnBalanceVolume extends EvalFunc<Long> {
	private long previousObv;
	private double previousClose;
	
	@Override
	public Long exec(Tuple input) throws IOException {
		long volume = Long.parseLong(input.get(0).toString());
		double currentClose = Double.parseDouble(input.get(1).toString());
		long obv = 0;
		if(currentClose > previousClose) {
			obv = this.previousObv + volume;
		} else if(currentClose < previousClose) {
			obv = this.previousObv - volume;
		} else {
			obv = this.previousObv;
		}
		this.previousObv = obv;
		this.previousClose = currentClose;
		return obv;
		
	}

}
