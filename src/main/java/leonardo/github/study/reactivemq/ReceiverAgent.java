package leonardo.github.study.reactivemq;

import java.util.Arrays;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverAgent implements Subscriber<String[]>{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ReceiverAgent.class);
	
	private Subscription subscription;

	private static String myId;
	
	public ReceiverAgent(String newId) {
		myId = newId;
	}
	
	@Override
	public void onSubscribe(Subscription newsub) {
		LOGGER.debug(myId+" subscribing "+newsub.toString());
		this.subscription = newsub;
		this.subscription.request(1);
		
	}

	@Override
	public void onNext(String[] item) {
		LOGGER.debug("Received "+Arrays.deepToString(item));
		if(item[0] != myId) {
			LOGGER.info("Ignoring "+Arrays.deepToString(item));
			return;
		}
		LOGGER.debug("Processed.");
		
	}

	@Override
	public void onError(Throwable throwable) {
		LOGGER.error("Exception "+throwable.getMessage());
		throwable.printStackTrace();
	}

	@Override
	public void onComplete() {
		LOGGER.debug("Completed!");
	}

}
