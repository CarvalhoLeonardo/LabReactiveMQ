package leonardo.github.study.reactivemq;

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 *         Based on a <a href="http://zguide.zeromq.org/java:asyncsrv">Zero MQ Example</a>
 *
 */

public class ReactiveMQ {
  private static String FRONTADDRESS = "ipc://" + UUID.randomUUID().toString();
  static Random rand = new Random(System.nanoTime());
  private static MessageEchoer echoServer;
  private final static Logger LOGGER = LoggerFactory.getLogger(ReactiveMQ.class);


  static ExecutorService executor = Executors.newFixedThreadPool(2);

  static {
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());
  }


  public static void main(String[] args) throws InterruptedException, ExecutionException {
    int parallelFactor;
    int messageCount;
    int messageLatency;
    if (args.length < 1) {
      parallelFactor = 4;
      messageCount = 100;
      messageLatency = 100;
    } else {
      
      if (args.length != 3) {
        throw new IllegalArgumentException("I need 0 or 3 parametes : <parallelism> <how many messages> <messages latency>");
      }
      parallelFactor =  Integer.parseInt(args[0]);
      messageCount = Integer.parseInt(args[1]);
      messageLatency = Integer.parseInt(args[2]);
      
    }
    echoServer = new MessageEchoer(FRONTADDRESS, parallelFactor);
    executor.submit(echoServer);

    MessageSender mesgGen = new MessageSender(200, 5000, FRONTADDRESS, messageCount, messageLatency, parallelFactor);

    long startTime = Calendar.getInstance().getTimeInMillis();
    mesgGen.run();
    long endTime = Calendar.getInstance().getTimeInMillis();

    Double totalRunTime = Double.valueOf(endTime-startTime)/1000;
    Double creationRunTime = Double.valueOf((mesgGen.getEndGenerating() - startTime));
    Double sendRunTime = Double.valueOf((mesgGen.getEndSending() - startTime)/1000);
    
    LOGGER.debug("totalRunTime : " + totalRunTime);
    LOGGER.debug("creationRunTime : " + creationRunTime);
    LOGGER.debug("sendRunTime : " + sendRunTime);
 
    DecimalFormat decimalFormat = new DecimalFormat("###.###");
    String logMesgFormat = "Messages %s : %d -- aprox %s /s";

    long effectiveSentMessages = MessageSender.messagesSizes.stream().count();
    LOGGER.error(String.format(logMesgFormat, "generated", effectiveSentMessages, decimalFormat.format(effectiveSentMessages / creationRunTime * 1000)));
    LOGGER.error(String.format(logMesgFormat, "sent", effectiveSentMessages, decimalFormat.format(effectiveSentMessages / sendRunTime)));
    LOGGER.error(String.format(logMesgFormat, "echoed", echoServer.getEchoCounter(), decimalFormat.format(echoServer.getEchoCounter() / totalRunTime)));
    
    LOGGER
        .error("Minor size : " + MessageSender.messagesSizes.stream().min(Integer::compare).get());
    LOGGER.error("Mean size : " + MessageSender.messagesSizes.stream().mapToDouble(i -> {
      return Integer.valueOf(i).doubleValue();
    }).average().getAsDouble());

    LOGGER.error(
        "Biggest size : " + MessageSender.messagesSizes.stream().max(Integer::compare).get());

    
    LOGGER.debug("executor.shutdownNow()");
    executor.awaitTermination(1, TimeUnit.SECONDS);
   
    LOGGER.debug("fakeServer.shutdownNow()");
    echoServer.testEndOfOperation(true);
    
  }
}
