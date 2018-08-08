package leonardo.github.study.reactivemq;

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

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
  private static ZContext context;
  private static MessageEchoer fakeServer;
  private final static Logger LOGGER = LoggerFactory.getLogger(ReactiveMQ.class);


  static ExecutorService executor = Executors.newFixedThreadPool(2);

  static {
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());
  }


  public static void main(String[] args) throws InterruptedException {
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
    context = new ZContext();
    fakeServer = new MessageEchoer(FRONTADDRESS, context, parallelFactor);
    executor.execute(() ->{
      fakeServer.run(); 
    });

    MessageSender mesgGen = new MessageSender(200, 5000, FRONTADDRESS, messageCount, messageLatency, parallelFactor);

    long startTime = Calendar.getInstance().getTimeInMillis();
    mesgGen.run();
    long endTime = Calendar.getInstance().getTimeInMillis();

    long totalRunTime = (endTime-startTime)/1000;
    long creationRunTime = (mesgGen.getEndGenerating() - startTime);
    long sendRunTime = (mesgGen.getEndSending() - startTime)/1000;
    
    LOGGER.debug("fakeServer.shutdownNow()");
    fakeServer.testEndOfOperation(true);
    
    LOGGER.debug("executor.shutdownNow()");
    executor.shutdown();
    
    DecimalFormat decimalFormat = new DecimalFormat("#,##0.00000");
    String logMesgFormat = "Messages %s : %d -- aprox %s /s";

    long totalMessages = MessageSender.messagesSizes.stream().count();
    LOGGER.error(String.format(logMesgFormat, "generated", totalMessages, decimalFormat.format(totalMessages / creationRunTime)));
    LOGGER.error(String.format(logMesgFormat, "sent", totalMessages, decimalFormat.format(totalMessages / sendRunTime)));
    LOGGER.error(String.format(logMesgFormat, "echoed", fakeServer.getEchoCounter(), decimalFormat.format(fakeServer.getEchoCounter() / totalRunTime)));
    
    LOGGER
        .error("Minor size : " + MessageSender.messagesSizes.stream().min(Integer::compare).get());
    LOGGER.error("Mean size : " + MessageSender.messagesSizes.stream().mapToDouble(i -> {
      return Integer.valueOf(i).doubleValue();
    }).average().getAsDouble());

    LOGGER.error(
        "Biggest size : " + MessageSender.messagesSizes.stream().max(Integer::compare).get());

  }
}
