package leonardo.github.study.reactivemq;

import java.util.Random;
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
  private static final int TIMERUNNIG = 5;
  private static String ADDRESS = "ipc://testsocket.ipc";
  static Random rand = new Random(System.nanoTime());
  private final static ZContext context = new ZContext();
  private static ServerAgent fakeServer = new ServerAgent(ADDRESS,context);
  private final static Logger LOGGER = LoggerFactory.getLogger(ReactiveMQ.class);

  public static final BlockingQueue<Runnable> actors = new ArrayBlockingQueue<Runnable>(20);

  public static final ThreadPoolExecutor GLOBAL_THREAD_POOL =
      new ThreadPoolExecutor(12, 20, 5, TimeUnit.SECONDS, actors);

  static {
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());
  }


  public static void main(String[] args) throws InterruptedException {
    

    GLOBAL_THREAD_POOL.execute(fakeServer);

    final MessageGenerator mesgGen = new MessageGenerator(ADDRESS);
    mesgGen.startSendind();

    Thread.sleep(5000);
    GLOBAL_THREAD_POOL.awaitTermination(1L,TimeUnit.SECONDS);
    GLOBAL_THREAD_POOL.shutdown();
    mesgGen.stopSending();
    context.close();
    context.destroy();
    
    
    long totalMessages = MessageGenerator.messagesSizes.stream().count();
    LOGGER.error(
        "Messages sent : " + totalMessages + " -- aprox " + (totalMessages / TIMERUNNIG) + "/s");
    LOGGER.error(
        "Minor size : " + MessageGenerator.messagesSizes.stream().min(Integer::compare).get());
    LOGGER.error("Mean size : " + MessageGenerator.messagesSizes.stream().mapToDouble(i -> {
      return Integer.valueOf(i).doubleValue();
    }).average().getAsDouble());
    LOGGER.error(
        "Biggest size : " + MessageGenerator.messagesSizes.stream().max(Integer::compare).get());
  }
}
