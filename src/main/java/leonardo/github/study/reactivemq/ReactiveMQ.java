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
  private static ZContext context;
  private static ServerAgent fakeServer;
  private final static Logger LOGGER = LoggerFactory.getLogger(ReactiveMQ.class);

  private static BlockingQueue<Runnable> actors;

  public static ThreadPoolExecutor GLOBAL_THREAD_POOL;

  static {
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());
  }


  public static void main(String[] args) throws InterruptedException {
    int AGENTS_COUNT = 10;
    actors = new ArrayBlockingQueue<Runnable>(AGENTS_COUNT + 5);

    GLOBAL_THREAD_POOL = new ThreadPoolExecutor(AGENTS_COUNT + 2, AGENTS_COUNT + 10, 5, TimeUnit.SECONDS, actors);

    context = new ZContext(AGENTS_COUNT + 2);
    fakeServer = new ServerAgent(ADDRESS, context);
    GLOBAL_THREAD_POOL.execute(fakeServer);

    final MessageGenerator mesgGen = new MessageGenerator(200, 5000, AGENTS_COUNT, ADDRESS);
    mesgGen.startSendind();

    Thread.sleep(5000);
    GLOBAL_THREAD_POOL.awaitTermination(1L, TimeUnit.SECONDS);
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
