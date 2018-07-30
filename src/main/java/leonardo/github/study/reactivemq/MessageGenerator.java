package leonardo.github.study.reactivemq;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ParallelFlux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

/**
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         The class will generate random messages, sendo to the "frontend", and wait for a
 *         response.
 * 
 *
 */
public class MessageGenerator implements Runnable {

  private static final int fileSize;
  private static int minLength = 100;
  private static int maxLength;
  private static int NR_SENDING_AGENTS;
  static Random rand = new Random(System.nanoTime());
  private final static Logger LOGGER = LoggerFactory.getLogger(MessageGenerator.class);
  private static ByteBuffer sourceData;
  private static String addressMQ;
  public static ArrayList<Integer> messagesSizes = new ArrayList<>();
  public static AtomicInteger answerCounter = new AtomicInteger(0);
  public static AtomicInteger dropCounter = new AtomicInteger(0);

  private static ZContext context;
  private static int latencyInMilisseconds = 100;
  private static boolean keepSending = true;
  ZLoop looper;
  Socket client;
  String identity = "Sender Agent";
  ExecutorService localExecutor =
      Executors.newCachedThreadPool(ReactiveMQ.GLOBAL_THREAD_POOL.getThreadFactory());
  PollItem myReceiver;

  MessageReceiver mesgReceiver;
  Flux<ByteBuffer> fluxEmitter;

  private WorkQueueProcessor<ByteBuffer> senderProcessor = WorkQueueProcessor.<ByteBuffer>builder()
      .bufferSize(Queues.SMALL_BUFFER_SIZE).executor(localExecutor).build();
  private WorkQueueProcessor<ByteBuffer> receiverProcessor = WorkQueueProcessor
      .<ByteBuffer>builder().bufferSize(Queues.SMALL_BUFFER_SIZE).executor(localExecutor).build();

  // https://www.programcreek.com/java-api-examples/?code=reactor/reactor-core/reactor-core-master/reactor-core/src/test/java/reactor/core/publisher/scenarios/BurstyWorkQueueProcessorTests.java

  static {
    File randomDataFile =
        new File(MessageGenerator.class.getClassLoader().getResource("seed.bin").getFile());
    fileSize = Long.valueOf(randomDataFile.length()).intValue();
    try {
      InputStream input = new BufferedInputStream(new FileInputStream(randomDataFile));
      sourceData = ByteBuffer.allocate(fileSize);
      sourceData.put(input.readAllBytes());
      input.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    Loggers.useConsoleLoggers();
    
  }

  private void setUp() {
    context = new ZContext(MessageGenerator.NR_SENDING_AGENTS + 2);
    client = context.createSocket(ZMQ.DEALER);
    client.setIdentity(identity.getBytes());
    client.setLinger(0);
    client.setBacklog(10000);

    if (client.connect(addressMQ)) {
      LOGGER.info(identity + " connected to " + addressMQ);
    }


  }

  public MessageGenerator(String address) {
    LOGGER.debug("MessageGenerator() File Size : " + fileSize);
    LOGGER.debug("MessageGenerator() Address : " + address);
    MessageGenerator.addressMQ = address;
    MessageGenerator.maxLength = 5000;
    MessageGenerator.NR_SENDING_AGENTS = 5;
  }

  public MessageGenerator(int minLength, int maxLength, int sendAgs, String address) {
    this(address);
    MessageGenerator.minLength = minLength;
    MessageGenerator.maxLength = maxLength;
    MessageGenerator.NR_SENDING_AGENTS = sendAgs;
    LOGGER.debug("MessageGenerator() Agents : " + NR_SENDING_AGENTS);
  }

  private class MessageReceiver implements IZLoopHandler {

    FluxSink<ByteBuffer> externalMessageSink;
    ZMsg reply;
    ZFrame address;

    public MessageReceiver(FluxSink<ByteBuffer> receiver) {
      this.externalMessageSink = receiver;
    }

    @Override
    public int handle(ZLoop loop, PollItem item, Object arg) {
      if (Thread.currentThread().isInterrupted()) {
        LOGGER.debug("MessageReceiver :: Interrupted");
        this.externalMessageSink.complete();
        stopSending();
        return 0;
      }
      LOGGER.debug("MessageReceiver :: Handle");
      reply = ZMsg.recvMsg(item.getSocket());
      address = reply.pop();
      // LOGGER.debug("MessageReceiver :: received : " + reply);
      this.externalMessageSink.next(ByteBuffer.wrap(reply.pop().getData()));
      address.destroy();
      reply.destroy();
      return 0;
    }

  }

  private Consumer<ByteBuffer> getMessageSenderAgent(int id) {
    return new Consumer<ByteBuffer>() {
      String myName = "Sender "+id;
      ZMsg responseMessage;
      ZFrame adrressing = new ZFrame(client.getIdentity());
      ZFrame responseFrame;
      byte[] myId = {Integer.valueOf(id).byteValue()};

      @Override
      public void accept(ByteBuffer dataHolder) {
        LOGGER.debug(myName + " Sending...");
        responseMessage = new ZMsg();
        responseFrame = new ZFrame(dataHolder.array());
        responseMessage.offer(responseFrame);
        responseFrame = new ZFrame(myId);
        responseMessage.offer(responseFrame);
        responseMessage.offer(adrressing);
        if (responseMessage.send(client)) {
          messagesSizes.add(dataHolder.limit());
          LOGGER.debug(myName + " MessageSender :: Sent " + messagesSizes.size());
        }
        responseMessage.clear();
        responseMessage.destroy();
      }
    };
  }

  private Consumer<ByteBuffer> repliesReceiver() {

    return new Consumer<ByteBuffer>() {

      @Override
      public void accept(ByteBuffer item) {
        LOGGER.debug(MessageGenerator.answerCounter.incrementAndGet() + " receiveMessage :: "
            + item.toString());

      }
    };
  }

  @Override
  public void run() {
    setUp();

    myReceiver = new PollItem(client, ZMQ.Poller.POLLIN);
    looper = new ZLoop(context);


    // for (int i = 0; i < MessageGenerator.NR_SENDING_AGENTS; i++)
    fluxEmitter = Flux.<ByteBuffer>create(emitter -> {
      final ByteBuffer myDataSource = sourceData.asReadOnlyBuffer();
      byte[] result;
      int offset;
      int lenght;
      
      while (! emitter.isCancelled() && keepSending) {
        lenght = MessageGenerator.minLength
            + rand.nextInt(MessageGenerator.maxLength - MessageGenerator.minLength);
        offset = rand.nextInt(fileSize - lenght);
        myDataSource.position(offset);
        result = new byte[lenght];
        myDataSource.position(rand.nextInt(fileSize - lenght));
        myDataSource.get(result);
        ByteBuffer envelope = ByteBuffer.wrap(result);
        emitter.next(envelope);
      }
      LOGGER.debug("messageProducer :: interrupted");
      emitter.complete();

    });

    fluxEmitter
       .sample(Duration.ofMillis(latencyInMilisseconds))
        // .log()
        // .delayElements(Duration.ofMillis(latencyInMilisseconds))  
      .subscribeOn(Schedulers.parallel())
      .subscribeWith(senderProcessor);
    
    ParallelFlux<ByteBuffer> parallelSender =  senderProcessor
        .share()
        .parallel(ReactiveMQ.AGENTS_COUNT)
        .runOn(Schedulers.parallel());
      
      for (int i = 0; i < ReactiveMQ.AGENTS_COUNT; i++) {
        parallelSender.subscribe(getMessageSenderAgent(i + 1));
      }
        

    Flux<ByteBuffer> fluxReceiver = Flux.<ByteBuffer>create(receiver -> {
      mesgReceiver = new MessageReceiver(receiver);
      looper.addPoller(myReceiver, mesgReceiver, null);
    });
    
    fluxReceiver
      .share()
      .subscribeOn(Schedulers.parallel())
      .subscribeWith(receiverProcessor);

    ParallelFlux<ByteBuffer> parallelReceiver = receiverProcessor
        .share()
        .parallel(ReactiveMQ.AGENTS_COUNT)
        .runOn(Schedulers.parallel());

    
    for (int i = 0; i < ReactiveMQ.AGENTS_COUNT; i++) {
      parallelReceiver
        .subscribe(repliesReceiver());
    }
    
    
    

    looper.start();

  }

  public void stopSending() {

    LOGGER.debug("Keepsending :: false");
    keepSending = false;

    LOGGER.error("Sender processor :: on queue : " + senderProcessor.getPending());
    LOGGER.debug("Sender processor :: destroy");
    senderProcessor.dispose();
    senderProcessor.forceShutdown();


    LOGGER.error("Receiver processor :: on queue : " + receiverProcessor.getPending());
    LOGGER.error("Receiver processor :: on flux : "
        + mesgReceiver.externalMessageSink.requestedFromDownstream());
    LOGGER.debug("Receiver processor :: destroy");
    mesgReceiver.externalMessageSink.complete();
    receiverProcessor.dispose();
    receiverProcessor.forceShutdown();

    localExecutor.shutdown();
    try {
      localExecutor.awaitTermination(1L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }

    LOGGER.debug("Loop :: destroy");
    looper.removePoller(myReceiver);
    myReceiver.getSocket().close();
    looper.destroy();

    LOGGER.debug("Socket :: disconnect/close");
    context.destroySocket(client);
    client.close();



    LOGGER.debug("Context :: destroy");
    context.close();
    context.destroy();

  }



}

