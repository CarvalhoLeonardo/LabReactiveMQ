package leonardo.github.study.reactivemq;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
public class MessageSender implements Runnable {

  private static final int fileSize;
  private static int minLength = 100;
  private static int maxLength;
  static Random rand = new Random(System.nanoTime());
  private final static Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);
  private static ByteBuffer sourceData;
  private static String addressMQ;
  public static ArrayList<Integer> messagesSizes = new ArrayList<>();
  public static AtomicInteger answerCounter = new AtomicInteger(0);
  public static AtomicInteger dropCounter = new AtomicInteger(0);
  public static AtomicInteger sentCounter = new AtomicInteger(0);

  private static ZContext context;
  private int numberOfMessages;
  private int milissecondsLatency;
  private int pFactor;
  
  private long endGenerating;
  private long endSending;
  
  ZLoop looper;
  Socket client;
  String identity = "Sender Agent";
  ExecutorService localExecutorService;
  PollItem myReceiver;

  MessageReceiver mesgReceiver;
  Flux<ByteBuffer> fluxEmitter;

  private WorkQueueProcessor<ByteBuffer> senderProcessor;
  private WorkQueueProcessor<ByteBuffer> receiverProcessor;

  // https://www.programcreek.com/java-api-examples/?code=reactor/reactor-core/reactor-core-master/reactor-core/src/test/java/reactor/core/publisher/scenarios/BurstyWorkQueueProcessorTests.java

  static {
    File randomDataFile =
        new File(MessageSender.class.getClassLoader().getResource("seed.bin").getFile());
    fileSize = Long.valueOf(randomDataFile.length()).intValue();
    try {
      InputStream input = new BufferedInputStream(new FileInputStream(randomDataFile));
      sourceData = ByteBuffer.allocate(fileSize);
      sourceData.put(input.readAllBytes());
      input.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Loggers.useConsoleLoggers();
    
  }

  public MessageSender(String address, int messagesToSend, int latency, int parallelism, ZContext ctx) {
    LOGGER.debug("MessageGenerator() File Size : " + fileSize);
    LOGGER.debug("MessageGenerator() Address : " + address);
    MessageSender.addressMQ = address;
    MessageSender.maxLength = 5000;
    numberOfMessages = messagesToSend;
    milissecondsLatency = latency;
    pFactor = parallelism;
    context = ctx;
  }

  public MessageSender(int minLength, int maxLength, String address, int messagesToSend,int latency, int parallelism, ZContext ctx) {
    this(address ,messagesToSend, latency, parallelism,ctx);
    MessageSender.minLength = minLength;
    MessageSender.maxLength = maxLength;
  }

  private void setUp() {
    
    client = context.createSocket(ZMQ.DEALER);
    client.setIdentity(identity.getBytes());
    client.setLinger(0);
    localExecutorService = Executors.newWorkStealingPool(pFactor * 2);
    
    senderProcessor = WorkQueueProcessor.<ByteBuffer>builder()
        .bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutorService)
        .share(true)
        .build();
    
    receiverProcessor = WorkQueueProcessor.<ByteBuffer>builder()
        .bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutorService)
        .share(true)
        .build();
    
    if (client.connect(addressMQ)) {
      LOGGER.info(identity + " connected to " + addressMQ);
    }
  }

  public final long getEndGenerating() {
    return endGenerating;
  }

  public final long getEndSending() {
    return endSending;
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
      LOGGER.debug("MessageReceiver :: Handle");
      reply = ZMsg.recvMsg(item.getSocket());
      address = reply.pop();
      LOGGER.debug("MessageReceiver :: received from " + address.toString());
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
        if (sentCounter.get() == numberOfMessages){
          LOGGER.debug("sender.complete()");
          endSending = Calendar.getInstance().getTimeInMillis();
        }
      }
    };
  }

  private Consumer<ByteBuffer> repliesReceiver() {

    return new Consumer<ByteBuffer>() {
      @Override
      public void accept(ByteBuffer item) {
        LOGGER.debug("END -  receiveMessage : "+MessageSender.answerCounter.incrementAndGet());
        if (MessageSender.answerCounter.get() == numberOfMessages) {
          LOGGER.debug("THE END");
          stop();
        }
      }
    };
  }

  @Override
  public void run() {
    setUp();

    myReceiver = new PollItem(client, ZMQ.Poller.POLLIN);
    looper = new ZLoop(context);

    fluxEmitter = Flux.<ByteBuffer>create(emitter -> {
      final ByteBuffer myDataSource = sourceData.asReadOnlyBuffer();
      byte[] result;
      int offset;
      int lenght;
      
      while (sentCounter.get() < numberOfMessages) {
        sentCounter.incrementAndGet();
        LOGGER.debug("messageProducer counter : "+sentCounter.get());
        lenght = MessageSender.minLength
            + rand.nextInt(MessageSender.maxLength - MessageSender.minLength);
        offset = rand.nextInt(fileSize - lenght);
        myDataSource.position(offset);
        result = new byte[lenght];
        myDataSource.position(rand.nextInt(fileSize - lenght));
        myDataSource.get(result);
        ByteBuffer envelope = ByteBuffer.wrap(result);
        emitter.next(envelope);
      }
      endGenerating = Calendar.getInstance().getTimeInMillis();
      emitter.complete();
      LOGGER.debug("emitter.complete()");

    });

    fluxEmitter
      .delayElements(Duration.ofMillis(milissecondsLatency))
      .subscribeOn(Schedulers.parallel())
      .subscribeWith(senderProcessor);
    
      
    
   senderProcessor.subscribeOn(Schedulers.parallel()).subscribe(getMessageSenderAgent(1));  
    

    Flux<ByteBuffer> fluxReceiver = Flux.<ByteBuffer>create(receiver -> {
      mesgReceiver = new MessageReceiver(receiver);
      looper.addPoller(myReceiver, mesgReceiver, null);
    });
    
    fluxReceiver
      .share()
      .subscribeWith(receiverProcessor);

    ParallelFlux<ByteBuffer> parallelReceiver = receiverProcessor
        .parallel(pFactor)
        .runOn(Schedulers.parallel());

    
   parallelReceiver
        .subscribe(repliesReceiver());
    
    LOGGER.debug("Starting to poll...");
    looper.start();
    LOGGER.debug("Polling stopped.");
  }

  public void stop() {
    
    LOGGER.debug("senderProcessor.awaitAndShutdown()");
    senderProcessor.awaitAndShutdown();
    LOGGER.debug("senderProcessor -- Shutdown");
    
    LOGGER.debug("receiverProcessor.awaitAndShutdown()");
    receiverProcessor.awaitAndShutdown();
    LOGGER.debug("receiverProcessor -- Shutdown");
    
    LOGGER.debug("Loop :: removePoller");
    looper.removePoller(myReceiver);
    
    LOGGER.debug("Looper :: destroy");
    looper.destroy();
    
    LOGGER.debug("Context :: destroy");
    context.destroy();
    LOGGER.debug("Context :: destroyed");
    
  }



}

