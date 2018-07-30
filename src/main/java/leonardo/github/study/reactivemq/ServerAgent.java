package leonardo.github.study.reactivemq;

import java.util.ArrayList;
import java.util.List;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ParallelFlux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

/**
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         This implementation will be based in ...
 *
 */
public class ServerAgent implements Runnable {

  private final static Logger LOGGER = LoggerFactory.getLogger(ServerAgent.class);
  private ZContext context;
  private String mqAddress;
  Socket serverSocket;
  static AtomicInteger echoCounter = new AtomicInteger(0);
  static AtomicInteger dropCounter = new AtomicInteger(0);
  InternalHandler mesgReceiver;
  ZLoop looper;
  PollItem pooler;
  ExecutorService localExecutor;
  private WorkQueueProcessor<ZMsg> echoProcessor;
  private List<Disposable> workers = new ArrayList<>();


  public ServerAgent(String mqAddress, ZContext context) {
    super();
    this.context = context;
    this.mqAddress = mqAddress;

    localExecutor = Executors.newWorkStealingPool(ReactiveMQ.AGENTS_COUNT);

    echoProcessor = WorkQueueProcessor.<ZMsg>builder()
        .bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutor)
        .share(true)
        .waitStrategy(WaitStrategy.yielding())
        .build();

  }



  public final int getEchoCounter() {
    return echoCounter.get();
  }

  @Override
  public void run() {
    serverSocket = context.createSocket(ZMQ.ROUTER);

    serverSocket.setLinger(0);
    serverSocket.setImmediate(false);

    if (serverSocket.bind(mqAddress))
      LOGGER.debug("Bound to " + mqAddress);


    looper = new ZLoop(context);
    pooler = new PollItem(serverSocket, ZMQ.Poller.POLLIN);

    
    Flux<ZMsg> echoer = Flux.<ZMsg>create(receiver -> {
      mesgReceiver = new InternalHandler(receiver);
      looper.addPoller(pooler, mesgReceiver, null);
    }).share();
    
    echoer
      .subscribe(echoProcessor);
    
    
    
    /*echoer
      .subscribeWith(echoProcessor)
      .share()
      .parallel()
      .runOn(Schedulers.parallel());
      
      messageSender = new Consumer<ByteBuffer>() {
      
      final ZFrame adrressing = new ZFrame(client.getIdentity());
      
      @Override
      public void accept(ByteBuffer dataHolder) {
        LOGGER.debug("Sending...");
        dataHolder.put(Integer.valueOf(messagesSizes.size()).byteValue());
        ZFrame responseFrame= new ZFrame(dataHolder.array());
        ZMsg responseMessage = new ZMsg();
        if (! responseMessage.offer(responseFrame)) {
          LOGGER.error(" MessageSender :: frame error ");
        }
        responseMessage.offer(adrressing);
        if (responseMessage.send(client)) {
          messagesSizes.add(dataHolder.remaining());
          LOGGER.debug(" MessageSender :: Sent " + messagesSizes.size());
          LOGGER.debug(" MessageSender :: Size " + dataHolder.remaining());
          responseMessage.dump(System.out);
        }
        responseMessage.clear();
        responseMessage.destroy();
      }
  };


    workers.add(echoer
        .subscribeOn(Schedulers.parallel(),false)
        .subscribe(echoMessage(1)));

*/
    ParallelFlux<ZMsg> parallelEchoer =  echoProcessor
      .share()
      .parallel()
      .runOn(Schedulers.parallel());
    
    for (int i = 0; i < ReactiveMQ.AGENTS_COUNT; i++) {
      workers.add(
          parallelEchoer.subscribe(echoMessage(i + 1)));
    }


    LOGGER.debug("Starting to pool... ");

    looper.start();


  }

  private class InternalHandler implements IZLoopHandler {
    FluxSink<ZMsg> emitter;
    
    public InternalHandler(FluxSink<ZMsg> emitter) {
      super();
      this.emitter = emitter;
    }

    @Override
    public int handle(ZLoop loop, PollItem item, Object arg) {
      LOGGER.debug("InternalHandler -- HANDLE");
      ZMsg incoming = ZMsg.recvMsg(item.getSocket());
      LOGGER.debug("InternalHandler -- RECEIVED "+incoming.peekFirst().toString());
      emitter.next(incoming.duplicate());
      incoming.destroy();
      incoming = null;
      return 0;
    }


  }

  private Consumer<ZMsg> echoMessage(int id) {

    return new Consumer<ZMsg>() {
      String myName = "Echoer " + id;
      ZFrame identity;

      @Override
      public void accept(ZMsg mesg) {
        LOGGER.debug(myName +" -- HANDLE :: " + echoCounter.get());
        mesg.dump(System.out);
        identity = mesg.getFirst().duplicate();
        
        if (identity == null) {
          LOGGER.warn("Received null Validator ID ...");
          return;
        }
        LOGGER.debug("Received the Validator ID " + identity.toString());

        mesg.getLast().reset("Hello " + identity.toString());
        LOGGER.debug(echoCounter.incrementAndGet() + " -- Trying to echo... ");
        if (mesg.send(serverSocket)) {
          LOGGER.debug(myName + " -- Echoed " + echoCounter.get() + " !");
        }
        identity.destroy();
        mesg.destroy();

      }

    };



  }

}
