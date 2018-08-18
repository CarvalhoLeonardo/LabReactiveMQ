package leonardo.github.study.reactivemq;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
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
public class MessageEchoer implements Runnable {

  private final static Logger LOGGER = LoggerFactory.getLogger(MessageEchoer.class);
  private ZContext context;
  private String mqAddress;
  Socket serverSocket;
  Socket backendSocket;
  static AtomicInteger echoCounter = new AtomicInteger(0);
  
  ZLoop looper;
  ExecutorService localExecutor;
  private WorkQueueProcessor<ZMsg> echoProcessor;
  ParallelFlux<ZMsg> parallelEchoer;
  private int pFactor;
  private List<Disposable> workers = new ArrayList<>();
  private List<Socket> workersSockets = new ArrayList<>();
  private List<PollItem> pollers = new ArrayList<>();
  
  private static String BACKADDRESS = "ipc://"+UUID.randomUUID().toString();


  public MessageEchoer(String mqAddress, int parallelFactor, ZContext shadowCtx) {
    super();
    this.context = shadowCtx;
    this.mqAddress = mqAddress;
    this.pFactor = parallelFactor;

    localExecutor = Executors.newWorkStealingPool(parallelFactor + 2);

    echoProcessor = WorkQueueProcessor.<ZMsg>builder()
        .bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutor)
        .share(true)
        .build();
  }

  public final int getEchoCounter() {
    return echoCounter.get();
  }

  @Override
  public void run() {
    serverSocket = context.createSocket(ZMQ.ROUTER);
    backendSocket = context.createSocket(ZMQ.DEALER);
    
    serverSocket.setLinger(0);
    serverSocket.setImmediate(false);
    serverSocket.setIdentity("Server Socket".getBytes());

    backendSocket.setLinger(0);
    backendSocket.setImmediate(false);
    backendSocket.setIdentity("Backend Socket".getBytes());
    
    if (serverSocket.bind(mqAddress))
      LOGGER.debug("Server bound to " + mqAddress);

    if (backendSocket .bind(BACKADDRESS))
      LOGGER.debug("Backend bound to " + BACKADDRESS);
    
    looper = new ZLoop(context);

    
   parallelEchoer =  echoProcessor
      .parallel(pFactor)
      .runOn(Schedulers.parallel());
   
   IntStream.range(1, pFactor + 1).forEach( n -> {
     LOGGER.debug("Creating individual Flux "+n);
     Socket worker = context.createSocket(ZMQ.DEALER);
     workersSockets.add(worker);
     worker.setLinger(0);
     worker.setImmediate(false);
     worker.setIdentity(("worker "+n).getBytes());
     worker.connect(BACKADDRESS);
     pollers.add(0,new PollItem(worker, ZMQ.Poller.POLLIN));
     
     Flux<ZMsg> individualFlux = Flux.<ZMsg>create(receiver -> {
       InternalHandler mesgReceiver = new InternalHandler(receiver);
       looper.addPoller(pollers.get(0), mesgReceiver, null);
     });
     
     workers.add(
         individualFlux.subscribe(echoMessage(n,worker)));
    
     Flux.<ZMsg>merge(parallelEchoer, individualFlux);
     LOGGER.debug("Created individual Flux "+n);
   });
   
   parallelEchoer.subscribe(echoProcessor);
   workers.add(echoProcessor);
   

   localExecutor.submit(() ->{
     LOGGER.debug("Starting to proxy... ");
     ZMQ.proxy(serverSocket, backendSocket, null);
     LOGGER.debug("Proxy ended");
   });

   localExecutor.submit(() ->{
     LOGGER.debug("Starting to poll... ");
     looper.start();
     LOGGER.debug("Polling ended.");
  });
   
   try {
    Thread.currentThread().join();
  } catch (InterruptedException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
  }
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

  private Consumer<ZMsg> echoMessage(int id, Socket echoSocket) {

    return new Consumer<ZMsg>() {
      String myName = "Echoer " + id;
      ZFrame identity;

      @Override
      public void accept(ZMsg mesg) {
        LOGGER.debug(myName +" -- HANDLE :: " + echoCounter.get());
        if (LOGGER.isTraceEnabled())
          mesg.dump(System.out);
        
        identity = mesg.getFirst().duplicate();
        
        if (identity == null) {
          LOGGER.error("Received null Socket ID ...");
          return;
        }
        LOGGER.debug("Received the Socket ID " + identity.toString());

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
  
  public void endOfOperation() {
      LOGGER.info(" Time to go - shutting down at counter "+echoCounter.get());
      
      LOGGER.info("localExecutor.shutdownNow()");
      try {
        localExecutor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        localExecutor.shutdownNow();
      }
      
      localExecutor.shutdown();
      
      LOGGER.debug("echoProcessor.awaitAndShutdown()");
      echoProcessor.awaitAndShutdown();
      LOGGER.debug("echoProcessor -- Shutdown");

      LOGGER.debug("Unbinding sockets...");
      serverSocket.unbind(mqAddress);
      backendSocket.unbind(BACKADDRESS);
      
      LOGGER.debug("Destroying sockets...");
      context.destroySocket(backendSocket);
      context.destroySocket(serverSocket);
      LOGGER.debug("Sockets closed.");
      
      LOGGER.debug("Context destroy().");
      context.destroy();
      LOGGER.debug("Context destroyed.");
      
  }

}
