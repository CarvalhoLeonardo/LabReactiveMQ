package leonardo.github.study.reactivemq;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
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
import reactor.util.Loggers;

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
 */
public class MessageGenerator {

  private static final int fileSize;
  private static int minLength = 100;
  private static int maxLength = 5000;
  private int NR_SENDING_AGENTS = 5;
  static Random rand = new Random(System.nanoTime());
  private final static Logger LOGGER = LoggerFactory.getLogger(MessageGenerator.class);
  private static ByteBuffer sourceData;
  private static String addressMQ;
  public static ArrayList<Integer> messagesSizes = new ArrayList<>();

  private final static ZContext context = new ZContext();
  private static boolean keepSending = true;

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

  public MessageGenerator(String address) {
    LOGGER.debug("MessageGenerator() File Size : " + fileSize);
    LOGGER.debug("MessageGenerator() Address : " + address);
    MessageGenerator.addressMQ = address;
  }

  public MessageGenerator(int minLength, int maxLength, int sendAgs, String address) {
    this(address);
    MessageGenerator.minLength = minLength;
    MessageGenerator.maxLength = maxLength;
    NR_SENDING_AGENTS = sendAgs;
    LOGGER.debug("MessageGenerator() Agents : " + NR_SENDING_AGENTS);
  }

  private class MessageReceiver implements IZLoopHandler {

    @Override
    public int handle(ZLoop loop, PollItem item, Object arg) {
      LOGGER.debug("Handle()");
      ZMsg reply = ZMsg.recvMsg(item.getSocket());
      ZFrame address = reply.pop();
      LOGGER.debug(address.toString() + " received : " + reply);
      return 0;
    }

  }
  private class MessageSender implements Runnable {
    String identity;
    ZMsg responseMessage;
    ZFrame adrressing;
    ZFrame responseFrame;
    Socket client;
    ZLoop looper;

    public MessageSender(int userId) {
      super();
      this.identity = "Sender Agent " + userId;
      client = context.createSocket(ZMQ.DEALER);
      client.setIdentity(identity.getBytes());
      client.setLinger(0);
      if (client.connect(addressMQ)) {
        LOGGER.info(identity + " connected to " + addressMQ);
      }
      looper = new ZLoop(context);
      PollItem myReceiver = new PollItem(client, ZMQ.Poller.POLLIN);
      looper.addPoller(myReceiver, new MessageReceiver(), null);
    }

    @Override
    public void run() {
      getRandomDataGenerator().subscribe(new Consumer<ByteBuffer>() {
        @Override
        public void accept(ByteBuffer t) {
          LOGGER.debug(identity + " Sending...");
          responseMessage = new ZMsg();
          adrressing = new ZFrame(client.getIdentity());
          responseFrame = new ZFrame(t.array());
          responseMessage.add(responseFrame);
          responseMessage.wrap(adrressing);
          if (responseMessage.send(client)) {
            LOGGER.debug(identity + " Sent!");
          }
        }
      });
      looper.start();
    }
  }

  public void startSendind() {
    for (int i = 0; i < NR_SENDING_AGENTS; i++)
      ReactiveMQ.GLOBAL_THREAD_POOL.submit(new MessageSender(i + 1));
  }

  public void stopSending() {
    context.close();
    context.destroy();
    keepSending = false;
  }

  Flux<ByteBuffer> getRandomDataGenerator() {
    return Flux.<ByteBuffer>create(fluxSink -> {

      final ByteBuffer myDataSource = sourceData.asReadOnlyBuffer();
      byte[] result;
      int offset;
      int lenght;
      while (keepSending) {
        lenght = MessageGenerator.minLength
            + rand.nextInt(MessageGenerator.maxLength - MessageGenerator.minLength);
        offset = rand.nextInt(fileSize - lenght);
        myDataSource.position(offset);
        result = new byte[lenght];
        myDataSource.position(rand.nextInt(fileSize - lenght));
        myDataSource.get(result);
        ByteBuffer envelope = ByteBuffer.wrap(result);
        messagesSizes.add(lenght);
        fluxSink.next(envelope);
      }
      fluxSink.complete();
    }).publish().autoConnect();
  }
}

