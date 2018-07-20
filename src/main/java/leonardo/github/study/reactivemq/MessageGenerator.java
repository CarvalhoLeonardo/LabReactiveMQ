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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread;

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

  public static ZContext context = new ZContext();

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

  private class MessageSender implements Runnable {

    private ByteBuffer myDataSource;
    String identity;
    
    ZMsg responseMessage;
    ZFrame adrressing;
    ZFrame responseFrame;
    
    public MessageSender(int userId) {
      super();
      this.myDataSource = sourceData.asReadOnlyBuffer();
      this.identity = "Sender Agent "+userId;
    }

    private byte[] getRandomData(int lenght) {
      byte[] result = new byte[lenght];
      if (LOGGER.isDebugEnabled()) {
        int offset = rand.nextInt(fileSize - lenght);
        myDataSource.position(offset);
        LOGGER.debug(" Reading " + lenght + " from position " + offset + " on size " + fileSize);
        myDataSource.get(result);
      } else {
        myDataSource.position(rand.nextInt(fileSize - lenght));
        myDataSource.get(result);
      }
      messagesSizes.add(result.length);
      return result;
    }
    
    @Override
    public void run() {

      Socket client = context.createSocket(ZMQ.DEALER);
      client.setIdentity(identity.getBytes());
      client.setLinger(0);

      if (client.connect(addressMQ)) {
        LOGGER.info(identity + " connected to " + addressMQ);
      }

      while (!Thread.currentThread().isInterrupted()) {
        LOGGER.debug(identity + " Sending...");
        responseMessage = new ZMsg();
        adrressing = new ZFrame(client.getIdentity());
        responseFrame = new ZFrame(getRandomData(MessageGenerator.minLength
            + rand.nextInt(MessageGenerator.maxLength - MessageGenerator.minLength)));
        responseMessage.add(responseFrame);
        responseMessage.wrap(adrressing);
        if (responseMessage.send(client)) {
          LOGGER.debug(identity + " Sent!");
        }
        ZMsg reply = ZMsg.recvMsg(client);
        reply.pop();
        LOGGER.debug(identity + " received : " + reply);
        
      }
      context.close();
    }
  }
  
  public void startSendind() {
    for (int i = 0; i < NR_SENDING_AGENTS; i++)
      ReactiveMQ.GLOBAL_THREAD_POOL.submit(new MessageSender(i+1));
  }
}
