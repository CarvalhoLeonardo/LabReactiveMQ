package leonardo.github.study.reactivemq;

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
public class GeneratorAgent implements Runnable {

  
  private final static Logger LOGGER = LoggerFactory.getLogger(GeneratorAgent.class);
  private ZContext context;
  private String mqAddress;
  Socket serverSocket;

  public GeneratorAgent(String mqAddress,ZContext context) {
    super();
    this.context = context;
    this.mqAddress = mqAddress;
  }


  @Override
  public void run() {
    serverSocket = context.createSocket(ZMQ.ROUTER);

    if (serverSocket.bind(mqAddress))
      LOGGER.debug("Bound to " + mqAddress);
    serverSocket.setLinger(0);
    ZLoop looper = new ZLoop(context);
    PollItem pooler = new PollItem(serverSocket, ZMQ.Poller.POLLIN);
    looper.addPoller(pooler, new InternalHandler(), null);

    LOGGER.debug("Starting to pool... ");

    looper.start();

  }

  private class InternalHandler implements IZLoopHandler {

    @Override
    public int handle(ZLoop loop, PollItem item, Object arg) {
      LOGGER.debug("HANDLE :: ");
      ZMsg firstMessage = ZMsg.recvMsg(item.getSocket());
      LOGGER.debug("RECEIVE :: ");
      ZFrame identity = firstMessage.pop();
      LOGGER.debug("Received the Validator ID " + identity.toString());

      LOGGER.debug("Received the Message " + firstMessage.toString());
      ZMsg responseMessage = new ZMsg();
      ZFrame responseFrame = new ZFrame("Helllo "+identity.toString());
      responseMessage.add(responseFrame);
      responseMessage.wrap(identity);
      LOGGER.debug("Trying to answer... ");
      if (responseMessage.send(item.getSocket())) {
        LOGGER.debug("Answer sent!");
      }

      return 0;
    }

  }

}
