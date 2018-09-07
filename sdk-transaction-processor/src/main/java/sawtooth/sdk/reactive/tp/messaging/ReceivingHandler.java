package sawtooth.sdk.reactive.tp.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMsg;
import reactor.core.publisher.EmitterProcessor;
import sawtooth.sdk.protobuf.Message;

/**
 * 
 * This class will route the received message, asynchronously, to the Flux publishing it
 *
 */

public class ReceivingHandler implements ZLoop.IZLoopHandler {

  protected final EmitterProcessor<Message> recEmitter;
  protected String idReceiver;
  protected String prefixId;
  private final static Logger LOGGER = LoggerFactory.getLogger(ReceivingHandler.class);
  final Map<String,String> addressCorrelationMapping;

  public ReceivingHandler(EmitterProcessor<Message> recEmitter, String prefixId, int idSender, Map<String,String> externaCorrelationMapping) {
    super();
    this.recEmitter = recEmitter;
    this.idReceiver = prefixId+"_RecHandler_"+idSender;
    this.addressCorrelationMapping = externaCorrelationMapping;
    LOGGER.debug("Created " + this.idReceiver);
    
  }

  @Override
  public int handle(ZLoop loop, PollItem item, Object arg) {
    LOGGER.debug(this.idReceiver+" handle()");
    ZMsg receivedMessage = ZMsg.recvMsg(item.getSocket());
    if (! receivedMessage.getLast().hasData()) {
      // ROUTER_PROBE message? We hope so.
      LOGGER.debug("{} Probable ROUTER_PROBE from {}, answering....",this.idReceiver,receivedMessage.getFirst().getString(StandardCharsets.UTF_8));
      receivedMessage.send(item.getSocket(),true);
      return 0;
    }
    
    if (LOGGER.isDebugEnabled())
      receivedMessage.dump(System.out);
    
    LOGGER.debug("{} Received the Validator ID {} on socket {}.",this.idReceiver,receivedMessage.pop().getData(),new String(item.getSocket().getIdentity()));
    Message sawtoothMessage;
    try {
      LOGGER.debug("Parsing message with {} frames and {} bytes...",receivedMessage.size(),receivedMessage.contentSize());
      Iterator<ZFrame> frames = receivedMessage.iterator();
      ByteArrayOutputStream bbuffer = new ByteArrayOutputStream();
      while (frames.hasNext()) {
          ZFrame frame = frames.next();
          byte[] frameData = frame.getData();
          bbuffer.write(frameData);
      }
      LOGGER.debug("Parsed.");
      sawtoothMessage = Message.parseFrom(bbuffer.toByteArray());
      addressCorrelationMapping.put(sawtoothMessage.getCorrelationId(),idReceiver);

      LOGGER.debug(this.idReceiver+" Emitting sawtooth Message "+ sawtoothMessage.toString());
      recEmitter.onNext(sawtoothMessage);
      LOGGER.debug(this.idReceiver+" emitted to "+recEmitter.name());

    } catch (IOException e) {
      LOGGER.error("Error on working the message.");
      e.printStackTrace();
      return -1;
    }
    return 0;
  }
}

