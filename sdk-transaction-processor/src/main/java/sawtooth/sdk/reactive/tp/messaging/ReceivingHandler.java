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
import sawtooth.sdk.protobuf.Message.MessageType;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 */
public class ReceivingHandler implements ZLoop.IZLoopHandler {

  /**
   * Our ubiquitous Logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingHandler.class);

  /**
   * Map to hold the messages being processed destinations.
   */
  final Map<String, byte[]> addressCorrelationMapping;
  protected String idReceiver;
  protected String prefixId;
  /**
   * Incoming flux emitter.
   */
  protected final EmitterProcessor<Message> recEmitter;

  /**
   * Constructor that binds the identity and emitter to the worker unit.
   *
   * @param recEmitter
   * @param prefixId
   * @param idSender
   * @param externaCorrelationMapping
   */
  public ReceivingHandler(final EmitterProcessor<Message> recEmitter, final String prefixId,
      final int idSender, final Map<String, byte[]> externaCorrelationMapping) {
    super();
    this.recEmitter = recEmitter;
    this.idReceiver = prefixId + "_RecHandler_" + idSender;
    this.addressCorrelationMapping = externaCorrelationMapping;
    LOGGER.debug("Created " + this.idReceiver);

  }

  @Override
  public final int handle(final ZLoop loop, final PollItem item, final Object arg) {
    LOGGER.debug(this.idReceiver + " handle()");
    ZMsg receivedMessage = ZMsg.recvMsg(item.getSocket());

    if (LOGGER.isDebugEnabled()) {
      receivedMessage.dump(System.out);
      LOGGER.debug("Message Dump ###############################");
    }

    if (!receivedMessage.getLast().hasData()) {
      // ROUTER_PROBE message? We hope so.
      LOGGER.debug("{} Probable ROUTER_PROBE from {}, answering....", this.idReceiver,
          receivedMessage.getFirst().getString(StandardCharsets.UTF_8));
      receivedMessage.send(item.getSocket(), true);
      return 0;
    }

    byte[] routerID = receivedMessage.pop().getData();
    LOGGER.debug("{} Received the Validator ID {} on socket {}.", this.idReceiver, routerID,
        new String(item.getSocket().getIdentity()));
    Message sawtoothMessage;
    try {
      LOGGER.debug("Parsing message with {} frames and {} bytes...", receivedMessage.size(),
          receivedMessage.contentSize());
      Iterator<ZFrame> frames = receivedMessage.iterator();
      ByteArrayOutputStream bbuffer = new ByteArrayOutputStream();
      while (frames.hasNext()) {
        ZFrame frame = frames.next();
        byte[] frameData = frame.getData();
        bbuffer.write(frameData);
      }
      LOGGER.debug("Parsed.");
      sawtoothMessage = Message.parseFrom(bbuffer.toByteArray());
      if (MessageType.PING_REQUEST.equals(sawtoothMessage.getMessageType())) {
        addressCorrelationMapping.put(sawtoothMessage.getCorrelationId(), routerID);
      } else {
        addressCorrelationMapping.put(sawtoothMessage.getCorrelationId(), idReceiver.getBytes());
      }



      LOGGER.debug(this.idReceiver + " Emitting sawtooth Message " + sawtoothMessage.toString());
      recEmitter.onNext(sawtoothMessage);
      LOGGER.debug(this.idReceiver + " emitted to " + recEmitter.name());

    } catch (IOException e) {
      LOGGER.error("Error on working the message.");
      e.printStackTrace();
      return -1;
    }
    return 0;
  }
}

