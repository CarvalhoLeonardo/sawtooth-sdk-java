package sawtooth.sdk.tp.fake;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.common.messaging.MessageFactory;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.tp.core.ReactorNetworkNode;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class intends to mimic a validator, construcing messages for various tests to any TPs
 * connecting to it.
 *
 * More than a Mock, much less than the real deal.
 *
 */
public class FakeValidator implements Runnable {
  private final static Logger LOGGER = LoggerFactory.getLogger(FakeValidator.class);

  MessageFactory internalMF;
  ReactorNetworkNode internalNode;

  private Function<Message, Message> transformationFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message m) {
      switch (m.getMessageTypeValue()) {
      case Message.MessageType.PING_REQUEST_VALUE:
        LOGGER.debug("--------------------- Receiving PING_REQUEST");
        m = internalMF.getPingResponse(m.getCorrelationId());
        break;
      case Message.MessageType.PING_RESPONSE_VALUE:
        LOGGER.debug("--------------------- Receiving PING_RESPONSE");
        break;
      case Message.MessageType.TP_REGISTER_REQUEST_VALUE:
        LOGGER.debug("--------------------- Receiving REGISTER_REQUEST");
        try {
          receiveRegisterRequest(TpRegisterRequest.parseFrom(m.getContent()));
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
        m = internalMF.getRegisterResponse(TpRegisterResponse.Status.OK_VALUE,
            m.getCorrelationId());
        break;
      default:
      }
      LOGGER.debug("answering with " + m.toString());
      return m;
    }
  };

  public FakeValidator(MessageFactory source, String mqAddress) {
    LOGGER.debug("Registering Message Factory of family " + source.getFamilyName());
    this.internalMF = source;
    internalNode = new ReactorNetworkNode(mqAddress, 4, "fakeValidator", true);
  }

  private void receiveRegisterRequest(TpRegisterRequest req) throws InvalidProtocolBufferException {
    LOGGER.debug("Registering Message Factory of family " + req.getFamily());
    if (!this.internalMF.getFamilyName().equalsIgnoreCase(req.getFamily())
        && this.internalMF.getFamilyVersion().equalsIgnoreCase(req.getVersion())) {
      throw new InvalidProtocolBufferException("Wrong TP version received !");
    }
  }

  @Override
  public void run() {
    internalNode.setWorkingFunction(transformationFunction);
    internalNode.run();
  }

}
