package sawtooth.sdk.reactive.tp.fake;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;
import sawtooth.sdk.reactive.tp.transport.zmq.ReactorNetworkNode;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class intends to mimic a validator, constructing messages for various tests to any TPs
 * connecting to it.
 *
 * More than a Mock, much less than the real deal.
 *
 */
public class FakeValidator implements Runnable {
  private final static Logger LOGGER = LoggerFactory.getLogger(FakeValidator.class);

  ReactorNetworkNode internalNode;
  TransactionHandler internalTR;

  private Function<Message, Message> transformationFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message m) {
      switch (m.getMessageTypeValue()) {
      case Message.MessageType.PING_REQUEST_VALUE:
        LOGGER.debug("--------------------- Receiving PING_REQUEST");
        m = internalTR.getCoreMessageFactory().getPingResponse(m.getCorrelationId());
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
        m = internalTR.getFamilyRegistryMessageFactory()
            .getRegisterResponse(TpRegisterResponse.Status.OK_VALUE, m.getCorrelationId());
        break;
      default:
      }
      LOGGER.debug("answering with " + m.toString());
      return m;
    }
  };

  public FakeValidator(TransactionHandler source, String mqAddress) {
    LOGGER.debug(
        "Registering Message Factory of family " + source.getTransactionFamily().getFamilyName());
    this.internalTR = source;
    internalNode = new ReactorNetworkNode(mqAddress, 4, "fakeValidator", true);
  }

  private void receiveRegisterRequest(TpRegisterRequest req) throws InvalidProtocolBufferException {
    LOGGER.debug("Registering Message Factory of family " + req.getFamily());
    if (!this.internalTR.getTransactionFamily().getFamilyName().equalsIgnoreCase(req.getFamily())
        && this.internalTR.getTransactionFamily().getFamilyVersion()
            .equalsIgnoreCase(req.getVersion())) {
      throw new InvalidProtocolBufferException("Wrong TP version received !");
    }
  }

  @Override
  public void run() {
    internalNode.setWorkingFunction(transformationFunction);
    internalNode.run();
  }

}
