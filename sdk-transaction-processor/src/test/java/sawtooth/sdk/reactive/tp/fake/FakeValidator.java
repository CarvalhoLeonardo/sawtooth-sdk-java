package sawtooth.sdk.reactive.tp.fake;

import static sawtooth.sdk.protobuf.Message.MessageType.PING_REQUEST_VALUE;
import static sawtooth.sdk.protobuf.Message.MessageType.PING_RESPONSE_VALUE;
import static sawtooth.sdk.protobuf.Message.MessageType.TP_REGISTER_REQUEST_VALUE;

import java.security.NoSuchAlgorithmException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.reactive.tp.fake.factory.message.FakeValidatorCoreMF;
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
  private final static FakeValidatorCoreMF localCMF;
  private final static Logger LOGGER = LoggerFactory.getLogger(FakeValidator.class);
  private final static TpRegisterResponse.Builder registerRespBldr = TpRegisterResponse
      .newBuilder();
  static {
    FakeValidatorCoreMF tmpFC = null;
    try {
      tmpFC = new FakeValidatorCoreMF();
    } catch (NoSuchAlgorithmException e) {

    }
    localCMF = tmpFC;
  }

  ReactorNetworkNode internalNode;

  private Function<Message, Message> transformationFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message m) {
      switch (m.getMessageTypeValue()) {
      case PING_REQUEST_VALUE:
        LOGGER.debug("--------------------- Receiving PING_REQUEST -- CID {}",
            m.getCorrelationId());
        m = localCMF.getPingResponse(m.getCorrelationId());
        break;
      case PING_RESPONSE_VALUE:
        LOGGER.debug("--------------------- Receiving PING_RESPONSE -- CID {}",
            m.getCorrelationId());
        break;
      case TP_REGISTER_REQUEST_VALUE:
        /*
         * dispatcher.add_handler( validator_pb2.Message.TP_REGISTER_REQUEST,
         * processor_handlers.ProcessorRegisterHandler( executor.processor_manager), thread_pool)
         *
         */
        LOGGER.debug("--------------------- Receiving REGISTER_REQUEST -- CID {}",
            m.getCorrelationId());
        try {
          receiveRegisterRequest(TpRegisterRequest.parseFrom(m.getContent()));
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
        m = Message.newBuilder()
            .setContent(registerRespBldr.setStatusValue(TpRegisterResponse.Status.OK_VALUE).build()
                .toByteString())
            .setCorrelationId(m.getCorrelationId()).setMessageType(MessageType.TP_REGISTER_RESPONSE)
            .build();

        break;
      default:
        LOGGER.debug("--------------------- Receiving not prepared for type {} -- CID {}",
            m.getMessageType(), m.getCorrelationId());
      }
      LOGGER.debug("answering with " + m.toString());
      return m;
    }
  };

  public FakeValidator(String mqAddress, int pFactor) {
    internalNode = new ReactorNetworkNode(mqAddress, pFactor, "fakeValidator", true);
    internalNode.setWorkingFunction(transformationFunction);

  }

  public Function<Message, Message> getOriginalFunction() {
    return transformationFunction;
  }

  private void receiveRegisterRequest(TpRegisterRequest req) throws InvalidProtocolBufferException {
    LOGGER.debug("Registering Message Factory of family {} and version {}", req.getFamily(),
        req.getVersion());
    /*
     * if (!this.internalTR.getTransactionFamily().getFamilyName().equalsIgnoreCase(req.getFamily())
     * && this.internalTR.getTransactionFamily().getFamilyVersion()
     * .equalsIgnoreCase(req.getVersion())) { throw new
     * InvalidProtocolBufferException("Wrong TP version received !");
     */
  }

  @Override
  public void run() {
    internalNode.run();
  }

  public void setNewTransformatationFunction(Function<Message, Message> newFunction) {
    internalNode.setWorkingFunction(newFunction);
  }

}
