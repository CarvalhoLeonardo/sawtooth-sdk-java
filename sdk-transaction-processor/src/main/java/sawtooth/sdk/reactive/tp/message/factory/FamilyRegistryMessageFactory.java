package sawtooth.sdk.reactive.tp.message.factory;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.protobuf.TpUnregisterRequest;
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.common.message.factory.AbstractFamilyMessagesFactory;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class generates the messages to keep the Transaction Processor subscription flow with the
 * Validator or Processor.
 *
 *
 */
public class FamilyRegistryMessageFactory extends AbstractFamilyMessagesFactory<Message> {

  /**
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(FamilyRegistryMessageFactory.class);
  private final TransactionFamily tFamily;

  public FamilyRegistryMessageFactory(ECKey privateKey, TransactionFamily registeringFamily)
      throws NoSuchAlgorithmException {
    super(privateKey);
    this.tFamily = registeringFamily;
  }

  private TpRegisterRequest createTpRegisterRequest() {
    TpRegisterRequest.Builder reqBuilder = TpRegisterRequest.newBuilder();

    reqBuilder.setFamily(tFamily.getFamilyName());
    reqBuilder.addAllNamespaces(tFamily.getNameSpaces().keySet());
    reqBuilder.setVersion(tFamily.getFamilyVersion());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format("Register request : Family %s, Namespaces %s, Version %s",
          reqBuilder.getFamily(), Arrays.deepToString(reqBuilder.getNamespacesList().toArray()),
          reqBuilder.getVersion()));
    }
    return reqBuilder.build();
  }

  private TpRegisterResponse createTpRegisterResponse(int status) {
    TpRegisterResponse.Builder reqBuilder = TpRegisterResponse.newBuilder();

    reqBuilder.setStatusValue(status);

    return reqBuilder.build();
  }

  private TpUnregisterRequest createTpUnregisterRequest() {
    TpUnregisterRequest request = TpUnregisterRequest.newBuilder().build();
    return request;
  }

  public Message generateRegisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpRegisterRequest().toByteString())
        .setCorrelationId(this.generateId()).setMessageType(MessageType.TP_REGISTER_REQUEST)
        .build();

    return newMessage;
  }

  public Message generateUnregisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpUnregisterRequest().toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_UNREGISTER_REQUEST).build();

    return newMessage;
  }

  public Message getRegisterResponse(int status, String correlationId) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpRegisterResponse(status).toByteString()).setCorrelationId(correlationId)
        .setMessageType(MessageType.TP_REGISTER_RESPONSE).build();

    return newMessage;
  }

}
