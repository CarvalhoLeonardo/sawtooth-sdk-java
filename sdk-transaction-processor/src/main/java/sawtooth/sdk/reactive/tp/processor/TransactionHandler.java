package sawtooth.sdk.reactive.tp.processor;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.reactive.tp.message.factory.MessageFactory;

public interface TransactionHandler {

  static final int MESSAGE_SIZE_DELIMITER = 64;

  /**
   * Apply the Process Request on the State
   *
   * @param processRequest
   * @param state
   * @return Completable Future Result of the execution
   */
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state);

  public MessageFactory getMessageFactory();

  public Collection<String> getNameSpaces();

  public String getVersion();

  public void setContextId(byte[] externalContextID);

  /**
   * Set the message factory to use in this Handler. It will manage the semantics of the generated
   * and received messages.
   *
   * @param mFactory - the *INITIALIZED* Message Factory
   */
  public void setMessageFactory(MessageFactory mFactory);

  public String transactionFamilyName();

}
