package sawtooth.sdk.reactive.tp.processor;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;

public interface TransactionHandler {

  public String transactionFamilyName();

  public String getVersion();

  public Collection<String> getNameSpaces();

  /**
   * Apply the Process Request on the State
   * 
   * @param processRequest
   * @param state
   * @return Completable Future Result of the execution
   */
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state);

  /**
   * Set the message factory to use in this Handler. It will manage the semantics of the generated
   * and received messages.
   * 
   * @param mFactory - the *INITIALIZED* Message Factory
   */

  public void setMessageFactory(MessageFactory mFactory);

  public MessageFactory getMessageFactory();
  
  static final int MESSAGE_SIZE_DELIMITER = 64;

}
