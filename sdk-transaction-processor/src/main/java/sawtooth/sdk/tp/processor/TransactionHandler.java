package sawtooth.sdk.tp.processor;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import sawtooth.sdk.common.messaging.MessageFactory;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;

public interface TransactionHandler {

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

  /**
   * Set the message factory to use in this Handler. It will manage the semantics of the generated
   * and received messages.
   *
   * @param mFactory - the *INITIALIZED* Message Factory
   */

  public void setMessageFactory(MessageFactory mFactory);

  public String transactionFamilyName();

}
