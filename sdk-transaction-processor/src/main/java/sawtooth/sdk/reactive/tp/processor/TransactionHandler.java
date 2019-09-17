package sawtooth.sdk.reactive.tp.processor;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.bitcoinj.core.ECKey;

import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.common.message.factory.BatchFactory;
import sawtooth.sdk.reactive.common.message.factory.TransactionFactory;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.factory.FamilyRegistryMessageFactory;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * To handle the Transactions, the implementing class needs to work all the messages
 *
 */
public interface TransactionHandler {

  static final int MESSAGE_SIZE_DELIMITER = 64;

  /**
   * Apply the Process Request on the State
   *
   * @param processRequest
   * @param state
   * @return Completable Future Result of the execution
   */
  public CompletableFuture<TpProcessResponse> apply(TpProcessRequest processRequest,
      SawtoothState state);

  /**
   *
   * @return the Batch factory to that assembles the batches to submit to the validator/processor
   * for this family
   */
  public BatchFactory getBatchFactory();

  /**
   *
   * @return CoreMessagesFactory to this instance.
   */
  public CoreMessagesFactory getCoreMessageFactory();

  /**
   *
   * @return the registration messages to talk do the validator/processor for this family
   */
  public FamilyRegistryMessageFactory getFamilyRegistryMessageFactory();

  public Collection<String> getNameSpaces();

  /**
   *
   * @return the transaction factory to assembles the Transactions to submit to the
   * validator/processor for this family
   */
  public TransactionFactory getTransactionFactory();

  /**
   *
   * @return the definition fo the family this handler works with
   */
  public TransactionFamily getTransactionFamily();

  /**
   *
   * @return the public key of this trnsaction handler
   */
  public ECKey getTransactorPubKey();

  public void setContextId(byte[] externalContextID);

}
