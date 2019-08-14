package sawtooth.sdk.reactive.tp.processor;

import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * Interface that defines the Client's Sawtooth Events handling
 *
 */
public interface SawtoothEventHandler {

  /**
   *
   * Add a new event to the execution result for this transaction.
   *
   *
   * @param eventToAdd This is used to subscribe to events. It should be globally unique and
   * describe what, in general, has occured.
   * @param attributes Additional information about the event that is transparent to the validator.
   * Attributes can be used by subscribers to filter the type of events they receive.
   *
   * @param extraData Additional information about the event that is opaque to the validator.
   *
   * @return Response from the Validator
   */
  public ByteString AddEvent(String contextID, String eventType, Map<String, String> attributes,
      ByteString extraData)
      throws InternalError, InvalidTransactionException, InvalidProtocolBufferException;

}
