//@formatter:off
/*-----------------------------------------------------------------------------
 Copyright 2016, 2017 Intel Corporation

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
------------------------------------------------------------------------------*/
// @formatter:on

package sawtooth.sdk.reactive.tp.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import sawtooth.sdk.protobuf.Event;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpEventAddResponse;
import sawtooth.sdk.protobuf.TpStateEntry;
import sawtooth.sdk.protobuf.TpStateGetResponse;
import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.tp.messaging.MessagesStream;

/**
 * Client state that interacts with the context manager through Stream networking.
 */
public class DefaultSawtoothStateImpl implements SawtoothState {

  private final static Logger LOGGER = LoggerFactory.getLogger(DefaultSawtoothStateImpl.class);
  private static final int TIME_OUT = 2;
  MessageFactory mesgFact;
  private MessagesStream stream;

  public DefaultSawtoothStateImpl(MessagesStream stream) {
    this.stream = stream;
  }

  @Override
  public ByteString AddEvent(String contextID, String eventType, Map<String, String> attributes,
      ByteString extraData)
      throws InternalError, InvalidTransactionException, InvalidProtocolBufferException {

    final Event.Attribute.Builder attBuilder = Event.Attribute.newBuilder();

    Message setResponse = null;
    try {
      setResponse = getRemoteMessageResponse(mesgFact.getEventAddRequest(contextID, eventType,
          attributes.entrySet().stream().map(es -> {
            return attBuilder.setKey(es.getKey()).setValue(es.getValue()).build();
          }).collect(Collectors.toList()), extraData));
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      e.printStackTrace();
    }

    ByteString response = null;

    if (setResponse != null) {
      if (!setResponse.getMessageType().equals(MessageType.TP_EVENT_ADD_RESPONSE)) {
        LOGGER.info("Not a response , got {} instead.", setResponse.getMessageType().name());
      }
      TpEventAddResponse responsePayload = mesgFact.createTpEventAddResponse(setResponse);

      switch (responsePayload.getStatus()) {
        case ERROR:
          LOGGER.error(responsePayload.toString());
          throw new InvalidTransactionException(
              "Error received from the Validator for message with id "
                  + setResponse.getCorrelationId());
        case STATUS_UNSET:
          LOGGER.error(responsePayload.toString());
          throw new InvalidTransactionException(
              "Status UNSET for message with id " + setResponse.getCorrelationId());
        case UNRECOGNIZED:
          LOGGER.error(responsePayload.toString());
          throw new InvalidTransactionException("Event Type " + eventType
              + " UNRECOGNIZED for message with id " + setResponse.getCorrelationId());
        case OK:
        default:
          response = responsePayload.toByteString();
          break;

      }
    } else {
      throw new InternalError("State Error, no result found for set request !");
    }

    return response;
  }

  private Message getRemoteMessageResponse(Message mesg)
      throws InterruptedException, ExecutionException, TimeoutException {

    stream.send(mesg).get();
    Future<Message> future = stream.receive(mesg.getCorrelationId());

    return future.get(TIME_OUT, TimeUnit.SECONDS);
  }

  /**
   * Make a Get request on a specific context specified by contextId.
   *
   * @param addresses a collection of address Strings
   * @return Map where the keys are addresses, values Bytestring
   * @throws InternalError something went wrong processing transaction
   * @throws InvalidProtocolBufferException
   */
  @Override
  public Map<String, ByteString> getState(String contextID, List<String> addresses)
      throws InternalError, InvalidTransactionException, InvalidProtocolBufferException {

    Message getResponse;
    try {
      getResponse = getRemoteMessageResponse(mesgFact.getStateRequest(addresses));
    } catch (Exception iee) {
      throw new InternalError(iee.toString());
    }

    Map<String, ByteString> results = new HashMap<String, ByteString>();

    if (getResponse != null) {
      if (!getResponse.getMessageType().equals(MessageType.TP_STATE_GET_RESPONSE)) {
        LOGGER.info("Not a response , got " + getResponse.getMessageType().name() + " instead.");
      }
      TpStateGetResponse responsePayload = TpStateGetResponse.parseFrom(getResponse.getContent());
      if (responsePayload.getStatus() == TpStateGetResponse.Status.AUTHORIZATION_ERROR) {
        throw new InvalidTransactionException(
            "Tried to get unauthorized address " + addresses.toString());
      }
      for (TpStateEntry entry : responsePayload.getEntriesList()) {
        results.put(entry.getAddress(), entry.getData());
      }
    }

    return results;
  }

  /**
   * Make a Set request on a specific context specified by contextId.
   *
   * @param addressValuePairs A collection of Map.Entry's
   * @return addressesThatWereSet, A collection of address Strings that were set
   * @throws InternalError something went wrong processing transaction
   */
  @Override
  public Collection<String> setState(String contextID,
      List<Map.Entry<String, ByteString>> addressValuePairs)
      throws InternalError, InvalidTransactionException {

    Message setResponse;
    try {
      setResponse =
          getRemoteMessageResponse(mesgFact.getSetStateRequest(contextID, addressValuePairs));
    } catch (Exception iee) {
      throw new InternalError(iee.toString());
    }

    List<String> addressesThatWereSet = new ArrayList<String>();
    if (setResponse != null) {
      try {
        addressesThatWereSet = mesgFact.parseStateSetResponse(setResponse);
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
    } else {
      LOGGER.error("Response for Set State was null.");
      LOGGER.error("ContextID {}, map {}.", contextID,
          Arrays.deepToString(addressValuePairs.toArray()));
      throw new InternalError("Response for Set State null.");
    }

    return addressesThatWereSet;
  }


}
