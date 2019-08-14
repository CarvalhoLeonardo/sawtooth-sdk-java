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
import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;
import sawtooth.sdk.reactive.tp.message.factory.EventMessagesFactory;
import sawtooth.sdk.reactive.tp.message.flow.MessagesStream;

/**
 * Client state that interacts with the context manager through Stream networking.
 */
public class DefaultSawtoothEventHandlerImpl implements SawtoothEventHandler {

  private final static Logger LOGGER = LoggerFactory
      .getLogger(DefaultSawtoothEventHandlerImpl.class);
  private static final int TIME_OUT = 2;
  EventMessagesFactory mesgFact;
  private MessagesStream stream;

  public DefaultSawtoothEventHandlerImpl(MessagesStream stream) {
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

}
