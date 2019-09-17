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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpStateEntry;
import sawtooth.sdk.protobuf.TpStateGetResponse;
import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.flow.MessagesStream;

/**
 * Client state that interacts with the context manager through Stream networking.
 */
public class DefaultSawtoothStateImpl implements SawtoothState {

  private final static Logger LOGGER = LoggerFactory.getLogger(DefaultSawtoothStateImpl.class);
  CoreMessagesFactory mesgFact;
  private MessagesStream stream;
  private final int TIME_OUT;

  @SuppressWarnings("unused")
  private DefaultSawtoothStateImpl() {
    this.TIME_OUT = 0;
    this.stream = null;
  }

  /**
   * Create the default instance
   *
   * @param stream - Messages Stream
   * @param timeou_milisseconds - timeout in millisseconds for the async operations
   */
  public DefaultSawtoothStateImpl(MessagesStream stream, int timeou_milisseconds) {
    this.TIME_OUT = timeou_milisseconds;
    this.stream = stream;
    try {
      mesgFact = new CoreMessagesFactory();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
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
      getResponse = sendRemoteMessageGetResponse(mesgFact.getStateRequest(addresses));
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
    if (results.isEmpty()) {
      throw new InvalidTransactionException(
          "State Error, no result found for get request:" + addresses.toString());
    }

    return results;
  }

  private Message sendRemoteMessageGetResponse(Message mesg)
      throws InterruptedException, ExecutionException, TimeoutException {

    Future<Message> sent = stream.send(mesg);
    sent.get(TIME_OUT, TimeUnit.MILLISECONDS);

    Future<Message> future = null;

    if (sent.isDone()) {
      future = stream.receive(mesg.getCorrelationId());
    } else {

    }

    return future.get(TIME_OUT, TimeUnit.MILLISECONDS);
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
      setResponse = sendRemoteMessageGetResponse(
          mesgFact.getSetStateRequest(contextID, addressValuePairs));
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
    }

    return addressesThatWereSet;
  }

}
