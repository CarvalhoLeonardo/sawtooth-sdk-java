/*
 * Copyright 2016, 2017 Intel Corporation Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. ------------------------------------------------------------------------------
 */

package sawtooth.examples.intkey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import co.nstant.in.cbor.CborBuilder;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborEncoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.DataItem;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpProcessResponse.Status;
import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.common.messaging.SawtoothAddressFactory;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;
import sawtooth.sdk.reactive.tp.processor.SawtoothState;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;

public class IntegerKeyHandler implements TransactionHandler, SawtoothAddressFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegerKeyHandler.class.getName());

  private static final long MAX_NAME_LENGTH = 20;
  private static final long MAX_VALUE = Long.MAX_VALUE;
  private static final long MIN_VALUE = 0;
  private MessageFactory tpMesgFactory;

  /**
   * constructor.
   *
   */
  public IntegerKeyHandler() {

    try {
      tpMesgFactory = new MessageFactory("intkey", "1.0", null, null, "intkey");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  /**
   * Helper function to decode the Payload of a transaction. Convert the co.nstant.in.cbor.model.Map
   * to a HashMap.
   */
  public Map<String, String> decodePayload(byte[] bytes) throws CborException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    co.nstant.in.cbor.model.Map data = (co.nstant.in.cbor.model.Map) new CborDecoder(bais)
        .decodeNext();
    DataItem[] keys = data.getKeys().toArray(new DataItem[0]);
    Map<String, String> result = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      result.put(keys[i].toString(), data.get(keys[i]).toString());
    }
    return result;
  }

  /**
   * Helper function to decode State retrieved from the address of the name. Convert the
   * co.nstant.in.cbor.model.Map to a HashMap.
   */
  public Map<String, Long> decodeState(byte[] bytes) throws CborException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    co.nstant.in.cbor.model.Map data = (co.nstant.in.cbor.model.Map) new CborDecoder(bais)
        .decodeNext();
    DataItem[] keys = data.getKeys().toArray(new DataItem[0]);
    Map<String, Long> result = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      result.put(keys[i].toString(), Long.decode(data.get(keys[i]).toString()));
    }
    return result;
  }

  /**
   * Helper function to encode the State that will be stored at the address of the name.
   */
  public Map.Entry<String, ByteString> encodeState(String address, String name, Long value)
      throws CborException {
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    new CborEncoder(boas).encode(new CborBuilder().addMap().put(name, value).end().build());

    return new AbstractMap.SimpleEntry<String, ByteString>(address,
        ByteString.copyFrom(boas.toByteArray()));
  }

  @Override
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state) {
    /*
     * IntKey state will be stored at an address of the name with the key being the name and the
     * value an integer. so { "foo": 20, "bar": 26} would be a possibility if the hashing algorithm
     * hashes foo and bar to the same address
     */

    TpProcessResponse.Builder responseBulder = TpProcessResponse.newBuilder();

    try {
      Map<String, String> updateMap;
      LOGGER.debug("Got a TpProcessRequest with {} inputs for context id {} ...",
          processRequest.getHeader().getInputsCount(), processRequest.getContextId());
      updateMap = this.decodePayload(processRequest.getPayload().toByteArray());
      // validate name
      String name = updateMap.get("Name").toString();

      if (name.length() == 0) {
        throw new InvalidTransactionException("Name is required");
      }

      if (name.length() > MAX_NAME_LENGTH) {
        throw new InvalidTransactionException("Name must be a string of no more than "
            + Long.toString(MAX_NAME_LENGTH) + " characters");
      }

      // validate verb
      String verb = updateMap.get("Verb").toString();

      if (verb.length() == 0) {
        throw new InvalidTransactionException("Verb is required");
      }

      if (TPOperations.getByVerb(verb) == null) {
        throw new InvalidTransactionException(
            "Verb must be one of " + TPOperations.values() + ", not " + verb);
      }

      // validate value
      Long value = null;

      try {
        value = Long.decode(updateMap.get("Value").toString());
      } catch (NumberFormatException ex) {
        throw new InvalidTransactionException("Value must be an integer");
      }

      if (value == null) {
        throw new InvalidTransactionException("Value is required");
      }

      if (value > MAX_VALUE || value < MIN_VALUE) {
        throw new InvalidTransactionException("Value must be an integer " + "no less than "
            + Long.toString(MIN_VALUE) + " and no greater than " + Long.toString(MAX_VALUE));
      }

      String address = generateAddress(name);

      Collection<String> addresses = new ArrayList<String>();
      Map<String, ByteString> possibleAddressValues;
      Map<String, Long> stateValue = null;
      byte[] stateValueRep;

      switch (TPOperations.getByVerb(verb)) {
      case SET:

        if (value < 0) {
          throw new InvalidTransactionException("Verb is set but Value is less than 0");
        }

        // The ByteString is cbor encoded dict/hashmap
        possibleAddressValues = state.getState(processRequest.getContextId(),
            Arrays.asList(address));
        stateValueRep = possibleAddressValues.get(address) != null
            ? possibleAddressValues.get(address).toByteArray() : new byte[0];
        stateValue = null;
        if (stateValueRep.length > 0) {
          stateValue = this.decodeState(stateValueRep);
          if (stateValue.containsKey(name)) {
            throw new InvalidTransactionException("Verb is set but Name already in state, "
                + "Name: " + name + " Value: " + stateValue.get(name).toString());
          }
        }

        // 'set' passes checks so store it in the state
        Map.Entry<String, ByteString> entry = this.encodeState(address, name, value);

        List<Map.Entry<String, ByteString>> addressValues = Arrays.asList(entry);
        addresses = state.setState(processRequest.getContextId(), addressValues);

        break;

      case INC:
        Map<String, ByteString> possibleValues = state.getState(processRequest.getContextId(),
            Arrays.asList(address));
        stateValueRep = possibleValues.get(address).toByteArray();
        if (stateValueRep.length == 0) {
          throw new InvalidTransactionException("Verb is inc but Name is not in state");
        }
        stateValue = this.decodeState(stateValueRep);
        if (!stateValue.containsKey(name)) {
          throw new InvalidTransactionException("Verb is inc but Name is not in state");
        }
        if (stateValue.get(name) + value > MAX_VALUE) {
          throw new InvalidTransactionException(
              "Inc would set Value to greater than " + Long.toString(MAX_VALUE));
        }
        // Increment the value in state by value
        entry = this.encodeState(address, name, stateValue.get(name) + value);
        addressValues = Arrays.asList(entry);
        addresses = state.setState(processRequest.getContextId(), addressValues);
        break;

      case DEC:
        Map<String, ByteString> possibleAddressResult = state
            .getState(processRequest.getContextId(), Arrays.asList(address));
        stateValueRep = possibleAddressResult.get(address).toByteArray();

        if (stateValueRep.length == 0) {
          throw new InvalidTransactionException("Verb is dec but Name is not in state");
        }
        stateValue = this.decodeState(stateValueRep);
        if (!stateValue.containsKey(name)) {
          throw new InvalidTransactionException("Verb is dec but Name is not in state");
        }
        if (stateValue.get(name) - value < MIN_VALUE) {
          throw new InvalidTransactionException(
              "Dec would set Value to less than " + Long.toString(MIN_VALUE));
        }

        // Decrement the value in state by value
        entry = this.encodeState(address, name, stateValue.get(name) - value);

        addressValues = Arrays.asList(entry);
        addresses = state.setState(processRequest.getContextId(), addressValues);
        break;
      }

      // if the 'set', 'inc', or 'dec' set to state didn't work
      if (addresses.size() == 0) {
        throw new InternalError("State error!.");
      }
      LOGGER.info("Verb: " + verb + " Name: " + name + " value: " + value);
      responseBulder.setStatus(Status.OK);
      responseBulder.setMessage(address + " set correctly.");
    } catch (InvalidTransactionException e) {
      LOGGER.error("Exception {}", e);
      e.printStackTrace();
      responseBulder.setStatus(Status.INVALID_TRANSACTION);
      responseBulder.setMessage(e.getMessage());
    } catch (InternalError e) {
      e.printStackTrace();
      responseBulder.setStatus(Status.INTERNAL_ERROR);
      responseBulder.setMessage(e.getMessage());

    } catch (CborException e) {
      LOGGER.error("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Exception {}", e);
      LOGGER.debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Exception {}", e);
      e.printStackTrace();
    } catch (InvalidProtocolBufferException e) {
      LOGGER.error("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Exception {}", e);
      LOGGER.debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Exception {}", e);
      e.printStackTrace();
    }

    return CompletableFuture.completedFuture(responseBulder.build());
  }

  @Override
  public String generateAddress(ByteBuffer data) {
    return FormattingUtils.hash512(data.array());
  }

  @Override
  public String generateAddress(String... names) {
    String hashedName = "";
    try {
      hashedName = FormattingUtils.hash512(names[0].getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return tpMesgFactory.getNameSpaces()[0] + hashedName.substring(hashedName.length() - 64);
  }

  @Override
  public MessageFactory getMessageFactory() {
    return tpMesgFactory;
  }

  @Override
  public Collection<String> getNameSpaces() {
    return Arrays.asList(tpMesgFactory.getNameSpaces());
  }

  @Override
  public String getVersion() {
    return tpMesgFactory.getFamilyVersion();
  }

  @Override
  public void setMessageFactory(MessageFactory mFactory) {

  }

  @Override
  public String transactionFamilyName() {
    return tpMesgFactory.getFamilyName();
  }
}
