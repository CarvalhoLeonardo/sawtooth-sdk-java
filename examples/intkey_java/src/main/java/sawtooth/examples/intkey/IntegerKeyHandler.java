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
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.bitcoinj.core.ECKey;
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
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.common.message.factory.BatchFactory;
import sawtooth.sdk.reactive.common.message.factory.TransactionFactory;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.factory.FamilyRegistryMessageFactory;
import sawtooth.sdk.reactive.tp.processor.SawtoothState;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;

public class IntegerKeyHandler implements TransactionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegerKeyHandler.class.getName());
  private static final long MAX_NAME_LENGTH = 20;
  private static final long MAX_VALUE = Long.MAX_VALUE;
  private static final long MIN_VALUE = 0;
  private static final String ONLY_NAMESPACE = "intkey";
  private final BatchFactory INTKEY_BATCH_FACTORY;
  private final CoreMessagesFactory INTKEY_CORE_MESSAGE_FACTORY;
  private final TransactionFamily INTKEY_MESSAGE_FAMILY;
  private final FamilyRegistryMessageFactory INTKEY_REGISTRY_MESSAGE_FACTORY;
  private final TransactionFactory INTKEY_TRANSACTION_FACTORY;
  private ECKey pubKey;

  /**
   * constructor.
   * @throws NoSuchAlgorithmException
   *
   */
  public IntegerKeyHandler(ECKey privateKey) throws NoSuchAlgorithmException {
    pubKey = ECKey.fromPublicOnly(privateKey.getPubKeyPoint());
    LOGGER.debug("Integet K Handler with EC Key using crypter {} and Public Key {}",
        privateKey.getKeyCrypter(), pubKey.getPublicKeyAsHex());
    INTKEY_MESSAGE_FAMILY = new TransactionFamily("intkey", "1.0", new String[] { ONLY_NAMESPACE });

    INTKEY_CORE_MESSAGE_FACTORY = new CoreMessagesFactory();
    INTKEY_REGISTRY_MESSAGE_FACTORY = new FamilyRegistryMessageFactory(privateKey,
        INTKEY_MESSAGE_FAMILY);
    INTKEY_BATCH_FACTORY = new BatchFactory(privateKey);
    INTKEY_TRANSACTION_FACTORY = new TransactionFactory(INTKEY_MESSAGE_FAMILY, privateKey);
  }

  @Override
  public CompletableFuture<TpProcessResponse> apply(TpProcessRequest processRequest,
      SawtoothState state) {
    /*
     * IntKey state will be stored at an address of the name with the key being the name and the
     * value an integer. so { "foo": 20, "bar": 26} would be a possibility if the hashing algorithm
     * hashes foo and bar to the same address
     */

    TpProcessResponse.Builder responseBuilder = TpProcessResponse.newBuilder();

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

      String address = INTKEY_MESSAGE_FAMILY.generateAddress(ONLY_NAMESPACE, name);

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

        if (possibleValues.isEmpty()) {
          throw new InvalidTransactionException("Verb is inc but Name is not in state");
        }
        stateValueRep = possibleValues.get(address).toByteArray();
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

        if (possibleAddressResult.isEmpty()) {
          throw new InvalidTransactionException("Verb is dec but Name is not in state");
        }
        stateValueRep = possibleAddressResult.get(address).toByteArray();
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
      responseBuilder.setStatus(Status.OK);
      responseBuilder.setMessage(address + " set correctly.");
    } catch (InvalidTransactionException e) {
      LOGGER.error("Exception {}", e);
      e.printStackTrace();
      responseBuilder.setStatus(Status.INVALID_TRANSACTION);
      responseBuilder.setMessage(e.getMessage());
    } catch (InternalError e) {
      e.printStackTrace();
      responseBuilder.setStatus(Status.INTERNAL_ERROR);
      responseBuilder.setMessage(e.getMessage());
    } catch (CborException e) {
      LOGGER.error("Cbor Exception {}", e);
      e.printStackTrace();
      responseBuilder.setStatus(Status.INTERNAL_ERROR);
      responseBuilder.setMessage(e.getMessage());
    } catch (InvalidProtocolBufferException e) {
      LOGGER.error("InvalidProtocolBuffer Exception {}", e);
      e.printStackTrace();
      responseBuilder.setStatus(Status.INTERNAL_ERROR);
      responseBuilder.setMessage(e.getMessage());
    }

    return CompletableFuture.completedFuture(responseBuilder.build());
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
  public BatchFactory getBatchFactory() {
    return INTKEY_BATCH_FACTORY;
  }

  @Override
  public CoreMessagesFactory getCoreMessageFactory() {
    return INTKEY_CORE_MESSAGE_FACTORY;
  }

  @Override
  public FamilyRegistryMessageFactory getFamilyRegistryMessageFactory() {
    return INTKEY_REGISTRY_MESSAGE_FACTORY;
  }

  @Override
  public Collection<String> getNameSpaces() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TransactionFactory getTransactionFactory() {
    return INTKEY_TRANSACTION_FACTORY;
  }

  @Override
  public TransactionFamily getTransactionFamily() {
    return INTKEY_MESSAGE_FAMILY;
  }

  @Override
  public ECKey getTransactorPubKey() {
    return pubKey;
  }

  @Override
  public void setContextId(byte[] externalContextID) {
    // TODO Auto-generated method stub

  }

}
