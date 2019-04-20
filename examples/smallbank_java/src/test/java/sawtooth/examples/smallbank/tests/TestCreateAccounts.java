package sawtooth.examples.smallbank.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.enterprise.inject.spi.CDI;
import javax.ws.rs.core.Response;

import org.bitcoinj.core.ECKey;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.examples.smallbank.model.protobuf.SmallbankTransactionPayload;
import sawtooth.examples.smallbank.model.protobuf.SmallbankTransactionPayload.CreateAccountTransactionData;
import sawtooth.examples.smallbank.model.protobuf.SmallbankTransactionPayload.DepositCheckingTransactionData;
import sawtooth.examples.smallbank.model.protobuf.SmallbankTransactionPayload.PayloadType;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;
import sawtooth.sdk.reactive.rest.ops.RESTBatchOps;

@Test
public class TestCreateAccounts extends BaseTest {
  static Map<String, String> clients;
  static Map<String, String> clientsAddresses = new HashMap<>();
  static {
    clients = new HashMap<String, String>();
    clients.put("aaaa", "Client number 1");
    clients.put("bbbb", "Client number 2");
    clients.put("cccc", "Client number 3");
    clients.put("dddd", "Client number 4");
  }

  ECKey signerPrivateKey, signerPublicKey;
  MessageFactory smallBankMF;

  String startAddressData = "";

  RESTBatchOps underTest;

  @Test(dependsOnMethods = { "createAccounts" })
  public void addToAAACheckings() throws InterruptedException, ExecutionException {
    addToCheckinAccount("aaaa", 100);
  }

  @Test(dependsOnMethods = { "createAccounts" })
  public void addToBBBBCheckings() throws InterruptedException, ExecutionException {
    addToCheckinAccount("bbbb", 200);
  }

  @Test(dependsOnMethods = { "createAccounts" })
  public void addToCCCCheckings() throws InterruptedException, ExecutionException {
    addToCheckinAccount("cccc", 1000);
  }

  private void addToCheckinAccount(String client, int amount)
      throws InterruptedException, ExecutionException {
    Integer customerId = Integer.parseInt(client, 16);
    DepositCheckingTransactionData depositTransaction = DepositCheckingTransactionData.newBuilder()
        .setAmount(amount).setCustomerId(customerId).build();

    LOGGER.debug("Adding {} to checking account for customer id {} (@ address {})...", amount,
        customerId, clientsAddresses.get(client));
    Message addCheckingAccountTX = null;
    try {
      SmallbankTransactionPayload assembledPayload = SmallbankTransactionPayload.newBuilder()
          .setDepositChecking(depositTransaction).setPayloadType(PayloadType.DEPOSIT_CHECKING)
          .build();
      addCheckingAccountTX = smallBankMF.getProcessRequest("",
          ByteBuffer.wrap(assembledPayload.toByteArray()), Arrays.asList(""), Arrays.asList(""),
          null, signerPublicKey.getPublicKeyAsHex());
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    Batch toSend = smallBankMF.createBatch(Arrays.asList(addCheckingAccountTX), true);
    LOGGER.debug("Batch : {}.", toSend.toString());

    Future<Response> result = underTest.submitBatches(Arrays.asList(toSend), null);
    assertNotNull(result);
    assertFalse(((CompletableFuture<Response>) result).isCompletedExceptionally());
    Response serverResult = result.get();
    assertNotNull(serverResult);
    assertEquals(serverResult.getStatus(), HttpURLConnection.HTTP_ACCEPTED);
  }

  @Test
  public void createAccounts() throws InterruptedException, ExecutionException {
    List<Message> transactions = clients.keySet().stream().map(nci -> {

      Integer customerId = Integer.parseInt(nci, 16);
      CreateAccountTransactionData accountCreation = CreateAccountTransactionData.newBuilder()
          .setCustomerId(customerId).setCustomerName(clients.get(nci))
          .setInitialCheckingBalance(100).setInitialSavingsBalance(100).build();
      ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(customerId.toString());
      String addressToSet = FormattingUtils.hash512(byteBuffer.array());
      addressToSet = startAddressData + addressToSet.substring(addressToSet.length() - 63);
      clientsAddresses.put(nci, addressToSet);
      LOGGER.debug("Creating account for customer id {} (@ address {})...", customerId,
          addressToSet);
      Message createAccountTX = null;
      try {
        SmallbankTransactionPayload assembledPayload = SmallbankTransactionPayload.newBuilder()
            .setCreateAccount(accountCreation).setPayloadType(PayloadType.CREATE_ACCOUNT).build();
        createAccountTX = smallBankMF.getProcessRequest("",
            ByteBuffer.wrap(assembledPayload.toByteArray()), Arrays.asList(""), Arrays.asList(""),
            null, signerPublicKey.getPublicKeyAsHex());
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
      return createAccountTX;
    }).collect(Collectors.toList());

    Batch toSend = smallBankMF.createBatch(transactions, true);
    LOGGER.debug("Batch : {}.", toSend.toString());

    Future<Response> result = underTest.submitBatches(Arrays.asList(toSend), null);
    assertNotNull(result);
    assertFalse(((CompletableFuture<Response>) result).isCompletedExceptionally());
    Response serverResult = result.get();
    assertNotNull(serverResult);
    assertEquals(serverResult.getStatus(), HttpURLConnection.HTTP_ACCEPTED);
  }

  @BeforeClass
  public void getCDIBeans() throws ClassNotFoundException, NoSuchAlgorithmException {
    underTest = CDI.current().select(RESTBatchOps.class).get();

    BigInteger keyUnderProc = new BigInteger("12345678901234567890");
    signerPrivateKey = ECKey.fromPrivate(keyUnderProc, false);
    signerPublicKey = ECKey.fromPublicOnly(ECKey.publicKeyFromPrivate(keyUnderProc, true));
    smallBankMF = new MessageFactory("smallbank", "1.0", signerPrivateKey, signerPublicKey,
        "smallbank");
    startAddressData = FormattingUtils.hash512("smallbank".getBytes(StandardCharsets.UTF_8))
        .substring(0, 7);

    LOGGER.debug("Address prefix : {}.", startAddressData);

  }

}
