package sawtooth.sdk.reactive.common.message.factory;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchHeader;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionList;
import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;

public class BatchFactory implements AbstractMessageFactory<Batch> {

  /**
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(BatchFactory.class);

  public Batch createBatch(List<? extends Message> transactions, boolean trace) {

    TransactionList.Builder transactionListBuilder = TransactionList.newBuilder();

    List<String> txnSignatures = transactions.stream().map(et -> {
      String result = "";
      try {
        Transaction toAdd;
        if (et.getMessageType().equals(MessageType.TP_PROCESS_REQUEST)) {
          toAdd = createTransactionFromProcessRequest(et);

        } else {
          toAdd = Transaction.parseFrom(et.getContent());
        }
        transactionListBuilder.addTransactions(toAdd);
        result = toAdd.getHeaderSignature();
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error(
            "InvalidProtocolBufferException on Message " + et.toString() + " : " + e.getMessage());
        e.printStackTrace();
      }
      return result;
    }).collect(Collectors.toList());

    BatchHeader batchHeader = BatchHeader.newBuilder().addAllTransactionIds(txnSignatures)
        .setSignerPublicKey(getPubliceyString()).build();

    String headerSignature = SawtoothSigner.signHexSequence(signerPrivateKey,
        batchHeader.toByteArray());

    Batch.Builder batchBuilder = Batch.newBuilder().setHeader(batchHeader.toByteString())
        .setHeaderSignature(headerSignature)
        .addAllTransactions(transactionListBuilder.build().getTransactionsList());

    if (LOGGER.isTraceEnabled() || trace) {
      batchBuilder.setTrace(true);
    }

    return batchBuilder.build();
  }

}
