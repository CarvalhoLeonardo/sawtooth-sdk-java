package sawtooth.sdk.reactive.common.message.factory;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchHeader;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionList;
import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;

public class BatchFactory extends AbstractFamilyMessagesFactory<Batch> {

  /*
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(BatchFactory.class);

  public BatchFactory(ECKey privateKey) throws NoSuchAlgorithmException {
    super(privateKey);
  }

  public Batch createBatch(List<Transaction> transactions, boolean trace) {

    TransactionList.Builder transactionListBuilder = TransactionList.newBuilder();

    List<String> txnSignatures = transactions.stream().map(et -> {
      String result = "";
      transactionListBuilder.addTransactions(et);
      result = et.getHeaderSignature();
      return result;
    }).collect(Collectors.toList());

    BatchHeader batchHeader = BatchHeader.newBuilder().addAllTransactionIds(txnSignatures)
        .setSignerPublicKey(this.privateKey.getPublicKeyAsHex()).build();

    String headerSignature = SawtoothSigner.signHexSequence(privateKey, batchHeader.toByteArray());

    Batch.Builder batchBuilder = Batch.newBuilder().setHeader(batchHeader.toByteString())
        .setHeaderSignature(headerSignature)
        .addAllTransactions(transactionListBuilder.build().getTransactionsList());

    if (LOGGER.isTraceEnabled() || trace) {
      batchBuilder.setTrace(true);
    }

    return batchBuilder.build();
  }

}
