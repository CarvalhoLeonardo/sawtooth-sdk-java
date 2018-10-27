package sawtooth.sdk.tp.messaging;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.tp.core.ReactorNetworkNode;

public class ReactorStream extends MessagesStream implements Runnable {

  private final static Logger LOGGER = LoggerFactory.getLogger(ReactorStream.class);

  private String url = "";
  private int parallelismFactor = 4;
  ReactorNetworkNode internalNode;
  CompletableFuture<Boolean> started = new CompletableFuture<Boolean>();


  private void setUp() {
    internalNode = new ReactorNetworkNode(this.url, parallelismFactor, "reactorStream", false);
  }

  public ReactorStream(String url, int parallelismFactor) {
    super();
    this.url = url;
    this.parallelismFactor = parallelismFactor;
  }

  public void setTransformationFunction(Function<Message, Message> newTF) {
    this.internalNode.setWorkingFunction(newTF);
  }

  @Override
  public void run() {
    this.setUp();
    internalNode.run();
    started.complete(Boolean.TRUE);
  }

  @Override
  public Future<Message> send(Message payload) {
    LOGGER.debug("Future<Message> Sending...");
    internalNode.sendMessage(payload);

    LOGGER.debug("Future<Message> Sent.");
    return CompletableFuture.completedFuture(payload);
  }

  @Override
  public void sendBack(String correlationId, Message payload) {
    send(payload);
  }

  @Override
  public Future<Message> receive(String corlID) {
    return internalNode.waitForMessage(corlID);
  }

  @Override
  public Future<Message> receive(String corlID, Duration timeout) throws TimeoutException {

    return internalNode.waitForMessage(corlID).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

  }

  @Override
  public Flux<Message> listenToAllIncoming() {
    return internalNode.getIncomingFlux();
  }

  @Override
  public Flux<Message> listenToAllOutgoing() {
    return internalNode.getOutgoingFlux();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  public final CompletableFuture<Boolean> getStarted() {
    return started;
  }

}
