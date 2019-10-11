package sawtooth.sdk.reactive.tp.message.flow;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.tp.transport.zmq.ReactorNetworkNode;

public class ReactorStream extends MessagesStream implements Runnable {

  private final static Logger LOGGER = LoggerFactory.getLogger(ReactorStream.class);

  ReactorNetworkNode internalNode;
  private int parallelismFactor = 4;
  CompletableFuture<Boolean> started = new CompletableFuture<Boolean>();
  private String url = "";

  public ReactorStream(String url, int parallelismFactor) {
    super();
    this.url = url;
    this.parallelismFactor = parallelismFactor;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public byte[] getExternalContext() {
    return internalNode.getZMQRouterID();
  }

  public final CompletableFuture<Boolean> getStarted() {
    return started;
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
  public CompletableFuture<Message> receive(final String corlID) {
    return internalNode.waitForMessage(corlID);
  }

  @Override
  public CompletableFuture<Message> receive(final String corlID, final Duration timeout)
      throws TimeoutException {

    return internalNode.waitForMessage(corlID).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

  }

  @Override
  public void run() {
    this.setUp();
    internalNode.run();
    started.complete(Boolean.TRUE);
  }

  @Override
  public CompletableFuture<Message> send(final Message payload) {
    LOGGER.debug("Future<Message> Sending...");
    internalNode.sendMessage(payload);

    LOGGER.debug("Future<Message> Sent.");
    return CompletableFuture.completedFuture(payload);
  }

  @Override
  public void sendBack(final String correlationId, final Message payload) {
    send(payload);
  }

  /**
   * This is the function to process the stream of messages
   *
   * @param newTF
   */
  public void setTransformationFunction(Function<Message, Message> newTF) {
    this.internalNode.setWorkingFunction(newTF);
  }

  private void setUp() {
    internalNode = new ReactorNetworkNode(this.url, parallelismFactor, "reactorStream", false);
  }

}
