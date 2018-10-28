package sawtooth.sdk.tp.messaging;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import reactor.core.publisher.Flux;
import sawtooth.sdk.protobuf.Message;

public abstract class MessagesStream {

  /**
   * close the Stream.
   */
  public abstract void close();

  public abstract byte[] getExternalContext();

  /**
   * Acess the hot flux of messages being received
   *
   * @return the hot flux
   */
  public abstract Flux<Message> listenToAllIncoming();

  /**
   * Acess the hot flux of messages being sent
   *
   * @return the hot flux
   */
  public abstract Flux<Message> listenToAllOutgoing();

  /**
   * Get a message that is expected.
   *
   * @return result, a protobuf Message
   */
  public abstract Future<Message> receive(String corlID);

  /**
   * Get a message that has been received. If the timeout is expired, throws TimeoutException.
   *
   * @param timeout time to wait for a message.
   * @return result, a protobuf Message
   */
  public abstract Future<Message> receive(String corlID, Duration timeout) throws TimeoutException;

  /**
   * Send a message and return a Future that will later have the Bytestring.
   *
   * @param payload - the Message being sent
   * @return future a future that will hold the answer
   */
  public abstract Future<Message> send(Message payload);

  /**
   * Send a message without getting a future back. Useful for sending a response message to, for
   * example, a transaction
   *
   * @param correlationId a random string generated on the server for the client to send back
   * @param payload - the Message the server is expecting
   */
  public abstract void sendBack(String correlationId, Message payload);

}