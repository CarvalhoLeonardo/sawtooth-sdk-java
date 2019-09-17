package sawtooth.sdk.reactive.tp.transport.zmq;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import sawtooth.sdk.protobuf.Message;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class will consume Messages from the external bound Flux.
 */
public class SenderAgent implements Consumer<Message> {

  /**
   * Our ubiquitous Logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(SenderAgent.class);

  final Map<String, byte[]> addressCorrelationMapping;
  final byte[] externalSocketId;
  final String myName;
  final ZMQ.Socket socket;

  public SenderAgent(final int newId, final ZMQ.Socket senderSocket, final String prefix,
      final byte[] externalrouterID, final Map<String, byte[]> externaCorrelationMapping) {
    myName = prefix + "_Sender_" + newId;
    externalSocketId = externalrouterID;
    socket = senderSocket;
    this.addressCorrelationMapping = externaCorrelationMapping;
    LOGGER.debug(myName + " created.");
  }

  @Override
  public final void accept(final Message sawtoothMessage) {
    LOGGER.debug(myName + " Accepting...");

    ZMsg msg = new ZMsg();
    if (addressCorrelationMapping.containsKey(sawtoothMessage.getCorrelationId())) {
      // We are answering a message, we need to address it!
      LOGGER.debug("{} It's a response to socket ID {}", myName,
          new String(addressCorrelationMapping.get(sawtoothMessage.getCorrelationId())));
      msg.offer(new ZFrame(addressCorrelationMapping.get(sawtoothMessage.getCorrelationId())));
      addressCorrelationMapping.remove(sawtoothMessage.getCorrelationId());
    } else {
      LOGGER.debug(myName + " It's a request from us.");
      if (externalSocketId != null && !(externalSocketId.length == 0)) {
        LOGGER.debug(" External socket ID : {}.", new String(externalSocketId));
        /**
@formatter:off
     msg.wrap(new ZFrame(externalSocketId)) gets the error
     [InterconnectThread-1] interconnect ERROR] Received a message on address tcp://0.0.0.0:4004 that caused an error:
     too many values to unpack (expected 2)
     Traceback (most recent call last):
     File "/usr/lib/python3/dist-packages/sawtooth_validator/networking/interconnect.py", line 344, in _receive_message
            yield from self._socket.recv_multipart()
        ValueError: too many values to unpack (expected 2)
@formatter:on
         */
        msg.offer(new ZFrame(externalSocketId));
      }
    }

    LOGGER.debug("{} sent through socket {} to route {}", myName, new String(socket.getIdentity()),
        new String(externalSocketId));
    msg.offer(new ZFrame(sawtoothMessage.toByteArray()));
    if (msg.send(socket, false)) {
      LOGGER.debug(myName + " Sent " + sawtoothMessage.toString());
      msg.dump(System.err);
    } else {
      LOGGER.error(myName + " couldn't sent " + sawtoothMessage.toString());
    }
  }

}
