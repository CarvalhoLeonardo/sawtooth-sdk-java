package sawtooth.sdk.reactive.tp.errorprocessor;

import java.util.function.BiConsumer;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;

import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpProcessResponse.Status;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class will handle upstream errors without terminating the mapped Flows
 *
 */
public class StreamErrorProcessor implements BiConsumer<Throwable, Object> {

  private final static Logger LOGGER = Loggers.getLogger(StreamErrorProcessor.class);

  /**
   * This will be the destination of the created Messages holding the error information
   */
  final FluxSink<Message> internalDispatcher;

  public StreamErrorProcessor(FluxSink<Message> destinationFlow) {
    super();
    this.internalDispatcher = destinationFlow;
  }

  @Override
  public void accept(Throwable throwable, Object msg) {
    if (msg instanceof GeneratedMessageV3) {
      LOGGER.error("Error on Stream, Message {}, Throwable {}",
          ((GeneratedMessageV3) msg).toString(), throwable.toString());
      TpProcessResponse.Builder responseBuilder = TpProcessResponse.newBuilder();
      try {
        responseBuilder.mergeFrom(((GeneratedMessageV3) msg).toByteArray());
        responseBuilder.setStatus(Status.INTERNAL_ERROR);
        responseBuilder.setMessage(throwable.getMessage());
        responseBuilder.clear();
        internalDispatcher.next(Message.parseFrom(responseBuilder.build().toByteArray()));
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

    } else {
      LOGGER.error("Error on Stream, Object {}, Throwable {}", msg.toString(),
          throwable.toString());
      throwable.printStackTrace();
    }

  }

}
