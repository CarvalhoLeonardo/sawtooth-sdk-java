package sawtooth.sdk.reactive.tp.transport.zmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import sawtooth.sdk.protobuf.Message;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class will use Reactor patterns to create a basic I/O to handle cycles of messaging.
 *
 * Both the Transaction Processors and the Validators would use this as a core component.
 *
 * WorkQueueProcessor: "can also be replaced by equivalent constructs like EmitterProcessor with
 * publishOn and ParallelFlux with runOn."
 *
 */
public class ReactorCoreProcessor {

  private class InternalThreadFactory implements ThreadFactory, Supplier<String> {

    final private String qName;
    final private AtomicInteger tCounter = new AtomicInteger(1);
    final private String threadPrefix;

    InternalThreadFactory(final String threadPrefix, final String queueName) {
      super();
      this.threadPrefix = threadPrefix;
      this.qName = queueName;
    }

    @Override
    public String get() {
      return threadPrefix + "_" + tCounter.get();
    }

    @Override
    public Thread newThread(final Runnable r) {
      return new Thread(r, threadPrefix + "_" + qName + "_" + tCounter.getAndIncrement());
    }

  }

  private final static Logger LOGGER = Loggers.getLogger(ReactorCoreProcessor.class);
  private BiConsumer<Throwable, Object> incomingErrorConsumer;
  private final ConnectableFlux<Message> incomingMessages;
  ExecutorService internalTaskExecutorService;
  private BiConsumer<Throwable, Object> outgoingErrorConsumer;

  private final ConnectableFlux<Message> outgoingMessages;
  int parallelRails;

  private final String processorName;
  int queuesDepth;

  private final EmitterProcessor<Message> receiverProcessor;

  ThreadFactory receivingTF;

  private final EmitterProcessor<Message> senderProcessor;

  ExecutorService sendingExecutorService;

  ThreadFactory sendingTF;

  public ReactorCoreProcessor(final int parallelism, final int queuesBufferSize,
      final String identity, final int timeOutTaskMillis) {
    parallelRails = parallelism;
    queuesDepth = queuesBufferSize;
    this.processorName = identity;
    receivingTF = new InternalThreadFactory(processorName, "receive");
    sendingTF = new InternalThreadFactory(processorName, "send");

    internalTaskExecutorService = Executors.newWorkStealingPool(parallelRails);

    receiverProcessor = EmitterProcessor.<Message>create(queuesBufferSize);

    if (LOGGER.isTraceEnabled()) {
      receiverProcessor
          .doOnNext(
              s -> LOGGER.debug(identity + " InQueue : I just see the Correlation ID {} passing...",
                  s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity + " InQueue : Got a subscription from  {}.",
              s.getClass().getSimpleName()));
    }
    // incomingErrorConsumer = new StreamErrorProcessor(receiverProcessor.sink());
    // HERE

    receiverProcessor.publishOn(Schedulers.newParallel(parallelRails, receivingTF))
        // .onErrorContinue(incomingErrorConsumer)
        .log();

    senderProcessor = EmitterProcessor.<Message>create(queuesBufferSize);

    if (LOGGER.isTraceEnabled()) {
      senderProcessor
          .doOnNext(
              s -> LOGGER.debug(identity + " InQueue : I just see the Correlation ID {} passing...",
                  s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity + " InQueue : Got a subscription from  {}.",
              s.getClass().getSimpleName()));
    }

    senderProcessor.publishOn(Schedulers.newParallel(parallelRails, sendingTF)).log();

    incomingMessages = receiverProcessor.share()
        // .onErrorContinue(incomingErrorConsumer)
        .publish();

    if (LOGGER.isTraceEnabled()) {
      incomingMessages
          .doOnNext(
              s -> LOGGER.debug(identity + " InFlux : I just see the Correlation ID {} passing...",
                  s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity + " InFlux : Got a subscription from  {}.",
              s.getClass().getSimpleName()));
    }

    incomingMessages.subscribe();
    incomingMessages.connect();
    // outgoingErrorConsumer = new StreamErrorProcessor(senderProcessor.sink());
    // HERE
    outgoingMessages = senderProcessor
        // .onErrorContinue(outgoingErrorConsumer)
        .share().publish();

    if (LOGGER.isTraceEnabled()) {
      outgoingMessages
          .doOnNext(
              s -> LOGGER.debug(identity + " OutFlux : I just see the Correlation ID {} passing...",
                  s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity + " OutFlux : Got a subscription from  {}.",
              s.getClass().getSimpleName()));
    }

    outgoingMessages.subscribe();
    outgoingMessages.connect();

    if (LOGGER.isTraceEnabled()) {
      SignalType[] allSignals = { SignalType.ON_NEXT, SignalType.ON_SUBSCRIBE,
          SignalType.ON_COMPLETE, SignalType.ON_ERROR };
      senderProcessor.log(Loggers.getLogger(ReactorCoreProcessor.class), Level.ALL, true,
          allSignals);
      receiverProcessor.log(Loggers.getLogger(ReactorCoreProcessor.class), Level.ALL, true,
          allSignals);
      incomingMessages.log("reactor.", Level.ALL, true, allSignals);
      outgoingMessages.log("reactor.", Level.ALL, true, allSignals);
    }

  }

  public final ConnectableFlux<Message> getIncomingMessages() {
    return incomingMessages;
  }

  public final ConnectableFlux<Message> getOutgoingMessages() {
    return outgoingMessages;
  }

  public final EmitterProcessor<Message> getReceiverProcessor() {
    return receiverProcessor;
  }

  public final EmitterProcessor<Message> getSenderProcessor() {
    return senderProcessor;
  }

}
