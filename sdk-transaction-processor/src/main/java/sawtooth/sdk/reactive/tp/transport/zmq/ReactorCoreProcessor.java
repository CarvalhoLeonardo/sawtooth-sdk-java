package sawtooth.sdk.reactive.tp.transport.zmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.WorkQueueProcessor;
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

  private final WorkQueueProcessor<Message> receiverProcessor;
  ExecutorService receivingExecutorService;

  ThreadFactory receivingTF;

  private final WorkQueueProcessor<Message> senderProcessor;

  ExecutorService sendingExecutorService;

  ThreadFactory sendingTF;

  public ReactorCoreProcessor(final int parallelism, final int queuesBufferSize,
      final String identity, final int timeOutTaskMillis) {
    parallelRails = parallelism;
    queuesDepth = queuesBufferSize;
    this.processorName = identity;
    receivingTF = new InternalThreadFactory(processorName, "receive");
    sendingTF = new InternalThreadFactory(processorName, "send");

    receivingExecutorService = new ThreadPoolExecutor(1, parallelism, timeOutTaskMillis,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(parallelism), receivingTF);
    sendingExecutorService = new ThreadPoolExecutor(1, parallelism, timeOutTaskMillis,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(parallelism), sendingTF);

    internalTaskExecutorService = Executors.newWorkStealingPool(parallelRails);

    receiverProcessor = internalWQPBuilder(queuesBufferSize, receivingExecutorService,
        internalTaskExecutorService);

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

    receiverProcessor.publish()
        // .onErrorContinue(incomingErrorConsumer)
        .log();

    senderProcessor = internalWQPBuilder(queuesBufferSize, sendingExecutorService,
        internalTaskExecutorService);

    if (LOGGER.isTraceEnabled()) {
      senderProcessor
          .doOnNext(
              s -> LOGGER.debug(identity + " InQueue : I just see the Correlation ID {} passing...",
                  s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity + " InQueue : Got a subscription from  {}.",
              s.getClass().getSimpleName()));
    }

    senderProcessor.publish().log();

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

  public final WorkQueueProcessor<Message> getReceiverProcessor() {
    return receiverProcessor;
  }

  public final WorkQueueProcessor<Message> getSenderProcessor() {
    return senderProcessor;
  }

  private WorkQueueProcessor<Message> internalWQPBuilder(int queueSize, ExecutorService execServ,
      ExecutorService taskExecutor) {
    return WorkQueueProcessor.<Message>builder().bufferSize(queueSize).executor(execServ)
        .requestTaskExecutor(taskExecutor).share(true).build();
  }

}
