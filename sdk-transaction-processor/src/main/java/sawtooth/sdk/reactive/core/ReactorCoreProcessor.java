package sawtooth.sdk.reactive.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         This class will use Reactor patterns to create a basic I/O to handle cycles of messaging.
 * 
 *         Both the Transaction Processors and the Validators would use this as a core component.
 *
 */
public class ReactorCoreProcessor {

  private final static Logger LOGGER = Loggers.getLogger(ReactorCoreProcessor.class);

  private final WorkQueueProcessor<Message> senderProcessor;
  private final WorkQueueProcessor<Message> receiverProcessor;

  private final ConnectableFlux<Message> outgoingMessages;
  private final ConnectableFlux<Message> incomingMessages;

  ExecutorService receivingExecutorService;
  ExecutorService sendingExecutorService;
  
  ThreadFactory receivingTF;
  ThreadFactory sendingTF;
  
  int parallelRails;
  int queuesDepth;

  private final String processorName;
  
  private class InternalThreadFactory implements ThreadFactory,Supplier<String>{

    final String threadPrefix;
    final String qName;
    final AtomicInteger tCounter = new AtomicInteger(1);
    
    public InternalThreadFactory(String threadPrefix,String queueName) {
      super();
      this.threadPrefix = threadPrefix;
      this.qName = queueName;
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, threadPrefix+"_"+qName+"_"+tCounter.getAndIncrement());
    }

    @Override
    public String get() {
      return threadPrefix+"_"+tCounter.get();
    }
    
  }
  
  public ReactorCoreProcessor(int parallelism, int queuesBufferSize, String identity) {
    parallelRails = parallelism;
    queuesDepth = queuesBufferSize;
    this.processorName = identity;
    receivingTF = new InternalThreadFactory(processorName,"receive");
    sendingTF = new InternalThreadFactory(processorName,"send");
    
    receivingExecutorService = new ThreadPoolExecutor(parallelism, parallelism * 2, 1000,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(parallelism * 2), receivingTF);
    sendingExecutorService = new ThreadPoolExecutor(parallelism, parallelism * 2, 1000,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(parallelism * 2), sendingTF);
    
    receiverProcessor = WorkQueueProcessor.<Message>builder().bufferSize(queuesDepth)
        .executor(receivingExecutorService)
        .requestTaskExecutor(receivingExecutorService)
        .share(true)
        .build();

    if (LOGGER.isTraceEnabled()) {
        receiverProcessor
          .doOnNext(s -> LOGGER.debug(identity+" InQueue : I just see the Correlation ID {} passing...", s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity+" InQueue : Got a subscription from  {}.", s.getClass().getSimpleName()));
    }
    
    receiverProcessor
        .publish()
        .log();
    
    senderProcessor = WorkQueueProcessor.<Message>builder().bufferSize(queuesDepth)
        .executor(sendingExecutorService)
        .requestTaskExecutor(sendingExecutorService)
        .share(true)
        .build();
    
    if (LOGGER.isTraceEnabled()) {
      senderProcessor
          .doOnNext(s -> LOGGER.debug(identity+" InQueue : I just see the Correlation ID {} passing...", s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity+" InQueue : Got a subscription from  {}.", s.getClass().getSimpleName()));
    }
    
    senderProcessor
        .publish()
        .log();
    
    incomingMessages = receiverProcessor.share().publish();
    
    if (LOGGER.isTraceEnabled()) {
      incomingMessages
        .doOnNext(s -> LOGGER.debug(identity+" InFlux : I just see the Correlation ID {} passing...", s.getCorrelationId()))
        .doOnSubscribe(s -> LOGGER.debug(identity+" InFlux : Got a subscription from  {}.", s.getClass().getSimpleName()));
    }
    
    incomingMessages.subscribe();
    incomingMessages.connect();
    
    outgoingMessages = senderProcessor.share().publish();
     
    if (LOGGER.isTraceEnabled()) {
      outgoingMessages
          .doOnNext(s -> LOGGER.debug(identity+" OutFlux : I just see the Correlation ID {} passing...", s.getCorrelationId()))
          .doOnSubscribe(s -> LOGGER.debug(identity+" OutFlux : Got a subscription from  {}.", s.getClass().getSimpleName()));
    }
    
    outgoingMessages.subscribe();
    outgoingMessages.connect();

    if (LOGGER.isTraceEnabled()) {
      SignalType[] allSignals = {SignalType.ON_NEXT, SignalType.ON_SUBSCRIBE,
          SignalType.ON_COMPLETE, SignalType.ON_ERROR};
      senderProcessor.log(Loggers.getLogger(ReactorCoreProcessor.class), Level.ALL, true,
          allSignals);
      receiverProcessor.log(Loggers.getLogger(ReactorCoreProcessor.class), Level.ALL, true,
          allSignals);
      incomingMessages.log("reactor.", Level.ALL, true, allSignals);
      outgoingMessages.log("reactor.", Level.ALL, true, allSignals);
    }

  }


  public final ConnectableFlux<Message> getOutgoingMessages() {
    return outgoingMessages;
  }

  public final ConnectableFlux<Message> getIncomingMessages() {
    return incomingMessages;
  }

  public final WorkQueueProcessor<Message> getSenderProcessor() {
    return senderProcessor;
  }

  public final WorkQueueProcessor<Message> getReceiverProcessor() {
    return receiverProcessor;
  }

}
