package sawtooth.sdk.reactive.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;
import sawtooth.sdk.reactive.tp.messaging.ReceivingHandler;
import sawtooth.sdk.reactive.tp.messaging.SenderAgent;

/**
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         A Network Node is an agent that holds the I/O of the messages to a Sawtooth Solution.
 * 
 *         It will store maps of current sockets and messages being worked on.
 *
 */
public class ReactorNetworkNode implements Runnable {

  private final static Logger LOGGER = Loggers.getLogger(ReactorNetworkNode.class);
  private String BACK_END_ADDRESS = "ipc://" + UUID.randomUUID().toString();
  private String NODE_IDENTIFICATION;

  private List<Socket> workersSockets = new ArrayList<>();
  private List<PollItem> pollers = new ArrayList<>();

  // Correlation IDs of messages being worked on
  private final Map<String, String> corrIDsAtWork = new ConcurrentHashMap<String, String>();

  // Messages waiting responses
  private final Map<String,CompletableFuture<Message>> expectingResponses = new ConcurrentHashMap<String,CompletableFuture<Message>>();

  private int threadCount = 4;
  private ReactorCoreProcessor localProcessor;

  // Emitter for messages we want to send - only put it in the localProcessor.getSenderProcessor().
  private EmitterProcessor<Message> senderEmitter = EmitterProcessor.<Message>create();

  // Emitter for received messages - put either it in the localProcessor.getReceiverProcessor() or
  // sends to EOL task processor.
  private EmitterProcessor<Message> receivingEmitter = EmitterProcessor.<Message>create();

  // Emitter for processed messages - only put it in the localProcessor.getSenderProcessor().
  private EmitterProcessor<Message> transformationEmitter = EmitterProcessor.<Message>create();

  private ExecutorService tPoll = Executors.newWorkStealingPool(3);
  
  private Disposable workingFunctionSubscription = null;

  ZContext context = new ZContext();

  // The socket visible to the TCP protocol
  Socket frontendSocket;

  // Socket to work on the internal agents
  Socket backEndSocket;

  // Address of the frontEndSocket
  String mqMainAddress;
  
  byte[] remoteRouterID = null;
  ZLoop looper;
  private Boolean server = false;

  public ReactorNetworkNode(String mqAddress, int parallelismFactor, String name,
      Boolean isServer) {
   
    this.mqMainAddress = mqAddress;
    this.threadCount = parallelismFactor;
    this.NODE_IDENTIFICATION = name;
    this.server = isServer;
    localProcessor =
        new ReactorCoreProcessor(threadCount, Queues.SMALL_BUFFER_SIZE, this.NODE_IDENTIFICATION);
    
    senderEmitter
      .log()
      .subscribeOn(Schedulers.parallel(),false)
      .subscribe(localProcessor.getSenderProcessor());
    
    workingFunctionSubscription  = this.transformationEmitter
        .doOnNext(s -> LOGGER.debug("We will only pass the message with CID {} ...", s.getCorrelationId()))
        .publish()
        .autoConnect()
        .subscribe( cs ->{
          LOGGER.debug("senderEmitter.onNext({}) ...", cs.getCorrelationId());
          senderEmitter.onNext(cs);
     });
    
    
  }

  
  @Override
  public void run() {

    backEndSocket = context.createSocket(ZMQ.DEALER);
    backEndSocket.setLinger(0);
    backEndSocket.setImmediate(false);
    backEndSocket.setIdentity((NODE_IDENTIFICATION + "_Backend").getBytes());
    
    frontendSocket = context.createSocket(ZMQ.ROUTER);
    // To user ROUTER, we need or to implement https://github.com/zeromq/pyzmq/issues/974 or know the ID of the server socket... 
    frontendSocket.setLinger(0);
    frontendSocket.setImmediate(false);
    frontendSocket.setProbeRouter(true);
    frontendSocket.setIdentity((this.getClass().getName() + UUID.randomUUID().toString()).getBytes());
    
   
    if (server) {
      
      LOGGER.debug(NODE_IDENTIFICATION + " Server mode");
      frontendSocket.setIdentity((this.getClass().getName() + UUID.randomUUID().toString()).getBytes());
      if (frontendSocket.bind(mqMainAddress)) {
        LOGGER.debug(NODE_IDENTIFICATION + " Server : Bound to " + mqMainAddress);
      }
    } else {
      LOGGER.debug(NODE_IDENTIFICATION + " Client mode");
      if (frontendSocket.connect(mqMainAddress)) {
        // wait for probe reply before sending
        ZMsg ack = ZMsg.recvMsg(frontendSocket);
        ack.dump(System.err);
        remoteRouterID = ack.getFirst().getData();
        LOGGER.debug("{} - Client : HEXA to {} -- {}" ,NODE_IDENTIFICATION,FormattingUtils.bytesToHex(remoteRouterID), remoteRouterID);
        LOGGER.debug(NODE_IDENTIFICATION + " Client : Connected to " + remoteRouterID);
      }
    }

    if (backEndSocket.bind(BACK_END_ADDRESS))
      LOGGER.debug(NODE_IDENTIFICATION + " Backend bound to " + BACK_END_ADDRESS);

    ZLoop looper = new ZLoop(context);
    
    ParallelFlux<Message> multipleSenderFlux = localProcessor
            .getOutgoingMessages()
            .parallel(threadCount)
            .runOn(Schedulers.parallel());
    
    IntStream.range(1, threadCount + 1).forEach(n -> {
      LOGGER.debug(NODE_IDENTIFICATION+" - Creating individual Worker " + n);
      Socket worker = context.createSocket(ZMQ.DEALER);
      workersSockets.add(worker);
      worker.setLinger(0);
      worker.setImmediate(false);
      worker.setIdentity((this.NODE_IDENTIFICATION + "_" + n).getBytes());
      if (worker.connect(BACK_END_ADDRESS)) {
        LOGGER.debug(" - > {}_{} connected on {}" ,this.NODE_IDENTIFICATION,n,BACK_END_ADDRESS);
      }
      PollItem pi = new PollItem(worker, ZMQ.Poller.POLLIN);
      pollers.add(pi);

      ReceivingHandler mesgReceiver =
          new ReceivingHandler(receivingEmitter, this.NODE_IDENTIFICATION, n, corrIDsAtWork);
      looper.addPoller(pi, mesgReceiver, null);
      
      multipleSenderFlux
        .groups()
        .elementAt(n-1)
        .doOnNext( msf -> {
          LOGGER.debug(" - > {}_{} subscribed on Paraller Flux #{}" ,this.NODE_IDENTIFICATION,n, msf.key());
        })
        .block()
        .subscribe(new SenderAgent(n, worker, this.NODE_IDENTIFICATION, remoteRouterID, corrIDsAtWork));

    });
    
    receivingEmitter
        .publish()
        .autoConnect()
        .log()
        .subscribe(internalPlexer);
    
    frontendSocket.monitor("inproc://"+BACK_END_ADDRESS+"monitor.s", ZMQ.EVENT_DISCONNECTED);
    final ZMQ.Socket monitor = this.context.createSocket(ZMQ.PAIR);
    monitor.connect("inproc://"+BACK_END_ADDRESS+"monitor.s");
    
    tPoll.submit(() -> {
      while (true) {
        // blocks until disconnect event recieved
        ZMQ.Event event = ZMQ.Event.recv(monitor);
        if (event.getEvent() == ZMQ.EVENT_DISCONNECTED) {
          LOGGER.warn("Validator closed connection...");
          expectingResponses.values().forEach(er ->{
            er.completeExceptionally(new Throwable("Validator Disconnected."));
          });
        }
      }
    });
    
    tPoll.submit(() -> {
      LOGGER.debug(this.NODE_IDENTIFICATION+" Starting proxy... ");
      ZMQ.proxy(frontendSocket, backEndSocket, null);
      LOGGER.debug(this.NODE_IDENTIFICATION+" Proxy stopped.");
    });
  
    
    tPoll.submit(() -> {
      LOGGER.debug(this.NODE_IDENTIFICATION+" Starting to poll... ");
      looper.start();
      LOGGER.debug(this.NODE_IDENTIFICATION+" Poll stopped.");
    });
  }
  

  public void setWorkingFunction(Function<Message, Message> mesgFunction) {
    LOGGER.debug(this.NODE_IDENTIFICATION+" Setting new Message Function.");
    
    this.workingFunctionSubscription.dispose();
    
    this.transformationEmitter = EmitterProcessor.<Message>create();
    
    workingFunctionSubscription  = this.transformationEmitter
      .doOnNext(s -> LOGGER.debug("We will transform the message with CID {} ...", s.getCorrelationId()))
      .map(mesgFunction)
      .doOnNext(s -> LOGGER.debug("We transformed the message with CID {} ...", s.getCorrelationId()))
      .publish()
      .autoConnect()
      .subscribe( cs ->{
        LOGGER.debug("senderEmitter.onNext({}) ...", cs.getCorrelationId());
        senderEmitter.onNext(cs);
      });
  }
  
  public void sendMessage(Message toSend) {
    LOGGER.debug(NODE_IDENTIFICATION+" - Starting message cycle for {}...",toSend.getCorrelationId());
    senderEmitter.onNext(toSend);
    CompletableFuture<Message> awaitingOne = new CompletableFuture<Message>();
    expectingResponses.put(toSend.getCorrelationId(),awaitingOne);
    awaitingOne.whenComplete( (rs,ex) -> {
      if (ex != null) {
        LOGGER.error("Error on processing {} from awating map .",awaitingOne);
      } else {
        LOGGER.debug("Removing Message {} from awating map .",rs.getCorrelationId());
        expectingResponses.remove(rs.getCorrelationId());
      }
    });
    LOGGER.debug("{} - responseEmitter.onNext({})- done",NODE_IDENTIFICATION,toSend.getCorrelationId());
  }
  
  public CompletableFuture<Message> waitForMessage(String correlationID) {
    return expectingResponses.get(correlationID);
  }
  
  public final Flux<Message> getIncomingFlux() {
    return localProcessor.getIncomingMessages().share();
  }
  
  public final Flux<Message> getOutgoingFlux() {
    return localProcessor.getOutgoingMessages().share();
  }
  
  private Consumer<Message> internalPlexer = new Consumer<Message>() {
    @Override
    public void accept(Message mesg) {
        LOGGER.debug(NODE_IDENTIFICATION+" - Receiving Emitter consumed "+mesg.getCorrelationId());
        if (expectingResponses.containsKey(mesg.getCorrelationId())) {
          LOGGER.debug(NODE_IDENTIFICATION+" - Ending message cycle for {}.", mesg.toString());
          expectingResponses.get(mesg.getCorrelationId()).complete(mesg);
        } else if (corrIDsAtWork.containsKey(mesg.getCorrelationId())) {
          LOGGER.debug(NODE_IDENTIFICATION+" - Continuing message cycle for {}" ,mesg.toString());
          transformationEmitter.onNext(mesg);
          LOGGER.debug(NODE_IDENTIFICATION+" - Sent {} to the message function",mesg.getCorrelationId());
        } 
    }
  };

}
