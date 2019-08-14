package sawtooth.sdk.reactive.tp.stress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.google.protobuf.InvalidProtocolBufferException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.factory.FamilyRegistryMessageFactory;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This class intends to mimic a validator, but sending a LOT of messages.
 *
 */
public class StressValidator implements Runnable {
  private class InternalHandler implements IZLoopHandler {
    FluxSink<Message> emitter;
    String idSender;
    private boolean registered = false;

    public InternalHandler(FluxSink<Message> emitter) {
      super();
      this.emitter = emitter;
    }

    @Override
    public int handle(ZLoop loop, PollItem item, Object arg) {
      LOGGER.debug("HANDLE :: ");
      ZMsg receivedMessage = ZMsg.recvMsg(item.getSocket());
      LOGGER.debug("RECEIVE - Size : " + receivedMessage.size());
      receivedMessage.dump(System.out);
      ZFrame identityFrame = receivedMessage.pop();
      idSender = identityFrame.toString();
      LOGGER.debug("Received the Validator ID " + idSender);
      Message sawtoothMessage;
      try {
        sawtoothMessage = Message.parseFrom(receivedMessage.getLast().getData());
        Message returned = generateResponse(identityFrame.getData(), sawtoothMessage);
        if (registered) {
          LOGGER.debug("Consuming the Message " + sawtoothMessage.toString());
          emitter.next(sawtoothMessage);
        } else {
          LOGGER.debug("Received the Message " + sawtoothMessage.toString() + " sending answer "
              + returned.toString());
          if (returned != null) {
            ZMsg outMessage = new ZMsg();
            ByteArrayOutputStream mesgBucket = new ByteArrayOutputStream();
            returned.writeDelimitedTo(mesgBucket);
            mesgBucket.flush();
            // outMessage.add(new ZFrame(mesgBucket.toByteArray()));
            outMessage.add(returned.toByteArray());
            outMessage.wrap(identityFrame);
            outMessage.dump(System.out);
            if (outMessage.send(item.getSocket())) {
              LOGGER.debug(
                  "Register response sent with correlation id " + returned.getCorrelationId());
              registered = true;
              LOGGER.debug("Registering Subscriber for " + idSender);

              sendProcessor.subscribe(new SenderAgent(1, idSender.getBytes()));

              LOGGER.debug("Subscriber " + idSender + " -- subscribed");
              receivedMessage.destroy();
              outMessage.destroy();
            }
          }

        }
      } catch (IOException e) {
        e.printStackTrace();
        return -1;
      }
      return 0;
    }

  }

  /**
   *
   * This Consumer will feed from the Random Message Flux and send to the TP, and at the same time
   * prepare a Consumer to receive the reply.
   *
   * @param id - id of the Agent
   * @param tpSocketId - Socket ID from the connection of the TP
   * @return - The Consumer.
   */
  private class SenderAgent implements Consumer<Message> {

    ZFrame addressFrame;
    String myName;
    ZMsg outputMesg = new ZMsg();

    public SenderAgent(int newId, byte[] tpSocketId) {
      myName = "Sender " + newId;
      addressFrame = new ZFrame(tpSocketId);
    }

    @Override
    public void accept(Message sawtoothMessage) {
      outputMesg.offer(new ZFrame(sawtoothMessage.toByteArray()));
      outputMesg.wrap(addressFrame);
      LOGGER.debug(myName + " Sending...");
      if (outputMesg.send(serverSocket)) {
        sentCounter.incrementAndGet();
        LOGGER.debug(myName + " MessageSender :: Sent " + sentCounter.get());
        /*
         * receiveProcessor.filter(p -> { return
         * p.getCorrelationId().equalsIgnoreCase(sawtoothMessage.getCorrelationId());
         * }).single().subscribe(replyConsumer());
         */
      }

      outputMesg.clear();
    }
  }

  private static int latencyInMilisseconds = 100;
  private final static Logger LOGGER = LoggerFactory.getLogger(StressValidator.class);
  public static AtomicInteger sentCounter = new AtomicInteger(0);
  ZContext context = new ZContext();
  Flow.Publisher<Message> echoListener;
  CoreMessagesFactory internalCoreMF;
  MessageGenerator internalMGenerator;
  FamilyRegistryMessageFactory internalRegistryMF;
  TransactionFamily internalTF;

  ExecutorService localExecutor;

  InternalHandler mesgReceiver;
  String mqServerAddress;
  private Flux<Message> publisherAgent;
  private WorkQueueProcessor<Message> receiveProcessor;

  private WorkQueueProcessor<Message> sendProcessor;

  Socket serverSocket;

  public StressValidator(TransactionFamily source, String mqAddress)
      throws NoSuchAlgorithmException {
    LOGGER.debug("Registering Message Factory of family " + source.getFamilyName());
    this.internalTF = source;
    this.mqServerAddress = mqAddress;
    this.internalCoreMF = new CoreMessagesFactory();
    this.internalRegistryMF = new FamilyRegistryMessageFactory(null, source);
    internalMGenerator = new MessageGenerator(5);
    internalMGenerator.startFluxFor(source);
    localExecutor = Executors.newWorkStealingPool();

    sendProcessor = WorkQueueProcessor.<Message>builder().bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutor).share(true).waitStrategy(WaitStrategy.yielding()).build();

    receiveProcessor = WorkQueueProcessor.<Message>builder().bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutor).share(true).waitStrategy(WaitStrategy.yielding()).build();

    publisherAgent = internalMGenerator.getMessagesflux(1000L);
    localExecutor.submit(() -> {
      publisherAgent.subscribeWith(sendProcessor);
    });

    // publisherAgent =
    // internalMGenerator.getMessagesflux(1000L).subscribeWith(receiveProcessor).publish();

    LOGGER.debug("Publisher Agent bound...");
  }

  private Message generateResponse(byte[] socketID, Message request)
      throws InvalidProtocolBufferException {
    Message answer = null;
    switch (request.getMessageTypeValue()) {
    case Message.MessageType.PING_REQUEST_VALUE:
      LOGGER.debug("Answering PING_REQUEST ");
      answer = internalCoreMF.getPingResponse(request.getCorrelationId());
      break;
    case Message.MessageType.PING_RESPONSE_VALUE:
      LOGGER.debug("Receiving PING_RESPONSE");
      return null;
    case Message.MessageType.TP_REGISTER_REQUEST_VALUE:

      receiveRegisterRequest(socketID, TpRegisterRequest.parseFrom(request.getContent()));

      answer = internalRegistryMF.getRegisterResponse(TpRegisterResponse.Status.OK_VALUE,
          request.getCorrelationId());

      LOGGER.debug("Answering REGISTER_REQUEST with correlation ID " + request.getCorrelationId()
          + " with " + answer.toString());
      break;
    }

    return answer;
  }

  /**
   * This is where the Consumer for the TP is created.
   *
   * @param req
   * @throws InvalidProtocolBufferException
   */
  private void receiveRegisterRequest(byte[] socketID, TpRegisterRequest req)
      throws InvalidProtocolBufferException {
    LOGGER.debug("Registering Message Factory of family " + req.getFamily());
    if (!this.internalTF.getFamilyName().equalsIgnoreCase(req.getFamily())
        && this.internalTF.getFamilyVersion().equalsIgnoreCase(req.getVersion())) {
      throw new InvalidProtocolBufferException("Wrong TP version received !");

    }
  }

  /**
   *
   * This consumer will await for the responses for the messages sent to the TP.
   *
   * @param correlationId - the correlation to expect.
   * @return
   */
  private Consumer<String> replyConsumer() {
    return new Consumer<String>() {

      @Override
      public void accept(String correlationId) {
        LOGGER.debug(" Received Correlation ID " + correlationId);

      }
    };
  };

  @Override
  public void run() {
    serverSocket = context.createSocket(ZMQ.ROUTER);

    if (serverSocket.bind(mqServerAddress))
      LOGGER.debug("Bound to " + mqServerAddress);

    ZLoop looper = new ZLoop(context);
    PollItem pooler = new PollItem(serverSocket, ZMQ.Poller.POLLIN);

    Flux<Message> echoer = Flux.<Message>create(receiver -> {
      mesgReceiver = new InternalHandler(receiver);
      looper.addPoller(pooler, mesgReceiver, null);
    }).share();

    echoer.subscribeWith(receiveProcessor);

    LOGGER.debug("Starting to poll... ");

    looper.start();

  }

}
