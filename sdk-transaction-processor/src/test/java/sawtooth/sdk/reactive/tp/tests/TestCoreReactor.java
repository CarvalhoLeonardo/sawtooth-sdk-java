package sawtooth.sdk.reactive.tp.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.transport.zmq.ReactorCoreProcessor;

@Test
public class TestCoreReactor extends BaseTest {

  class InternalConsumer implements Consumer<Message> {

    private final List<Message> consumer;

    public InternalConsumer(List<Message> consumer) {
      super();
      this.consumer = consumer;
    }

    @Override
    public void accept(Message value) {
      if (consumer != null) {
        LOGGER.debug("Receiving correlation id {}... ", value.getCorrelationId());
        this.consumer.add(value);
      }
    }

  }

  private static final CoreMessagesFactory coreReactorTestFactory;
  static {
    CoreMessagesFactory tmp = null;
    try {
      tmp = new CoreMessagesFactory();
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    coreReactorTestFactory = tmp;
  }
  private EmitterProcessor<Message> emp = EmitterProcessor.<Message>create();

  private ReactorCoreProcessor reactor1 = new ReactorCoreProcessor(parallelFactor,
      Queues.SMALL_BUFFER_SIZE, "one", 1000);
  private ReactorCoreProcessor reactor2 = new ReactorCoreProcessor(parallelFactor,
      Queues.SMALL_BUFFER_SIZE, "two", 1000);

  /**
   *
   * configuration to short-circuit the ends, sending to reactor 1, looping on reactor 2, back on
   * reactor 1.
   */
  @BeforeClass
  public void setUp() {

    /*
     * The Reactor1 OutgoingFlux will submit it to the ReceiverProcessor on Reactor2...
     */
    ConnectableFlux<Message> outgoingOnR1 = reactor1.getOutgoingMessages();

    outgoingOnR1.doOnNext(s -> LOGGER.debug("R1 Out- I just see the Correlation ID {} passing...",
        s.getCorrelationId())).log().subscribe(reactor2.getReceiverProcessor());

    outgoingOnR1.connect();

    /*
     * ... so that the IncomingFlux will submit it to it's SenderProcessor ...
     */

    ConnectableFlux<Message> incomingOnR2 = reactor2.getIncomingMessages();

    incomingOnR2.doOnNext(s -> LOGGER.debug("R2 In- I just see the Correlation ID {} passing...",
        s.getCorrelationId())).log().subscribeWith(reactor2.getSenderProcessor());

    incomingOnR2.connect();

    /*
     * ... thus the Reactor2 OutGoing Flux can submit it to Reactor 1's ReceiveProcessor...
     */
    ConnectableFlux<Message> outgoingOnR2 = reactor2.getOutgoingMessages();

    outgoingOnR2.doOnNext(s -> LOGGER.debug("R2 Out- I just see the Correlation ID {} passing...",
        s.getCorrelationId())).log().subscribe(reactor1.getReceiverProcessor());

    outgoingOnR2.connect();
  }

  @AfterClass
  public void tearDown() {
    emp.onComplete();
  }

  /**
   * Let's send a hundred messages
   *
   * @throws InvalidProtocolBufferException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Test
  public void testALOTBackToBack()
      throws InvalidProtocolBufferException, InterruptedException, ExecutionException {

    final int alotMessages = 2000;

    Disposable emitterSubscription = emp
        .doOnNext(
            s -> LOGGER.debug("Emitting message with correlation ID {} ...", s.getCorrelationId()))
        .subscribeWith(reactor1.getSenderProcessor()).subscribe();

    final List<Message> allMessages = Collections.synchronizedList(new ArrayList<>());

    ConnectableFlux<Message> reference = reactor1.getIncomingMessages();

    reference.parallel(parallelFactor).runOn(Schedulers.parallel())
        .subscribe(new InternalConsumer(allMessages));

    IntStream.range(0, alotMessages).forEach(n -> {
      try {
        emp.onNext(coreReactorTestFactory.getPingRequest());
      } catch (InvalidProtocolBufferException e) {
      }
    });

    // 100 milisseconds or bust!!!
    Thread.sleep(100L);
    assertFalse(allMessages.isEmpty());
    assertEquals(allMessages.size(), alotMessages);

    emitterSubscription.dispose();
  }

  /**
   * Let's send a hundred messages
   *
   * @throws InvalidProtocolBufferException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Test(dependsOnMethods = { "testOneBackToBack" })
  public void testFourHundredBackToBack()
      throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
    Disposable emitterSubscription = emp
        .doOnNext(
            s -> LOGGER.debug("Emitting message with correlation ID {} ...", s.getCorrelationId()))
        .subscribeWith(reactor1.getSenderProcessor()).subscribe();

    final List<Message> allMessages = Collections.synchronizedList(new ArrayList<>());

    ConnectableFlux<Message> reference = reactor1.getIncomingMessages();

    reference.parallel(parallelFactor).runOn(Schedulers.parallel())
        .subscribe(new InternalConsumer(allMessages));

    IntStream.range(0, 400).forEach(n -> {
      try {
        emp.onNext(coreReactorTestFactory.getPingRequest());
      } catch (InvalidProtocolBufferException e) {
      }
    });

    // 100 milisseconds or bust!!!
    Thread.sleep(100L);
    assertFalse(allMessages.isEmpty());
    assertEquals(allMessages.size(), 400);

    emitterSubscription.dispose();
  };

  /**
   * Let's send one message
   *
   * @throws InvalidProtocolBufferException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Test
  public void testOneBackToBack()
      throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
    /*
     * Our emitter will submit a message to the SenderProcessor on Reactor1...
     */
    Disposable emitterSubscription = emp.doOnNext(s -> LOGGER
        .debug("EMITTER - I just see the Correlation ID {} going...", s.getCorrelationId()))
        .subscribeWith(reactor1.getSenderProcessor()).subscribe();

    final CompletableFuture<Message> answer = new CompletableFuture<Message>();

    Disposable oneMessageSubscription = reactor1.getIncomingMessages()
        .subscribe(new Consumer<Message>() {
          @Override
          public void accept(Message t) {
            LOGGER.debug("Hello Correlation ID {} ", t.getCorrelationId());
            answer.complete(t);
          }
        });

    emp.onNext(coreReactorTestFactory.getPingRequest());

    assertNotNull(answer.get());

    oneMessageSubscription.dispose();
    emitterSubscription.dispose();
  }

}
