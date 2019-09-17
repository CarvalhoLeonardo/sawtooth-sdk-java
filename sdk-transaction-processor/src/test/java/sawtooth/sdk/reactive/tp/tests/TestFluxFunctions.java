package sawtooth.sdk.reactive.tp.tests;

import static org.testng.Assert.assertEquals;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.stress.MessageGenerator;

@Test
public class TestFluxFunctions {

  class SubscriberWithBackPressure<T> extends BaseSubscriber<T> {
    private final Consumer<T> consumer;
    private Subscription internalsub;
    private final int maxRequest;

    SubscriberWithBackPressure(int maxRequest, Consumer<T> consumer) {
      this.maxRequest = maxRequest;
      this.consumer = consumer;
    }

    @Override
    protected void hookOnNext(T value) {
      this.consumer.accept(value);
      internalsub.request(1);
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      internalsub = subscription;
      internalsub.request(maxRequest);
    }

  }

  private class TestFluxReturn implements Consumer<Message> {

    FluxSink<Message> internalSink;

    public TestFluxReturn(FluxSink<Message> internalSink) {
      super();
      this.internalSink = internalSink;
    }

    @Override
    public void accept(Message consumed) {
      LOGGER.debug("Consumed " + consumed.getCorrelationId());
      internalSink.next(consumed);
    }

  }

  private static long delay = 10;

  private final static Logger LOGGER = LoggerFactory.getLogger(TestFluxFunctions.class);

  CoreMessagesFactory cmf;

  MessageGenerator messageGenerator;
  TransactionFamily myTF;

  Function<Flux<Message>, Publisher<Message>> getNewReceiver(List<Character> patterns,
      List<Message> holder) {
    return new Function<Flux<Message>, Publisher<Message>>() {

      @Override
      public Publisher<Message> apply(Flux<Message> flux) {

        LOGGER.debug("Apply for " + patterns);
        flux.doOnNext(nx -> {
          if (patterns.contains(nx.getCorrelationId().charAt(0))) {
            LOGGER.debug(patterns + " - Received.");
            holder.add(nx);
            flux.skip(1L);
          }
        });
        return flux;
      }
    };
  }

  @BeforeClass
  public void setUp() throws NoSuchAlgorithmException {
    // Hooks.onNextDroppedFail();
    myTF = new TransactionFamily("messagetest", "0.0.0", new String[] { "messagetest" });
    cmf = new CoreMessagesFactory();
    messageGenerator = new MessageGenerator(4);
    messageGenerator.startFluxFor(myTF);
  }

  @Test
  public void testFilterManyMessagesByFilter()
      throws InvalidProtocolBufferException, InterruptedException, ExecutionException {

    CompletableFuture<Boolean> complete = new CompletableFuture<Boolean>();
    long MESSAGE_COUNTER = 100L;
    int BACKPRESSURE_REQ = 5;

    List<Character> group1 = Arrays.asList('a', 'b');
    List<Character> group2 = Arrays.asList('c', 'd');
    List<Character> group3 = Arrays.asList('e', 'f');

    List<Message> group1Messages = new ArrayList<>();
    List<Message> group2Messages = new ArrayList<>();
    List<Message> group3Messages = new ArrayList<>();
    List<Message> numberedMessages = new ArrayList<>();

    Flux<Message> messageSources = messageGenerator.getMessagesflux(MESSAGE_COUNTER)
        .delayElements(Duration.ofMillis(delay)).doOnComplete(() -> {
          LOGGER.debug("COMPLETE");
          complete.complete(Boolean.TRUE);
        }).doOnCancel(() -> {
          LOGGER.debug("CANCEL");
          complete.complete(Boolean.FALSE);
        });

    messageSources.subscribeOn(Schedulers.parallel()).subscribe(
        new SubscriberWithBackPressure<Message>(BACKPRESSURE_REQ, new Consumer<Message>() {
          @Override
          public void accept(Message m) {
            if (group3.contains(m.getCorrelationId().charAt(0))) {
              LOGGER.debug(group3 + " - Received " + m.getCorrelationId());
              group3Messages.add(m);
              return;
            }

            if (group1.contains(m.getCorrelationId().charAt(0))) {
              LOGGER.debug(group1 + " - Received " + m.getCorrelationId());
              group1Messages.add(m);
              return;
            }

            if (group2.contains(m.getCorrelationId().charAt(0))) {
              LOGGER.debug(group2 + " - Received " + m.getCorrelationId());
              group2Messages.add(m);
              return;
            }

            LOGGER.debug("Numbers - Received " + m.getCorrelationId());
            numberedMessages.add(m);

          }
        }));

    LOGGER.debug("Waiting...");
    complete.get();
    LOGGER.debug("Group 1 : " + group1Messages.size());
    LOGGER.debug("Group 2 : " + group2Messages.size());
    LOGGER.debug("Group 3 : " + group3Messages.size());
    LOGGER.debug("Group Numbers : " + numberedMessages.size());

    assertEquals(group1Messages.size() + group2Messages.size() + group3Messages.size()
        + numberedMessages.size(), MESSAGE_COUNTER);
  }

  @Test(enabled = false)
  public void testFilterManyMessagesByGroup()
      throws InvalidProtocolBufferException, InterruptedException, ExecutionException {

    CompletableFuture<Boolean> complete = new CompletableFuture<Boolean>();
    long MESSAGE_COUNTER = 100L;
    int BACKPRESSURE_REQ = 5;

    List<Character> group1 = Arrays.asList('a', 'b');
    List<Character> group2 = Arrays.asList('c', 'd');
    List<Character> group3 = Arrays.asList('e', 'f');

    List<Message> group1Messages = new ArrayList<>();
    List<Message> group2Messages = new ArrayList<>();
    List<Message> group3Messages = new ArrayList<>();
    List<Message> numberedMessages = new ArrayList<>();

    Flux<GroupedFlux<Integer, Message>> messageSources = messageGenerator
        .getMessagesflux(MESSAGE_COUNTER).delayElements(Duration.ofMillis(delay))
        .doOnComplete(() -> {
          LOGGER.debug("COMPLETE");
          complete.complete(Boolean.TRUE);
        }).doOnCancel(() -> {
          LOGGER.debug("CANCEL");
          complete.complete(Boolean.FALSE);
        }).groupBy(new Function<Message, Integer>() {
          @Override
          public Integer apply(Message fs) {
            if (group1.contains(fs.getCorrelationId().charAt(0))) {
              return 1;
            }
            if (group2.contains(fs.getCorrelationId().charAt(0))) {
              return 2;
            }
            if (group3.contains(fs.getCorrelationId().charAt(0))) {
              return 3;
            }
            return 0;
          }
        });

    messageSources.subscribeOn(Schedulers.parallel()).subscribe(cs -> {
      switch (cs.key()) {
      case 1:
        LOGGER.debug("Subscribing group 1...");
        cs.subscribe(
            new SubscriberWithBackPressure<Message>(BACKPRESSURE_REQ, new Consumer<Message>() {
              @Override
              public void accept(Message m) {
                LOGGER.debug(group1 + " - Received " + m.getCorrelationId());
                group1Messages.add(m);
              };
            }));
        break;
      case 2:
        LOGGER.debug("Subscribing group 2...");
        cs.subscribe(
            new SubscriberWithBackPressure<Message>(BACKPRESSURE_REQ, new Consumer<Message>() {
              @Override
              public void accept(Message m) {
                LOGGER.debug(group2 + " - Received " + m.getCorrelationId());
                group2Messages.add(m);
              };
            }));
        break;
      case 3:
        LOGGER.debug("Subscribing group 3...");
        cs.subscribe(
            new SubscriberWithBackPressure<Message>(BACKPRESSURE_REQ, new Consumer<Message>() {
              @Override
              public void accept(Message m) {
                LOGGER.debug(group3 + " - Received " + m.getCorrelationId());
                group3Messages.add(m);
              };
            }));
        break;
      default:
        LOGGER.debug("Subscribing group others...");
        cs.subscribe(
            new SubscriberWithBackPressure<Message>(BACKPRESSURE_REQ, new Consumer<Message>() {
              @Override
              public void accept(Message m) {
                LOGGER.debug("Others - Received " + m.getCorrelationId());
                group1Messages.add(m);
              };
            }));
      }

    });

    LOGGER.debug("Waiting...");
    complete.get();
    LOGGER.debug("Group 1 : " + group1Messages.size());
    LOGGER.debug("Group 2 : " + group2Messages.size());
    LOGGER.debug("Group 3 : " + group3Messages.size());
    LOGGER.debug("Group Numbers : " + numberedMessages.size());

    assertEquals(group1Messages.size() + group2Messages.size() + group3Messages.size()
        + numberedMessages.size(), MESSAGE_COUNTER);
  }

  @Test(dependsOnMethods = { "testReceiveOneMessage" })
  public void testFilterMessages() {
    Flux<Message> messageSource = messageGenerator.getMessagesflux(100L);
    Flux<Message> messageReceiver = Flux.<Message>create(receiver -> {
      messageSource.subscribe(new TestFluxReturn(receiver));
    });

    Message receivedAnya = messageReceiver.filterWhen(p -> {
      LOGGER.debug("Testing " + p.getCorrelationId());
      return Mono.just(p.getCorrelationId().contains("a"));
    }).blockFirst(Duration.ofSeconds(1));

    Message receivedAnyb = messageReceiver.filter(p -> p.getCorrelationId().contains("b"))
        .blockFirst(Duration.ofSeconds(1));

    Assert.assertNotNull(receivedAnya);
    Assert.assertTrue(receivedAnya.getCorrelationId().contains("a"));
    Assert.assertNotNull(receivedAnyb);
    Assert.assertTrue(receivedAnyb.getCorrelationId().contains("b"));
  }

  @Test(dependsOnMethods = { "testReceiveOneMessage" })
  public void testFilterUnorderedMessages() throws InvalidProtocolBufferException {
    List<Message> messagesToTest = new ArrayList<>();
    messageGenerator.getMessagesflux(delay).subscribe(messagesToTest::add);
    Message expectedMessage = cmf.getPingRequest();
    String expectedCID = expectedMessage.getCorrelationId();
    messagesToTest.add(4, expectedMessage);
    Flux<Message> messageSource = Flux.fromIterable(messagesToTest);

    Flux<Message> messageReceiver = Flux.<Message>create(receiver -> {
      messageSource.subscribe(new TestFluxReturn(receiver));
    });
    Message receivedMessage = messageReceiver
        .filter(p -> p.getCorrelationId().equalsIgnoreCase(expectedCID)).blockFirst();

    Assert.assertNotNull(receivedMessage);
    Assert.assertTrue(receivedMessage.getCorrelationId().equalsIgnoreCase(expectedCID));
  }

  @Test
  public void testReceiveOneMessage() {
    Disposable subscript;
    Flux<Message> messageSource = messageGenerator.getMessagesflux(1L);
    Flux<Message> messageReceiver = Flux.<Message>create(receiver -> {
      messageSource.subscribe(new TestFluxReturn(receiver));
    });
    subscript = messageReceiver.subscribe();
    Message received = messageReceiver.blockFirst(Duration.ofSeconds(1L));
    Assert.assertNotNull(received);
    subscript.dispose();

  }
}
