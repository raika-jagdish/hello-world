package com.jd.scs;

import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.waitAtMost;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@RunWith(SpringRunner.class)
@SpringBootTest//(classes = { ScsFunctionDemoApplicationTests.App.class }, webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@EnableBinding(Source.class)
public class ScsFunctionDemoApplicationTests {

	private static final String TOPIC1 = "test-topic-1";

	// TODO: revisit this implementation
	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC1);

	@Autowired
	ProducerHandler producer;

	@Autowired
	ConsumerHandler consumerHandler;

	@BeforeClass
	public static void setup() {

		System.setProperty("spring.cloud.stream.kafka.binder.brokers", embeddedKafka.getEmbeddedKafka()
			.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getEmbeddedKafka()
			.getZookeeperConnectionString());
		System.setProperty("spring.cloud.stream.bindings.input.destination", TOPIC1);

		System.setProperty("spring.cloud.stream.bindings.input.content-type", "text/plain");

		System.setProperty("spring.cloud.stream.bindings.input.group", "input-group-1");

		System.setProperty("spring.cloud.stream.bindings.output.destination", TOPIC1);

		System.setProperty("spring.cloud.stream.bindings.output.content-type", "text/plain");

		System.setProperty("spring.cloud.stream.bindings.output.group", "output-group-1");
		
		//if we uncomment below property, application will not break
		//System.setProperty("spring.cloud.function.definition", "func");
	}

	@Test
	public void testThatCanInterceptEventFlow() throws InterruptedException {

		Message<String> msg = MessageBuilder.withPayload("payload")
				.setHeader("payloadType", "simple")
				.build();
		producer.getSource()
		.output()
		.send(msg);

		waitAtMost(1, TimeUnit.SECONDS)
		.untilAsserted(() -> {
			then("payload").isEqualTo(
				ScsFunctionDemoApplicationTests.this.consumerHandler.recievedMsg);
		});

		waitAtMost(1, TimeUnit.SECONDS)
		.untilAsserted(() -> {
			then("simple").isEqualTo(
				ScsFunctionDemoApplicationTests.this.consumerHandler.header);
		});
	}



	@EnableBinding(Sink.class)
	static class ConsumerHandler {

		String recievedMsg;
		String header;

		@StreamListener(Sink.INPUT)
		public void process(Message<String> message) {

			recievedMsg = message.getPayload();
			header = message.getHeaders().get("payloadType").toString();
		}
	}


	@EnableBinding(Source.class)
	static class ProducerHandler {

		private final Source source;

		ProducerHandler(Source source) {

			this.source = source;
		}

		Source getSource() {

			return source;
		}
	}

	@SpringBootApplication
	@Configuration
	static class App {
		
	@Bean
	public <K, V> Function<K, V> myFunction() {

		return v -> {
			return null;
		};
	}
	}
}