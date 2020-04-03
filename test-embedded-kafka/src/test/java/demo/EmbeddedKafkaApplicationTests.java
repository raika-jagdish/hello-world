/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.waitAtMost;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Test class demonstrating how to use an embedded kafka service with the
 * kafka binder.
 *
 * @author Gary Russell
 * @author Soby Chacko
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { EmbeddedKafkaApplicationTests.App.class} )
@EnableBinding(Source.class)
public class EmbeddedKafkaApplicationTests {

	private static final String INPUT_TOPIC = "testEmbeddedIn";

	@Autowired
	ProducerHandler producer;

	@Autowired
	ConsumerHandler consumerHandler;
	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, INPUT_TOPIC);

	@BeforeClass
	public static void setup() {
		System.setProperty("spring.cloud.stream.kafka.binder.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getEmbeddedKafka()
			.getZookeeperConnectionString());
		System.setProperty("spring.cloud.stream.bindings.input.destination", INPUT_TOPIC);

		System.setProperty("spring.cloud.stream.bindings.input.content-type", "text/plain");

		System.setProperty("spring.cloud.stream.bindings.input.group", "input-group-1");

		System.setProperty("spring.cloud.stream.bindings.output.destination", INPUT_TOPIC);

		System.setProperty("spring.cloud.stream.bindings.output.content-type", "text/plain");

		System.setProperty("spring.cloud.stream.bindings.output.group", "output-group-1");
		
		//if we add below  property, it will work.
		
		//System.setProperty("spring.cloud.function.definition", "func");
	}

	@Test
	public void testSendReceive() {
		Message<String> msg = MessageBuilder.withPayload("payload")
				.setHeader("payloadType", "simple")
				.build();
		producer.getSource()
		.output()
		.send(msg);

		waitAtMost(1, TimeUnit.SECONDS)
		.untilAsserted(() -> {
			then("payload").isEqualTo(
				EmbeddedKafkaApplicationTests.this.consumerHandler.recievedMsg);
		});

		waitAtMost(1, TimeUnit.SECONDS)
		.untilAsserted(() -> {
			then("simple").isEqualTo(
				EmbeddedKafkaApplicationTests.this.consumerHandler.header);
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
	static class App {}

}
