package com.example.redis;

import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.SessionAttributes;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

import static com.example.redis.RedisApplication.generateId;

@SpringBootApplication
public class RedisApplication {

	static Long generateId() {
		var tmp = new Random().nextLong();
		return Math.max(tmp, tmp * -1);
	}

	public static void main(String[] args) {
		SpringApplication.run(RedisApplication.class, args);
	}
}

// HTTP sessions
@Log4j2
@Controller
@SessionAttributes("cart")
@EnableRedisHttpSession
class HttpSessionConfig {

	@ModelAttribute("cart")
	ShoppingCart cart() {
		return new ShoppingCart();
	}

	@GetMapping("/orders")
	String orders(@ModelAttribute("cart") ShoppingCart cart, Model model) {

		cart.newOrder(new Order(RedisApplication.generateId(),
			new Date(), Collections.emptyList()));

		model.addAttribute("orders", cart.getOrders());
		return "orders";
	}

}

@Component
class ShoppingCart implements Serializable {

	private final Set<Order> orders = new HashSet<>();

	void newOrder(Order order) {
		this.orders.add(order);
	}

	Collection<Order> getOrders() {
		return this.orders;
	}
}

// repositories
@Configuration
@Log4j2
@RequiredArgsConstructor
class RepositoryConfig {

	private final LineItemRepository lineItemRepository;
	private final OrderRepository orderRepository;

	@EventListener(ApplicationReadyEvent.class)
	void repositories() {
		var orderId = generateId();
		var items = List.of(
			new LineItem(orderId, generateId(), "plunger"),
			new LineItem(orderId, generateId(), "soup"),
			new LineItem(orderId, generateId(), "coffee mug")
		);
		items.stream().map(this.lineItemRepository::save).forEach(log::info);

		var order = new Order(orderId, new Date(), items);
		orderRepository.save(order);

		var orders = orderRepository.findByWhen(order.getWhen());
		orders.forEach(log::info);
	}


}

@Data
@AllArgsConstructor
@NoArgsConstructor
@RedisHash("orders")
class Order implements Serializable {

	@Id
	private Long id;

	@Indexed
	private Date when;

	@Reference
	private List<LineItem> lineItems;
}


@Data
@AllArgsConstructor
@NoArgsConstructor
@RedisHash("lineItems")
class LineItem implements Serializable {

	@Indexed
	private Long orderId;

	@Id
	private Long id;

	private String decription;
}

interface LineItemRepository extends CrudRepository<LineItem, Long> {
}

interface OrderRepository extends CrudRepository<Order, Long> {

	Collection<Order> findByWhen(Date when);
}

// geospatial queries
@Log4j2
@Configuration
@RequiredArgsConstructor
class GeographyConfig {

	private final RedisTemplate<String, String> template;

	@EventListener(ApplicationReadyEvent.class)
	void geography() {

		var geo = this.template.opsForGeo();
		geo.add("Sicily", new Point(13.361389, 38.1155556), "Arigento");
		geo.add("Sicily", new Point(15.087269, 37.502669), "Catania");
		geo.add("Sicily", new Point(13.583333, 37.316667), "Palermo");

		var circle = new Circle(new Point(13.583333, 37.316667), new Distance(100, org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.KILOMETERS));
		var sicily = geo.radius("Sicily", circle);
		sicily
			.getContent()
			.forEach(log::info);

	}
}

// caching
@Log4j2
@Configuration
@EnableCaching
@RequiredArgsConstructor
class CachingConfig {

	private final SlowService service;

	@Bean
	CacheManager redisCache(RedisConnectionFactory cf) {
		return RedisCacheManager.builder(cf).build();
	}

	@EventListener(ApplicationReadyEvent.class)
	void cache() {

		var result = new Runnable() {

			@Override
			public void run() {
				var result = service.greet("World");
				log.info("possibly cached result: " + result);
			}
		};

		this.measure(result);
		this.measure(result);
		this.measure(result);
	}

	private void measure(Runnable runnable) {
		var start = System.currentTimeMillis();
		runnable.run();
		var stop = System.currentTimeMillis();
		var delta = stop - start;
		log.info("delta:  " + delta);
	}

	@Service
	static class SlowService {

		@SneakyThrows
		@Cacheable("slow-greet")
		public String greet(String name) {
			Thread.sleep(1000 * 10);
			return "Hello " + name + " @ " + Instant.now();
		}
	}

}

// pub/sub
@Log4j2
@Configuration
@RequiredArgsConstructor
class PubsubConfig {

	private final RedisTemplate<String, String> template;
	private final String topic = "chat";

	@EventListener(ApplicationReadyEvent.class)
	void pubsub() {
		this.template.convertAndSend(this.topic, "Hello @ " + Instant.now());
	}

	@Bean
	RedisMessageListenerContainer listener(RedisConnectionFactory connectionFactory) {
		var ml = new MessageListener() {

			@Override
			public void onMessage(Message message, byte[] bytes) {
				var str = new String(message.getBody());
				log.info("message from '" + topic + "': " + str);
			}
		};
		return new RedisMessageListenerContainer() {
			{
				addMessageListener(ml, new PatternTopic(topic));
				setConnectionFactory(connectionFactory);
			}
		};

	}

}