import com.rabbitmq.client.BuiltinExchangeType

fun main(args: Array<String>) {

    defaultExchange()

    directExchange()

    fanoutExchange()

}

/**
 *     Default exchange: routing key directly states the queue name.
 *     This binding is default, so it doesn't need to be declared.
 *
 *     Creates two consumers so that the round-robin delivery can be observed.
 */
private fun defaultExchange() {

    println()
    println("Default exchange")

    val queueName = "test-queue"
    Consumer(queueName).use {
        Consumer(queueName).use {
            Producer().use { producer ->

                producer.produce("Hello Prague", queueName)
                producer.produce("Hello Berlin", queueName)
                producer.produce("Hello Paris", queueName)
                producer.produce("Hello London", queueName)
            }
        }
    }
}


/**
 * Direct exchange: declares direct exchange and adds binding that sends all messages with a given routing key to the
 * specified queue.
 *
 * Creates two consumers for the single queue so that the round-robin delivery can be observed.
 */
fun directExchange() {

    val queueName = "test-queue"
    val exchangeName = "test-direct-exchange"
    val routingKey = "hola"

    println()
    println("Direct exchange")

    Consumer(queueName).use {
        Consumer(queueName).use {
            Producer(
                exchangeName,
                BuiltinExchangeType.DIRECT,
                Producer.Binding(queueName, routingKey)
            ).use { producer ->
                producer.produce("Hello Prague", routingKey)
                producer.produce("Hello Berlin", routingKey)
                producer.produce("Hello Paris", routingKey)
                producer.produce("Hello London", routingKey)
            }
        }
    }
}

/**
 * Fanout exchange: sends all messages with the given routing key to *two* different queues. Creates two consumers,
 * one for each queue. Every sent message is consumed by both consumers, one from each queue.
 */
fun fanoutExchange() {

    val queue1Name = "test-fanout-queue-1"
    val queue2Name = "test-fanout-queue-2"
    val exchangeName = "test-fanout-exchange"
    val routingKey = "hola"

    println()
    println("Fanout exchange")

    Consumer(queue1Name).use {
        Consumer(queue2Name).use {
            Producer(
                exchangeName,
                BuiltinExchangeType.FANOUT,
                Producer.Binding(queue1Name, routingKey),
                Producer.Binding(queue2Name, routingKey)
            ).use { producer ->
                // Supplied routing key is ignored, so we can use whatever:
                producer.produce("Hello Prague", "whatever1")
                producer.produce("Hello Berlin", "whatever2")
            }
        }
    }
}
