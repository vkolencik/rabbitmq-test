import com.rabbitmq.client.BuiltinExchangeType

fun main(args: Array<String>) {

    defaultExchange()

    directExchange()

    fanoutExchange()

}

private fun defaultExchange() {
    /*
    Default exchange --> routing key directly states the queue name.
    This binding is default, so it doesn't need to be declared.
      */

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

fun directExchange() {
    /*
    Declare direct exchange and add binding that sends all messages with a given routing key to a specified queue.
     */
    val queueName = "test-queue"
    val exchangeName = "test-direct-exchange"
    val routingKey = "hola"

    println()
    println("Direct exchange")

    Consumer(queueName).use {
        Consumer(queueName).use {
            Producer(exchangeName, BuiltinExchangeType.DIRECT, Producer.Binding(queueName, routingKey)).use { producer ->
                producer.produce("Hello Prague", routingKey)
                producer.produce("Hello Berlin", routingKey)
                producer.produce("Hello Paris", routingKey)
                producer.produce("Hello London", routingKey)
            }
        }
    }
}

fun fanoutExchange() {
    val queue1Name = "test-fanout-queue-1"
    val queue2Name = "test-fanout-queue-2"
    val exchangeName = "test-fanout-exchange"
    val routingKey = "hola"

    println()
    println("Fanout exchange")

    Consumer(queue1Name).use {
        Consumer(queue2Name).use {
            Producer(exchangeName, BuiltinExchangeType.FANOUT, Producer.Binding(queue1Name, routingKey), Producer.Binding(queue2Name, routingKey)).use { producer ->
                producer.produce("Hello Prague", routingKey)
                producer.produce("Hello Berlin", routingKey)
            }
        }
    }
}
