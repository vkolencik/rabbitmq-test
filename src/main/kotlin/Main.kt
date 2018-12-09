fun main(args: Array<String>) {

    defaultExchange()

    directExchange()



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
    val exchangeName = "test-exchange"
    val routingKey = "hola"

    println()
    println("Direct exchange")

    Consumer(queueName).use {
        Consumer(queueName).use {
            Producer(exchangeName, Producer.Binding(queueName, routingKey)).use { producer ->
                producer.produce("Hello Prague", routingKey)
                producer.produce("Hello Berlin", routingKey)
                producer.produce("Hello Paris", routingKey)
                producer.produce("Hello London", routingKey)
            }
        }
    }
}
