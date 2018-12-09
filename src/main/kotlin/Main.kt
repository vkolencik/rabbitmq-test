fun main(args: Array<String>) {

    directDefaultExchange()

}

private fun directDefaultExchange() {
    val queueName = "test-queue"
    Consumer(queueName).use {
        Consumer(queueName).use {
            Producer().use { producer ->
                /*
                 Direct exchange --> routing key directly states the queue name.
                 This binding is default, so it doesn't need to be declared.
                  */

                producer.produce("Hello Prague", queueName)
                producer.produce("Hello Berlin", queueName)
                producer.produce("Hello Paris", queueName)
                producer.produce("Hello London", queueName)
                producer.produce("Hello Tokyo", queueName)
                producer.produce("Hello Kairo", queueName)
                producer.produce("Hello New York", queueName)
            }
        }
    }
}

