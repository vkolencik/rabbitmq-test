import com.rabbitmq.client.*

fun main(args: Array<String>) {
    Producer().produce("hello, world")
}

class Producer {
    fun produce(msg: String) {
        val factory = ConnectionFactory()
        val conn = factory.newConnection()
        val channel = conn.createChannel()
        val exchangeName = "hello-exchange"
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true)
        val queueName = "hello-queue"
        channel.queueDeclare(queueName, true, false, false, emptyMap())
        channel.queueBind(queueName, exchangeName, "hola")

        channel.basicPublish("", "hola", null, msg.toByteArray())
    }
}
