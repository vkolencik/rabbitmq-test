import com.rabbitmq.client.*

fun main(args: Array<String>) {
    Producer().produce("hello, world")
}

class Producer {

    private val channel: Channel
    private val exchangeName = "hello-exchange"
    private val queueName = "hello-queue"

    init {
        val factory = ConnectionFactory()
        val conn = factory.newConnection()
        channel = conn.createChannel()
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true)
        channel.queueDeclare(queueName, true, false, false, emptyMap())
        channel.queueBind(queueName, exchangeName, "hola")

        channel.basicConsume(
            queueName,
            false,
            DeliverCallback { consumerTag, message ->
                println("Message: ${String(message.body)}")
                channel.basicAck(message.envelope.deliveryTag, true)
            },
            CancelCallback {  }
        )
    }

    fun produce(msg: String) {
        channel.basicPublish(exchangeName, "hola", null, msg.toByteArray())
    }
}
