import {connect} from "amqplib";
import MessageHandler from "../service/MessageHandler.js";
import config from "./Config.js";

class Broker {

    static instance;

    static getInstance() {
        if (!this.instance) {
            this.instance = new Broker();
        }
        return this.instance;
    }

    async initialize() {
        try {
            const connection = await connect(config.url);

            this.producerChannel = await connection.createChannel();
            this.consumerChannel = await connection.createChannel();

            await this.consumerChannel.assertQueue(config.queues.requestQueue, {exclusive: true})

            await this.startConsumer();
        } catch(e) {
            console.log("Error: ", e.message)
        }
    }

    async produceMessage(data, correlationId) {
        this.producerChannel.sendToQueue(
            config.queues.responseQueue,
            Buffer.from(JSON.stringify(data)),
            {
                correlationId: correlationId.toString(),

            }
        )
    }

    async startConsumer() {
        this.consumerChannel.consume(
            config.queues.requestQueue,
            async message => {
                const correlationId = message.properties.correlationId;
                const command = JSON.parse(message.content.toString());
                const payload = command.payload;
                switch (command.name) {
                    case "CONNECTED":
                        await MessageHandler.returnTasks(payload.email, correlationId);
                        break;
                    case "ADD_TASK":
                        await MessageHandler.addTask(payload.email, payload.task);
                        break;
                    case "COMPLETE_TASK":
                        await MessageHandler.completeTask(payload.email, payload.index);
                        break;
                    case "UPDATE_TASK":
                        await MessageHandler.updateTask(payload.email, payload.index, payload.task);
                        break;
                    default:
                        console.error("startConsumer(): Invalid command!");
                        break;
                }
                },
            { noAck: true }
        )
    }

}

export default Broker.getInstance();
