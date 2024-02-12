import Broker from "../rabbitmq/Broker.js";
import db from "../firebase/Database.js";
import admin from "firebase-admin";

export default class MessageHandler {

    static async updateTask(email, index, modifiedTask) {
        try {
            const {userRef, doc} = await this.getDocByEmail(email);

            const tasks = doc.data()["tasks"];
            tasks[index] = modifiedTask;

            await userRef.update({["tasks"]: tasks});
        } catch (e) {
            console.error("MessageHandler.completeTask(): ", e);
        }
    }

    static async addTask(email, task) {
        try {
            const userRef = db.collection("Users").doc(email);

            const userDoc = await userRef.get();
            if (!userDoc.data().hasOwnProperty('tasks')) {
                await userRef.set({ tasks: [task] }, { merge: true });
            } else {
                await userRef.update({
                    tasks: admin.firestore.FieldValue.arrayUnion(task)
                });
            }
        } catch (e) {
            console.error("MessageHandler.addTask(): ", e);
        }
    }

    static async completeTask(email, index) {
        try {
            const {userRef, doc} = await this.getDocByEmail(email);

            const tasks = doc.data()["tasks"];
            tasks.splice(index, 1);

            await userRef.update({["tasks"]: tasks});
        } catch (e) {
            console.error("MessageHandler.completeTask(): ", e);
        }
    }

    static async returnTasks(email, correlationId) {
        try {
            const { doc }= await this.getDocByEmail(email);
            const userData = doc.data()
            if (userData.tasks) {
                await Broker.produceMessage(userData.tasks, correlationId);
            }
        } catch (e) {
            console.error("MessageHandler.returnTasks(): ", e);
        }
    }

    static async getDocByEmail(email) {
        const userRef = db.collection("Users").doc(email);
        let doc;
        await db.runTransaction(async (t) => doc = await t.get(userRef))
        if (!doc.exists) {
            throw "User doesn't exist!";
        }
        return {userRef, doc};
    }

}
