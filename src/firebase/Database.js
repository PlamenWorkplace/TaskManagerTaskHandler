import admin from "firebase-admin";
import serviceAccount from "./AccountKey.js";

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
})

export default admin.firestore();
