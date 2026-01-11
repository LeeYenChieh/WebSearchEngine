import { CRAWLER_URL, CRAWLER_HEALTH_ENDPOINT, TYPESENSE_URL, TYPESENSE_HEALTH_ENDPOINT } from "./consts.js";

import express from "express";
import cors from "cors";

const app = express();
app.use(cors()); // ✔ 允許所有前端呼叫
app.use(express.json());

// 健康檢查：前端只要叫這個，就不會再 CORS
app.get("/api/crawl/health", async (req, res) => {
    try {
        const response = await fetch(`${CRAWLER_URL}${CRAWLER_HEALTH_ENDPOINT}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
            },
        });
        const data = await response.json();
        console.log(data)
        console.log(data.daily[0].fail_reasons)
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: "Crawler unreachable" });
    }
});

app.get("/api/typesense/health", async (req, res) => {
    try {
        const response = await fetch(`${TYPESENSE_URL}${TYPESENSE_HEALTH_ENDPOINT}`);
        const data = await response.json();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: "Typesense unreachable" });
    }
});


// 後端跑在 3001（或你喜歡的 port）
app.listen(4000, () => {
    console.log("Backend server running on http://127.0.0.1:4000");
});
