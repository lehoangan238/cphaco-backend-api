"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const dotenv_1 = require("dotenv");
const db_1 = require("./config/db");
const fullSync11055_1 = require("./modules/workflow/fullSync11055");
(0, dotenv_1.config)();
const app = (0, express_1.default)();
app.use(express_1.default.json());
// Health check – test DB ok chưa
app.get("/health", async (_req, res) => {
    try {
        await db_1.pool.query("SELECT 1");
        res.json({ ok: true });
    }
    catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});
// API để chạy full sync lần đầu
app.post("/api/workflow/11055/full-sync", async (_req, res) => {
    try {
        const total = await (0, fullSync11055_1.fullSync11055)();
        res.json({ ok: true, total });
    }
    catch (e) {
        console.error(e);
        res.status(500).json({ ok: false, error: e.message });
    }
});
const port = process.env.PORT || 8080;
app.listen(port, () => {
    console.log(`CPHACO backend listening on ${port}`);
});
//# sourceMappingURL=index.js.map