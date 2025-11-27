"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.pool = void 0;
const pg_1 = require("pg");
const dotenv_1 = require("dotenv");
(0, dotenv_1.config)();
exports.pool = new pg_1.Pool({
    connectionString: process.env.DATABASE_URL,
});
//# sourceMappingURL=db.js.map