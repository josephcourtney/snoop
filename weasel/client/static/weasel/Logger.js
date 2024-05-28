export class Logger {
	constructor(logLevel, debug, externalLog) {
		this.logLevel = logLevel;
		this.debug = debug;
		this.externalLog = externalLog;
	}

	log(level, msg, requireReceipt = true) {
		const logMessage = {
			type: "log",
			payload: { level, message: msg, timestamp: new Date().toISOString() },
			requireReceipt,
		};
		if (this.debug && this.shouldLog(level)) {
			this.consoleLog(level, `[${level.toUpperCase()}] ${msg}`);
		}
		if (this.externalLog) {
			this.externalLog(`[${level.toUpperCase()}] ${msg}`);
		}
		return logMessage;
	}

	consoleLog(level, msg) {
		const consoleMethods = ["debug", "info", "log", "warn", "error"];
		if (consoleMethods.includes(level)) {
			console[level](msg);
		} else {
			console.log(msg);
		}
	}

	shouldLog(level) {
		const levels = ["debug", "info", "log", "warn", "error"];
		return levels.indexOf(level) >= levels.indexOf(this.logLevel);
	}
}
