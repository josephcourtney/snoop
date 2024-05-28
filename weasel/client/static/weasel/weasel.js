export class WeaselClient {
	constructor(url, options = {}) {
		this.url = url;
		this.ws = null;
		this.retryInterval = options.retryInterval || 1000;
		this.maxRetries = options.maxRetries || 5;
		this.chunkSize = options.chunkSize || 1024;
		this.responseTimeout = options.responseTimeout || 5000;
		this.debug = options.debug || false;
		this.logLevel = options.logLevel || "info";
		this.localStorageKey = options.localStorageKey || "weaselClient";
		this.clientId = this._getClientId();
		this.logQueue = this._loadFromLocalStorage("logs");
		this.messageQueue = this._loadFromLocalStorage("messages");
		this.settings = this._loadFromLocalStorage("settings", {});
		this.isConnected = false;
		this.retries = 0;

		this._setupLogging();
		this.connect();
		window.addEventListener("beforeunload", () => this._saveToLocalStorage());
	}

	_setupLogging() {
		this.logger = {
			debug: (msg, requireReceipt = true) =>
				this._log("debug", msg, requireReceipt),
			info: (msg, requireReceipt = true) =>
				this._log("info", msg, requireReceipt),
			log: (msg, requireReceipt = true) =>
				this._log("log", msg, requireReceipt),
			warn: (msg, requireReceipt = true) =>
				this._log("warn", msg, requireReceipt),
			error: (msg, requireReceipt = true) =>
				this._log("error", msg, requireReceipt),
		};
	}

	_log(level, msg, requireReceipt = true) {
		const logMessage = {
			type: "log",
			payload: { level, message: msg, timestamp: new Date().toISOString() },
			requireReceipt,
		};
		if (this.isConnected && level !== "debug") {
			this.ws.send(JSON.stringify(logMessage));
		} else {
			this.logQueue.push(logMessage);
		}
		if (this.debug && this._shouldLog(level)) {
			this._consoleLog(level, `[${level.toUpperCase()}] ${msg}`);
		}
		if (this.externalLog) {
			this.externalLog(`[${level.toUpperCase()}] ${msg}`);
		}
	}

	_consoleLog(level, msg) {
		const consoleMethods = ["debug", "info", "log", "warn", "error"];
		if (consoleMethods.includes(level)) {
			console[level](msg);
		} else {
			console.log(msg);
		}
	}

	_shouldLog(level) {
		const levels = ["debug", "info", "log", "warn", "error"];
		return levels.indexOf(level) >= levels.indexOf(this.logLevel);
	}

	_getClientId() {
		let clientId = localStorage.getItem(`${this.localStorageKey}_clientId`);
		if (!clientId) {
			clientId = `client_${Math.random().toString(36).substr(2, 9)}`;
			localStorage.setItem(`${this.localStorageKey}_clientId`, clientId);
		}
		return clientId;
	}

	_loadFromLocalStorage(key, defaultValue = []) {
		return (
			JSON.parse(localStorage.getItem(`${this.localStorageKey}_${key}`)) ||
			defaultValue
		);
	}

	_saveToLocalStorage() {
		localStorage.setItem(
			`${this.localStorageKey}_logs`,
			JSON.stringify(this.logQueue),
		);
		localStorage.setItem(
			`${this.localStorageKey}_messages`,
			JSON.stringify(this.messageQueue),
		);
		localStorage.setItem(
			`${this.localStorageKey}_settings`,
			JSON.stringify(this.settings),
		);
	}

	clearLocalStorage() {
		for (const key in ["logs", "messages", "settings", "clientId"]) {
			localStorage.removeItem(`${this.localStorageKey}_${key}`);
		}
		this.logQueue = [];
		this.messageQueue = [];
		this.settings = {};
		this.clientId = this._getClientId();
		this.logger.info("Local storage cleared.");
	}

	resetSettings() {
		this.retryInterval = 1000;
		this.maxRetries = 5;
		this.chunkSize = 1024;
		this.responseTimeout = 5000;
		this.logLevel = "info";
		this.settings = {};
		this.logger.info("Settings reset to defaults.");
		this._saveToLocalStorage();
	}

	connect() {
		this.ws = new WebSocket(this.url);

		this.ws.onopen = () => {
			this.logger.info("WebSocket connection opened.");
			this.isConnected = true;
			this.retries = 0;
			this._sendQueue(this.logQueue);
			this._sendQueue(this.messageQueue);
		};

		this.ws.onmessage = (event) => {
			if (event.data) {
				this._handleMessage(event.data);
			}
		};

		this.ws.onerror = (error) => {
			this.logger.error(`WebSocket error: ${error.message}`);
			this.isConnected = false;
			this._retryConnection();
		};

		this.ws.onclose = (event) => {
			this.logger.warn(`WebSocket connection closed: ${event.reason}`);
			this.isConnected = false;
			if (!event.wasClean) {
				this._retryConnection();
			}
		};
	}

	_retryConnection() {
		if (this.retries < this.maxRetries) {
			this.retries++;
			const backoff = this.retryInterval * 2 ** this.retries;
			this.logger.info(
				`Retrying connection (attempt ${this.retries}) in ${backoff}ms...`,
			);
			setTimeout(() => this.connect(), backoff);
		} else {
			this.logger.error("Max retries reached. Could not reconnect.");
		}
	}

	async send(data, requireReceipt = true) {
		if (!data.type) {
			this.logger.error("Message type is undefined. Data:", data);
			return;
		}
		data.requireReceipt = requireReceipt;

		if (!this.isConnected) {
			this.logger.warn("Not connected. Attempting to reconnect...");
			this.connect();
			await this._waitForConnection();
		}

		const chunks = this._chunkMessage(JSON.stringify(data));
		this._sendChunks(chunks);
	}

	_chunkMessage(message) {
		const chunks = [];
		const totalChunks = Math.ceil(message.length / this.chunkSize);
		for (let i = 0; i < totalChunks; i++) {
			const chunk = message.slice(i * this.chunkSize, (i + 1) * this.chunkSize);
			chunks.push({
				type: "chunk",
				index: i,
				total: totalChunks,
				chunk,
				checksum: this._calculateChecksum(chunk),
				clientId: this.clientId,
			});
		}
		return chunks;
	}

	async _sendChunks(chunks) {
		for (const chunk of chunks) {
			await this._sendWithTimeout(JSON.stringify(chunk));
		}
	}

	async _sendWithTimeout(message) {
		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				this.logger.error("Response timed out.");
				reject(new Error("Response timed out."));
			}, this.responseTimeout);

			this.ws.send(message);

			this.ws.onmessage = (event) => {
				clearTimeout(timeout);
				resolve(event.data);
			};
		});
	}

	_waitForConnection() {
		return new Promise((resolve) => {
			const interval = setInterval(() => {
				if (this.isConnected) {
					clearInterval(interval);
					resolve();
				}
			}, 100);
		});
	}

	_sendQueue(queue) {
		while (queue.length > 0) {
			const item = queue.shift();
			if (!item.type) {
				this.logger.error("Queued message has undefined type. Item:", item);
				continue;
			}
			this.ws.send(JSON.stringify(item));
		}
	}

	_handleMessage(message) {
		const parsedMessage = JSON.parse(message);
		if (!parsedMessage.type) {
			this.logger.warn("Received message without type:", parsedMessage);
			return;
		}
		switch (parsedMessage.type) {
			case "log":
				this.logger.info(
					`Log message received from server: ${parsedMessage.payload.message}`,
					false,
				);
				break;
			case "chunk":
				this._handleChunk(parsedMessage);
				break;
			case "full_message":
				this.logger.info(
					`Server responded with full message status: ${parsedMessage.payload.message}`,
					false,
				);
				break;
			default:
				this.logger.warn(`Unknown message type: ${parsedMessage.type}`);
		}
	}

	_handleChunk(chunk) {
		const { index, total, chunk: data, checksum, clientId } = chunk;

		if (clientId !== this.clientId) {
			this.logger.warn(`Received chunk from different client: ${clientId}`);
			return;
		}

		if (this._calculateChecksum(data) !== checksum) {
			this.logger.error(
				`Checksum mismatch for chunk ${index}. Resending chunk...`,
			);
			this._sendChunks([chunk]);
			return;
		}

		if (!this.chunks) {
			this.chunks = new Array(total);
		}
		this.chunks[index] = data;
		this.logger.debug(`Received chunk ${index}/${total - 1}.`);

		if (this.chunks.every((c) => c !== undefined)) {
			const fullMessage = this.chunks.join("");
			this.chunks = null;
			this.logger.info("Reassembled full message.");
			this._processFullMessage(fullMessage);
		}
	}

	_processFullMessage(message) {
		try {
			//const data = JSON.parse(message);
			this.logger.info(`Full message received: ${message}`);
			// Process the full message here
		} catch (error) {
			this.logger.error(`Failed to parse full message: ${error.message}`);
		}
	}

	_calculateChecksum(data) {
		return data.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0);
	}
}
