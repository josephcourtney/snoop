import { Logger } from "./Logger.js";
import { Storage } from "./Storage.js";
import { WebSocketHandler } from "./WebSocketHandler.js";

export class WeaselClient {
	constructor(url, options = {}) {
		this.url = url;
		this.retryInterval = options.retryInterval || 1000;
		this.maxRetries = options.maxRetries || 5;
		this.chunkSize = options.chunkSize || 1024;
		this.responseTimeout = options.responseTimeout || 5000;
		this.debug = options.debug || false;
		this.logLevel = options.logLevel || "info";
		this.localStorageKey = options.localStorageKey || "weaselClient";
		this.clientId = Storage.getClientId(this.localStorageKey);
		this.logQueue = Storage.load(this.localStorageKey, "logs");
		this.messageQueue = Storage.load(this.localStorageKey, "messages");
		this.settings = Storage.load(this.localStorageKey, "settings", {});
		this.isConnected = false;
		this.retries = 0;

		this.logger = new Logger(this.logLevel, this.debug, this.externalLog);
		this.wsHandler = new WebSocketHandler(
			this.url,
			this.logger,
			this.chunkSize,
			this.responseTimeout,
			this.clientId,
			this.handleIncomingMessage.bind(this),
		);

		this.connect();

		window.addEventListener("beforeunload", () =>
			Storage.save(this.localStorageKey, {
				logs: this.logQueue,
				messages: this.messageQueue,
				settings: this.settings,
			}),
		);
	}

	connect() {
		this.wsHandler.connect(
			() => {
				this.isConnected = true;
				this.retries = 0;
				this.sendQueue(this.logQueue);
				this.sendQueue(this.messageQueue);
			},
			() => {
				this.isConnected = false;
				this.retryConnection();
			},
		);
	}

	retryConnection() {
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
			await this.wsHandler.waitForConnection();
		}

		const chunks = this.wsHandler.chunkMessage(JSON.stringify(data));
		this.wsHandler.sendChunks(chunks);
	}

	sendQueue(queue) {
		while (queue.length > 0) {
			const item = queue.shift();
			if (!item.type) {
				this.logger.error("Queued message has undefined type. Item:", item);
				continue;
			}
			this.wsHandler.send(JSON.stringify(item));
		}
	}

	handleIncomingMessage(message) {
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
				this.wsHandler.handleChunk(parsedMessage);
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
}
