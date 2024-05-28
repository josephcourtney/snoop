export class WebSocketHandler {
	constructor(
		url,
		logger,
		chunkSize,
		responseTimeout,
		clientId,
		messageHandler,
	) {
		this.url = url;
		this.logger = logger;
		this.chunkSize = chunkSize;
		this.responseTimeout = responseTimeout;
		this.clientId = clientId;
		this.messageHandler = messageHandler;
		this.ws = null;
		this.chunks = null;
	}

	connect(onOpen, onClose) {
		this.ws = new WebSocket(this.url);

		this.ws.onopen = () => {
			this.logger.log("info", "WebSocket connection opened.");
			onOpen();
		};

		this.ws.onmessage = (event) => {
			if (event.data) {
				this.messageHandler(event.data);
			}
		};

		this.ws.onerror = (error) => {
			this.logger.log("error", `WebSocket error: ${error.message}`);
			onClose();
		};

		this.ws.onclose = (event) => {
			this.logger.log("warn", `WebSocket connection closed: ${event.reason}`);
			onClose();
		};
	}

	async waitForConnection() {
		return new Promise((resolve) => {
			const interval = setInterval(() => {
				if (this.ws && this.ws.readyState === WebSocket.OPEN) {
					clearInterval(interval);
					resolve();
				}
			}, 100);
		});
	}

	chunkMessage(message) {
		const chunks = [];
		const totalChunks = Math.ceil(message.length / this.chunkSize);
		for (let i = 0; i < totalChunks; i++) {
			const chunk = message.slice(i * this.chunkSize, (i + 1) * this.chunkSize);
			chunks.push({
				type: "chunk",
				index: i,
				total: totalChunks,
				chunk,
				checksum: this.calculateChecksum(chunk),
				clientId: this.clientId,
			});
		}
		return chunks;
	}

	async sendChunks(chunks) {
		for (const chunk of chunks) {
			await this.sendWithTimeout(JSON.stringify(chunk));
		}
	}

	async sendWithTimeout(message) {
		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				this.logger.log("error", "Response timed out.");
				reject(new Error("Response timed out."));
			}, this.responseTimeout);

			this.ws.send(message);

			this.ws.onmessage = (event) => {
				clearTimeout(timeout);
				resolve(event.data);
			};
		});
	}

	handleChunk(chunk) {
		const { index, total, chunk: data, checksum, clientId } = chunk;

		if (clientId !== this.clientId) {
			this.logger.log(
				"warn",
				`Received chunk from different client: ${clientId}`,
			);
			return;
		}

		if (this.calculateChecksum(data) !== checksum) {
			this.logger.log(
				"error",
				`Checksum mismatch for chunk ${index}. Resending chunk...`,
			);
			this.sendChunks([chunk]);
			return;
		}

		if (!this.chunks) {
			this.chunks = new Array(total);
		}
		this.chunks[index] = data;
		this.logger.log("debug", `Received chunk ${index}/${total - 1}.`);

		if (this.chunks.every((c) => c !== undefined)) {
			const fullMessage = this.chunks.join("");
			this.chunks = null;
			this.logger.log("info", "Reassembled full message.");
			this.processFullMessage(fullMessage);
		}
	}

	processFullMessage(message) {
		try {
			this.logger.log("info", `Full message received: ${message}`);
			// Process the full message here
		} catch (error) {
			this.logger.log(
				"error",
				`Failed to parse full message: ${error.message}`,
			);
		}
	}

	calculateChecksum(data) {
		return data.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0);
	}
}
