export class Storage {
	static getClientId(localStorageKey) {
		let clientId = localStorage.getItem(`${localStorageKey}_clientId`);
		if (!clientId) {
			clientId = `client_${Math.random().toString(36).substr(2, 9)}`;
			localStorage.setItem(`${localStorageKey}_clientId`, clientId);
		}
		return clientId;
	}

	static load(localStorageKey, key, defaultValue = []) {
		return (
			JSON.parse(localStorage.getItem(`${localStorageKey}_${key}`)) ||
			defaultValue
		);
	}

	static save(localStorageKey, data) {
		for (const key in data) {
			localStorage.setItem(
				`${localStorageKey}_${key}`,
				JSON.stringify(data[key]),
			);
		}
	}
}
