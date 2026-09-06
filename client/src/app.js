class NodePanel {
    /**
     * @param {string} id         - 'a' or 'b'
     * @param {string} label      - 'Node A' or 'Node B'
     * @param {string} prefix     - '/node-a' or '/node-b'
     */
    constructor(id, label, prefix) {
        this.id = id;
        this.label = label;
        this.prefix = prefix;
        this.clientId = crypto.randomUUID();
        this.eventSource = null;

        this._build();
    }

    _build() {
        const container = document.getElementById(`panel-${this.id}`);

        container.innerHTML = `
            <div class="panel-header">
                <span class="status-dot" id="dot-${this.id}"></span>
                <span class="panel-title">${this.label}</span>
                <span class="status-label" id="status-${this.id}">connecting…</span>
            </div>
            <div class="messages" id="messages-${this.id}">
                <div class="empty-hint" id="hint-${this.id}">No messages yet</div>
            </div>
            <div class="input-row">
                <input id="input-${this.id}" type="text" placeholder="Type a message…" />
                <button id="send-${this.id}" disabled>Send</button>
            </div>
        `;

        this._dot = document.getElementById(`dot-${this.id}`);
        this._statusLabel = document.getElementById(`status-${this.id}`);
        this._feed = document.getElementById(`messages-${this.id}`);
        this._hint = document.getElementById(`hint-${this.id}`);
        this._input = document.getElementById(`input-${this.id}`);
        this._sendBtn = document.getElementById(`send-${this.id}`);

        this._input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this._send();
        });
        this._sendBtn.addEventListener('click', () => this._send());
    }

    connect() {
        this.close();
        this._statusLabel.textContent = 'connecting…';
        const url = `${this.prefix}/register/${this.clientId}`;
        this.eventSource = new EventSource(url);

        this.eventSource.addEventListener('open', () => this._setConnected());
        this.eventSource.addEventListener('error', () => this._setDisconnected());

        this.eventSource.addEventListener('chat', (e) => {
            try {
                const data = JSON.parse(e.data);
                if (typeof data.text === 'string' && typeof data.node === 'string') {
                    this._setConnected();
                    this._appendMessage(data.text, data.node);
                }
            } catch (error) {
                console.error('Invalid chat event', error);
            }
        });
    }

    close() {
        this.eventSource?.close();
        this.eventSource = null;
        this._sendBtn.disabled = true;
    }

    _setConnected() {
        this._dot.className = 'status-dot connected';
        this._statusLabel.textContent = 'connected';
        this._sendBtn.disabled = false;
    }

    _setDisconnected() {
        this._dot.className = 'status-dot error';
        this._statusLabel.textContent = 'reconnecting…';
        this._sendBtn.disabled = true;
    }

    _appendMessage(text, fromNode) {
        this._hint?.remove();
        this._hint = null;

        const isLocalOrigin = fromNode === this.label;
        const fromClass = fromNode === 'Node A' ? 'from-a' : 'from-b';

        const msg = document.createElement('div');
        msg.className = `message ${fromClass}`;
        const meta = document.createElement('div');
        meta.className = 'message-meta';
        meta.textContent = fromNode;
        if (!isLocalOrigin) {
            const remoteTag = document.createElement('span');
            remoteTag.className = 'remote-indicator';
            remoteTag.textContent = 'via Valkey';
            meta.appendChild(remoteTag);
        }
        const messageText = document.createElement('div');
        messageText.className = 'message-text';
        messageText.textContent = text;
        msg.append(meta, messageText);
        this._feed.appendChild(msg);
        this._feed.scrollTop = this._feed.scrollHeight;
    }

    async _send() {
        const text = this._input.value.trim();
        if (!text) return;
        this._input.value = '';
        this._sendBtn.disabled = true;

        try {
            const response = await fetch(`${this.prefix}/send`, {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain' },
                body: text
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (error) {
            console.error('Could not send message', error);
            this._input.value = text;
            this._statusLabel.textContent = 'send failed';
        } finally {
            this._sendBtn.disabled = this.eventSource?.readyState !== EventSource.OPEN;
            this._input.focus();
        }
    }
}

export default class App {
    start() {
        const prefixes = import.meta.env.DEV
            ? ['/node-a', '/node-b']
            : [
                `${window.location.protocol}//${window.location.hostname}:8080`,
                `${window.location.protocol}//${window.location.hostname}:8081`
            ];
        this.nodes = [
            new NodePanel('a', 'Node A', prefixes[0]),
            new NodePanel('b', 'Node B', prefixes[1])
        ];
        this.nodes.forEach((node) => node.connect());
    }

    stop() {
        this.nodes?.forEach((node) => node.close());
    }
}
