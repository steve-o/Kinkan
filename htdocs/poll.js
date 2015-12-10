// ES6 Harmony
// Whole-script strict mode syntax
"use strict";
class KinkanPoller {
// TBD: Named or default parameters not yet supported.
	constructor(url, ping_interval, reconnect_interval) {
		this.url = url;
		this.ping_interval = ping_interval;
		this.reconnect_interval = reconnect_interval;
		this.sock = undefined;
		this.poll_id = undefined;
		this.reconnect_id = undefined;
		this.pending_count = 0;
	}

// WebSocket is immutable, recreate entire object to reconnect.
	Connect() {
		let new_sock = new WebSocket(this.url);
		new_sock.onopen = (e) => this.OnOpen(e);
		new_sock.onclose = (e) => this.OnClose(e);
		new_sock.onmessage = (e) => this.OnMessage(e);
		this.sock = new_sock;
		this.pending_count = 0;
	}

	OnOpen() {
		document.getElementById("status").textContent = "connected";
		this.SchedulePoll();
	}

	Close() {
		this.CancelReconnect();
		this.CancelPoll();
		if (this.sock !== undefined) {
			this.sock.close();
			this.sock = undefined;
		}
	}

// Reconnect if close is not clean.
	OnClose(e) {
		this.CancelPoll();
		if (!e.wasClean) {
			document.getElementById("status").textContent = "disconnected";
			this.ScheduleReconnect();
		}
	}

	ScheduleReconnect() {
		let timeout = this.reconnect_interval;
		if (!document.hasFocus()) {
			timeout *= 2;
		}
		this.reconnect_id = window.setTimeout(() => this.Connect(), timeout);
	}

	CancelReconnect() {
		if (typeof this.reconnect_id === "number") {
			window.clearTimeout(this.reconnect_id);
			this.reconnect_id = undefined;
		}
	}

// Target 10fps in focus, 5fps out-of-focus but still visible, and paused if hidden.
	SchedulePoll() {
		let timeout = this.ping_interval;
		if (!document.hasFocus()) {
			timeout *= 2;
		}
		this.poll_id = window.setTimeout(() => this.SendPoll(), timeout);
	}

	CancelPoll() {
		if (typeof this.poll_id === "number") {
			window.clearTimeout(this.poll_id);
			this.poll_id = undefined;
		}
	}

	SendPoll() {
		if (this.sock.readyState !== WebSocket.OPEN)
			return;
		if (this.sock.bufferedAmount === 0) {
			this.sock.send("c");
			this.sock.send("p");
			this.pending_count += 2;
		}
	}

	OnMessage(e) {
		let msg = JSON.parse(e.data);
		window.requestAnimationFrame(() => this.OnUpdate(msg));
	}

	OnUpdate(msg) {
		if (msg.hostname) {
			document.getElementById("hostname").textContent = msg.hostname;
			document.getElementById("username").textContent = msg.username;
			document.getElementById("pid").textContent = msg.pid;
			document.getElementById("clients").textContent = msg.clients;
			document.getElementById("provider_msgs").textContent = msg.msgs;
			this.pending_count--;
		} else if (msg.is_active) {
			document.getElementById("infra").textContent = msg.is_active ? (msg.ip + ";" + msg.app + ";" + msg.component) : "not connected";
			document.getElementById("consumer_msgs").textContent = msg.msgs;
			this.pending_count--;
		}
		if (0 == this.pending_count)
			this.SchedulePoll();
	}

	OnHidden() {
		document.getElementById("status").textContent = "paused";
		this.CancelPoll();
	}

	OnVisible() {
		if (this.poll_id === undefined &&
			this.sock !== undefined &&
			this.sock.readyState === WebSocket.OPEN)
		{
			this.OnOpen();
		}
	}
}

let poller = new KinkanPoller("ws://" + window.location.host + "/ws", 100, 1000);
poller.Connect();

document.addEventListener("visibilitychange", function() {
	switch(document.visibilityState) {
	case "hidden":
		poller.OnHidden();
		break;
	case "unloaded":
		poller.Close();
		break;
	case "visible":
		poller.OnVisible();
		break;
	}
});
