/**
 * Lightweight Server-Sent Events (SSE) Broadcaster
 * Maintains per-user HTTP event connections for real-time storage and file synchronization.
 */
class EventBroadcaster {
  constructor() {
    this.clients = new Map(); // Map<userIdString, Set<express.Response>>
  }

  /**
   * Registers an open HTTP response stream for a user
   * @param {string|object} userId
   * @param {object} res - Express Response object
   */
  addClient(userId, res) {
    const id = String(userId);
    if (!this.clients.has(id)) {
      this.clients.set(id, new Set());
    }
    this.clients.get(id).add(res);

    res.on('close', () => {
      const userConns = this.clients.get(id);
      if (userConns) {
        userConns.delete(res);
        if (userConns.size === 0) {
          this.clients.delete(id);
        }
      }
    });
  }

  /**
   * Broadcasts an event to all active sessions/devices of a specific user
   * @param {string|object} userId
   * @param {string} eventName
   * @param {object} data
   */
  broadcast(userId, eventName, data = {}) {
    const id = String(userId);
    const userConns = this.clients.get(id);
    if (!userConns || userConns.size === 0) return;

    const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const res of userConns) {
      try {
        res.write(payload);
      } catch (err) {
        // Handle broken socket
        userConns.delete(res);
      }
    }
  }
}

export const broadcaster = new EventBroadcaster();
export default broadcaster;
