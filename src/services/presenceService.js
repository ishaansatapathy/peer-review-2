// In-memory store for user presence and last update times.
// For a production, distributed system, this should be replaced with Redis.
const connectedUsers = new Map();

const addUser = (userId, socketId) => {
    connectedUsers.set(userId, {
        socketId,
        lastUpdate: Date.now(),
        isStale: false
    });
    console.log(`User connected: ${userId}. Total users: ${connectedUsers.size}`);
};

const removeUser = (userId) => {
    if (connectedUsers.has(userId)) {
        connectedUsers.delete(userId);
        console.log(`User disconnected: ${userId}. Total users: ${connectedUsers.size}`);
    }
};

const updateUserTimestamp = (userId) => {
    if (connectedUsers.has(userId)) {
        const user = connectedUsers.get(userId);
        user.lastUpdate = Date.now();
        user.isStale = false; // Mark as active on update
    }
};

const getConnectedUsers = () => {
    return Object.fromEntries(connectedUsers.entries());
};

const checkStaleUsers = (io, staleTimeout = 30000) => {
    setInterval(() => {
        const now = Date.now();
        for (const [userId, userData] of connectedUsers.entries()) {
            if (!userData.isStale && (now - userData.lastUpdate > staleTimeout)) {
                userData.isStale = true; // Mark as stale to only send one notification
                console.log(`User ${userId} is stale.`);
                // Notify all clients that this user is stale/removed
                io.emit('userDisconnected', { userId });
            }
        }
    }, 10000); // Check every 10 seconds
};

module.exports = {
    addUser,
    removeUser,
    updateUserTimestamp,
    getConnectedUsers,
    checkStaleUsers
};
