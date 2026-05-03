const express = require('express');
const jwt = require('jsonwebtoken');
const router = express.Router();

// In a real application, you would validate against a user database
const users = [
    { id: 'user-123', username: 'testuser', password: 'password' }
];

router.post('/login', (req, res) => {
    const { username, password } = req.body;
    const user = users.find(u => u.username === username && u.password === password);

    if (!user) {
        return res.status(401).json({ message: 'Invalid credentials' });
    }

    const accessToken = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '1h' });
    res.json({ accessToken });
});

module.exports = router;
