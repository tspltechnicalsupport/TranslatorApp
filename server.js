const express = require('express')
const http = require('http')
const { Server } = require('socket.io')
const path = require('path')

const app = express()
const server = http.createServer(app)
const io = new Server(server)

app.use(express.static(path.join(__dirname, 'public')))

const rooms = {}

io.on('connection', (socket) => {
  console.log('User connected:', socket.id)

  socket.on('create-room', (callback) => {
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase()
    rooms[roomId] = { users: [socket.id] }
    socket.join(roomId)
    console.log(`Room ${roomId} created by ${socket.id}`)
    callback(roomId)
  })

  socket.on('join-room', (roomId, callback) => {
    const normalizedId = roomId.toUpperCase()
    const room = rooms[normalizedId]
    if (!room) {
      callback({ error: 'Room not found' })
      return
    }
    if (room.users.length >= 2) {
      callback({ error: 'Room is full' })
      return
    }
    room.users.push(socket.id)
    socket.join(normalizedId)
    console.log(`${socket.id} joined room ${normalizedId}`)
    callback({ success: true })

    socket.to(normalizedId).emit('user-joined', socket.id)
  })

  socket.on('offer', ({ to, offer }) => {
    io.to(to).emit('offer', { from: socket.id, offer })
  })

  socket.on('answer', ({ to, answer }) => {
    io.to(to).emit('answer', { from: socket.id, answer })
  })

  socket.on('ice-candidate', ({ to, candidate }) => {
    io.to(to).emit('ice-candidate', { from: socket.id, candidate })
  })

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id)
    for (const roomId in rooms) {
      const room = rooms[roomId]
      const idx = room.users.indexOf(socket.id)
      if (idx !== -1) {
        room.users.splice(idx, 1)
        socket.to(roomId).emit('user-left', socket.id)
        if (room.users.length === 0) {
          delete rooms[roomId]
        }
      }
    }
  })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`)
})
