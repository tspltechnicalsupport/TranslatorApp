require('dotenv').config()
const express = require('express')
const http = require('http')
const { Server } = require('socket.io')
const WebSocket = require('ws')
const path = require('path')

const app = express()
const server = http.createServer(app)
const io = new Server(server)

app.use(express.static(path.join(__dirname, 'public')))

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY
const rooms = {}

function getRoomList() {
  return Object.entries(rooms).map(([id, room]) => ({
    id,
    users: room.users.length,
    createdAt: room.createdAt
  }))
}

io.on('connection', (socket) => {
  console.log('User connected:', socket.id)

  let deepgramWs = null

  socket.on('start-transcription', ({ language }) => {
    if (!DEEPGRAM_API_KEY) {
      console.log('No DEEPGRAM_API_KEY set — transcription disabled')
      socket.emit('transcription-error', { error: 'No API key configured' })
      return
    }

    const params = new URLSearchParams({
      encoding: 'opus',
      sample_rate: '48000',
      channels: '1',
      language: language || 'en',
      interim_results: 'true',
      endpointing: '300',
      vad_events: 'true',
      smart_format: 'true'
    })

    const dgUrl = `wss://api.deepgram.com/v1/listen?${params}`

    deepgramWs = new WebSocket(dgUrl, {
      headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` }
    })

    deepgramWs.on('open', () => {
      console.log(`Deepgram connected for ${socket.id}`)
      socket.emit('transcription-ready')
    })

    deepgramWs.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString())
        if (msg.type === 'Results') {
          const alt = msg.channel?.alternatives?.[0]
          if (alt && alt.transcript) {
            socket.emit('transcription', {
              text: alt.transcript,
              is_final: msg.is_final,
              confidence: alt.confidence,
              words: alt.words
            })
          }
        } else if (msg.type === 'UtteranceEnd') {
          socket.emit('utterance-end')
        }
      } catch (e) {}
    })

    deepgramWs.on('error', (err) => {
      console.log('Deepgram error:', err.message)
      socket.emit('transcription-error', { error: err.message })
    })

    deepgramWs.on('close', (code, reason) => {
      console.log(`Deepgram closed for ${socket.id}: ${code}`)
      socket.emit('transcription-stopped')
      deepgramWs = null
    })
  })

  socket.on('audio-data', (data) => {
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.send(Buffer.from(data))
    }
  })

  socket.on('stop-transcription', () => {
    if (deepgramWs) {
      deepgramWs.close()
      deepgramWs = null
    }
  })

  socket.on('create-room', (callback) => {
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase()
    rooms[roomId] = { users: [socket.id], createdAt: Date.now() }
    socket.join(roomId)
    console.log(`Room ${roomId} created by ${socket.id}`)
    callback(roomId)
    io.emit('room-list', getRoomList())
  })

  socket.on('get-rooms', (callback) => {
    callback(getRoomList())
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

  socket.on('chat-message', ({ to, text }) => {
    io.to(to).emit('chat-message', { from: socket.id, text })
  })

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id)
    if (deepgramWs) {
      deepgramWs.close()
      deepgramWs = null
    }
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
    io.emit('room-list', getRoomList())
  })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`)
  console.log(`Deepgram API key: ${DEEPGRAM_API_KEY ? 'SET ✓' : 'NOT SET — add DEEPGRAM_API_KEY to .env'}`)
})
