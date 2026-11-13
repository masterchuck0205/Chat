# MORSE//CHAT (standalone)
Node server + WebSocket. No Claude account needed.
Local: `npm install && npm start` then open http://localhost:3000
Render: push this folder to a GitHub repo -> Render dashboard -> New -> Blueprint (uses render.yaml) or Web Service (build: npm install, start: node server.js).
Data is stored in DATA_DIR (db.json + blobs/). Free Render plans have no persistent disk, so data resets on restart.
