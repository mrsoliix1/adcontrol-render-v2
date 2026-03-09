FROM node:20-slim

# Install FFmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY . .

EXPOSE 3001
CMD ["node", "--max-old-space-size=384", "server.js"]
