FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY src/frontend/package.json src/frontend/package-lock.json* ./src/frontend/
RUN cd src/frontend && npm ci

COPY . .

RUN cd src/frontend && npm run build

FROM node:20-alpine AS runtime

RUN apk add --no-cache ffmpeg rclone curl

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY src/ ./src/

COPY --from=builder /app/src/frontend/dist ./src/frontend/dist

RUN mkdir -p /app/uploads /app/data && chown -R node:node /app

USER node

EXPOSE 3001

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:3001/api/health || exit 1

CMD ["node", "src/server.js"]
