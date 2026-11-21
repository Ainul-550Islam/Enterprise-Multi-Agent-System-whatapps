# syntax=docker/dockerfile:1
# PURPOSE: web dashboard image (Next.js standalone, non-root).
#
# Build from the repository root:
#   docker build -f deploy/docker/web.Dockerfile -t enterprise-mas/web:local .

FROM node:20-alpine AS deps
WORKDIR /src
COPY web/package.json web/package-lock.json ./
RUN npm ci

FROM node:20-alpine AS build
WORKDIR /src
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=deps /src/node_modules ./node_modules
COPY web/ ./
RUN npm run build

FROM node:20-alpine AS web
RUN addgroup -S mas && adduser -S mas -G mas
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 PORT=3000 HOSTNAME=0.0.0.0
WORKDIR /app
COPY --from=build --chown=mas:mas /src/.next/standalone ./
COPY --from=build --chown=mas:mas /src/.next/static ./.next/static
COPY --from=build --chown=mas:mas /src/public ./public
USER mas
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:3000/login >/dev/null || exit 1
CMD ["node", "server.js"]
