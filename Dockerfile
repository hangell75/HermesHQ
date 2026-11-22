# Hermy HQ production image for Docker/Compose.
# The database and Hermes bridge remain external to this web container.
FROM node:22-bookworm-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
# npm ci runs Prisma's postinstall, so the schema must exist in this stage.
COPY prisma ./prisma
RUN npm ci --no-audit --no-fund

FROM node:22-bookworm-slim AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build-time placeholders only. Runtime secrets come from Compose env_file.
ENV DATABASE_URL="postgresql://build:build@127.0.0.1:5432/build" \
    POSTGRES_URL="postgresql://build:build@127.0.0.1:5432/build" \
    NEXTAUTH_URL="http://localhost:3000" \
    NEXTAUTH_SECRET="build-only-secret" \
    GOOGLE_CLIENT_ID="build-only.apps.googleusercontent.com" \
    GOOGLE_CLIENT_SECRET="build-only-secret" \
    ALLOWED_EMAILS="build@example.invalid" \
    NEXT_PUBLIC_OWNER_NAME="Build" \
    NEXT_PUBLIC_BASE_URL="http://localhost:3000" \
    HERMES_BOARD="default" \
    HERMES_BIN="hermes" \
    HERMES_WIKI="/tmp/hermes-wiki" \
    BRIEF_HOUR="7" \
    INTERNAL_API_SECRET="build-only-secret" \
    CRON_SECRET="build-only-secret"

RUN npm run build

FROM node:22-bookworm-slim AS runner
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0

RUN groupadd --gid 1001 nodejs \
    && useradd --uid 1001 --gid 1001 --create-home nextjs

COPY --from=builder --chown=nextjs:nodejs /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs
EXPOSE 3000
CMD ["node", "server.js"]
