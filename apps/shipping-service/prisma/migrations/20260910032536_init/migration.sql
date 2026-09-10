-- CreateTable
CREATE TABLE "known_orders" (
    "order_id" TEXT NOT NULL,
    "shipping_address" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "known_orders_pkey" PRIMARY KEY ("order_id")
);

-- CreateTable
CREATE TABLE "shipments" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "carrier" TEXT NOT NULL,
    "tracking_code" TEXT NOT NULL,
    "label_url" TEXT NOT NULL,
    "estimated_delivery_at" TIMESTAMP(3) NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'CREATED',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shipments_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "outbox" (
    "id" BIGSERIAL NOT NULL,
    "event_id" TEXT NOT NULL,
    "aggregate_id" TEXT NOT NULL,
    "aggregate_type" TEXT NOT NULL,
    "event_type" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "headers" JSONB NOT NULL DEFAULT '{}',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "published_at" TIMESTAMP(3),
    "attempts" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "outbox_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "processed_messages" (
    "event_id" TEXT NOT NULL,
    "consumer_group" TEXT NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_messages_pkey" PRIMARY KEY ("event_id","consumer_group")
);

-- CreateIndex
CREATE UNIQUE INDEX "shipments_order_id_key" ON "shipments"("order_id");

-- CreateIndex
CREATE UNIQUE INDEX "outbox_event_id_key" ON "outbox"("event_id");

-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto.
CREATE INDEX "outbox_pending_idx" ON "outbox" ("created_at") WHERE "published_at" IS NULL;
