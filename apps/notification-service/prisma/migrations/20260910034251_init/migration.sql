-- CreateTable
CREATE TABLE "known_orders" (
    "order_id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "known_orders_pkey" PRIMARY KEY ("order_id")
);

-- CreateTable
CREATE TABLE "processed_messages" (
    "event_id" TEXT NOT NULL,
    "consumer_group" TEXT NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_messages_pkey" PRIMARY KEY ("event_id","consumer_group")
);
