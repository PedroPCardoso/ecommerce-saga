-- CreateTable
CREATE TABLE "processed_messages" (
    "event_id" TEXT NOT NULL,
    "consumer_group" TEXT NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_messages_pkey" PRIMARY KEY ("event_id","consumer_group")
);
