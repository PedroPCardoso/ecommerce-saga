-- CreateTable
CREATE TABLE "orchestrated_orders" (
    "id" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "amount_cents" INTEGER NOT NULL,
    "currency" TEXT NOT NULL,
    "items" JSONB NOT NULL,
    "address" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "orchestrated_orders_pkey" PRIMARY KEY ("id")
);
