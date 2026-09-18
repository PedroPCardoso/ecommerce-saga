-- AlterTable
ALTER TABLE "orders" ADD COLUMN     "compensation_reason" TEXT,
ADD COLUMN     "compensations_received" JSONB NOT NULL DEFAULT '[]';
