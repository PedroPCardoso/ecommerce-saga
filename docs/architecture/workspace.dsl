/*
 * Modelo C4 do Sistema de Pedidos.
 *
 * Um modelo, várias views: Contexto (L1) e Contêineres (L2) estão prontos.
 * Componentes (L3) e a máquina de estados (L4) entram na Fase 8.
 *
 * Ver localmente:  http://localhost:8081   (container `structurizr` do compose)
 */
workspace "Sistema de Pedidos de E-commerce" "SAGA coreografada sobre Kafka" {

    model {
        cliente = person "Cliente" "Faz pedidos e acompanha o status pelo app."
        operador = person "Operador de Loja" "Investiga pedidos travados e reprocessa mensagens da DLT."

        gatewayPagamento = softwareSystem "Gateway de Pagamento" "Autoriza e estorna cobranças. Devolve token opaco — nunca guardamos PAN." {
            tags "Externo"
        }
        transportadora = softwareSystem "Transportadora" "Emite etiqueta e código de rastreio." {
            tags "Externo"
        }
        provedorEmail = softwareSystem "Provedor de E-mail" "Entrega as notificações transacionais." {
            tags "Externo"
        }

        sistema = softwareSystem "Sistema de Pedidos" "Processa o pedido de ponta a ponta com SAGA coreografada, compensando o que já foi efetivado quando algum passo falha." {

            kafka = container "Kafka" "Log de eventos particionado por orderId. Retenção de 7 dias, o que torna replay uma operação de rotina." "Apache Kafka 3.9 (KRaft)" {
                tags "Broker"
            }

            orderService = container "Order Service" "Recebe o pedido, projeta o estado a partir dos eventos e vigia timeouts da saga." "NestJS / TypeScript" {
                tags "Servico"
            }
            paymentService = container "Payment Service" "Autoriza o pagamento e estorna quando um passo POSTERIOR da saga falha." "NestJS / TypeScript" {
                tags "Servico"
            }
            inventoryService = container "Inventory Service" "Reserva estoque com prazo de expiração e libera na compensação." "NestJS / TypeScript" {
                tags "Servico"
            }
            shippingService = container "Shipping Service" "Cria a etiqueta de envio." "NestJS / TypeScript" {
                tags "Servico"
            }
            notificationService = container "Notification Service" "Consome tudo e notifica o cliente. Não publica evento de negócio." "NestJS / TypeScript" {
                tags "Servico"
            }

            orderDb = container "order_db" "Pedidos, outbox, processed_messages, chaves de idempotência HTTP." "PostgreSQL 17" {
                tags "Banco"
            }
            paymentDb = container "payment_db" "Pagamentos, estornos, outbox, processed_messages." "PostgreSQL 17" {
                tags "Banco"
            }
            inventoryDb = container "inventory_db" "Saldo, reservas, outbox, processed_messages." "PostgreSQL 17" {
                tags "Banco"
            }
            shippingDb = container "shipping_db" "Envios, outbox, processed_messages." "PostgreSQL 17" {
                tags "Banco"
            }
            notificationDb = container "notification_db" "Histórico de notificações, processed_messages." "PostgreSQL 17" {
                tags "Banco"
            }
        }

        # --- Contexto -------------------------------------------------------
        cliente -> sistema "Cria pedidos e consulta status"
        operador -> sistema "Investiga a saga e reprocessa a DLT"
        sistema -> gatewayPagamento "Autoriza e estorna" "HTTPS"
        sistema -> transportadora "Solicita etiqueta" "HTTPS"
        sistema -> provedorEmail "Envia notificação" "SMTP"

        # --- Contêineres: borda ---------------------------------------------
        cliente -> orderService "POST /orders, GET /orders/{id}" "HTTPS + JWT"
        operador -> orderService "Consulta a saga" "HTTPS + JWT"

        # --- Publicação (via outbox, jamais direto do handler) --------------
        orderService -> kafka "Publica order.created / confirmed / cancelled" "ecommerce.orders.v1"
        paymentService -> kafka "Publica payment.approved / failed / refunded" "ecommerce.payments.v1"
        inventoryService -> kafka "Publica stock.reserved / unavailable / released" "ecommerce.inventory.v1"
        shippingService -> kafka "Publica shipment.created / failed" "ecommerce.shipping.v1"

        # --- Consumo --------------------------------------------------------
        # Repare no Payment: ele assina inventory e shipping, domínios que não são dele.
        # Esse é o acoplamento implícito da coreografia (ADR-0002).
        kafka -> orderService "payments, inventory, shipping" "grupo order-projection"
        kafka -> paymentService "orders (autoriza) + inventory e shipping (COMPENSA)" "grupo payment-service"
        kafka -> inventoryService "payments (reserva) + shipping (COMPENSA)" "grupo inventory-service"
        kafka -> shippingService "inventory" "grupo shipping-service"
        kafka -> notificationService "os 4 tópicos" "grupo notification-service"

        # --- Persistência ---------------------------------------------------
        orderService -> orderDb "Efeito de negócio + outbox na MESMA transação" "TCP/5432"
        paymentService -> paymentDb "Efeito de negócio + outbox na MESMA transação" "TCP/5432"
        inventoryService -> inventoryDb "Efeito de negócio + outbox na MESMA transação" "TCP/5432"
        shippingService -> shippingDb "Efeito de negócio + outbox na MESMA transação" "TCP/5432"
        notificationService -> notificationDb "Histórico + idempotência" "TCP/5432"

        # --- Integrações externas -------------------------------------------
        paymentService -> gatewayPagamento "Autoriza / estorna" "HTTPS"
        shippingService -> transportadora "Cria etiqueta" "HTTPS"
        notificationService -> provedorEmail "Envia e-mail" "SMTP"
    }

    views {
        systemContext sistema "L1-Contexto" {
            include *
            autolayout lr
            description "Nível 1: o sistema, quem o usa e de quem ele depende."
        }

        container sistema "L2-Conteineres" {
            include *
            autolayout tb
            description "Nível 2: os 5 serviços, o broker e um banco por serviço. Nenhuma flecha liga serviço a serviço: toda comunicação de negócio passa pelo log de eventos."
        }

        styles {
            element "Person" {
                shape person
                background #1168bd
                color #ffffff
            }
            element "Externo" {
                background #999999
                color #ffffff
            }
            element "Servico" {
                background #438dd5
                color #ffffff
            }
            element "Banco" {
                shape cylinder
                background #2e7d32
                color #ffffff
            }
            element "Broker" {
                shape pipe
                background #b71c1c
                color #ffffff
            }
        }

        theme default
    }
}
