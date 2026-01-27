# serve-wa-gateway (WhatsApp Cloud API → Kafka)

A thin gateway that:
- Receives WhatsApp Cloud API webhooks at `/wa/inbound` and produces messages to Kafka (`serve.vm.whatsapp.in`)
- Consumes Kafka messages from `serve.vm.whatsapp.out` and sends them via WhatsApp Cloud API

## Env
Copy `.env.example` to `.env` and fill:
- `WHATSAPP_PHONE_NUMBER_ID` (from Meta dashboard)
- `WHATSAPP_TOKEN` (temporary or permanent)
- `VERIFY_TOKEN` (any string; must match the one you set in Meta)

If running inside Docker on Windows/Mac, `KAFKA_BROKERS` should be `host.docker.internal:19092`.

## Run with infra compose
In the infra repo root:
```bash
docker compose up -d wa-gateway
docker compose logs -f wa-gateway
