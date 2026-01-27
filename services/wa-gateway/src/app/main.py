import os, json, asyncio, logging
from fastapi import FastAPI, Request, HTTPException, Query
from pydantic_settings import BaseSettings, SettingsConfigDict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import httpx, pathlib

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("wa-gateway")

class Settings(BaseSettings):
    KAFKA_BROKERS: str = "host.docker.internal:19092"
    TOPIC_IN: str = "serve.vm.whatsapp.in"
    TOPIC_OUT: str = "serve.vm.whatsapp.out"
    WHATSAPP_PHONE_NUMBER_ID: str = "REPLACE_ME"
    WHATSAPP_TOKEN: str = "REPLACE_ME"
    VERIFY_TOKEN: str = "serve-wa-verify-123"
    GRAPH_BASE: str = "https://graph.facebook.com/v21.0"
    PORT: int = 8088
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

S = Settings()
app = FastAPI(title="serve-wa-gateway")

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
client = httpx.AsyncClient(timeout=30)

def _val_ser(v): return json.dumps(v, ensure_ascii=False).encode('utf-8')
def _key_ser(k): return (k or "").encode('utf-8')

def _normalize(s: str | None) -> str:
    return (s or "").strip().strip('"').strip("'")

def _build_message_payload(
    to: str,
    text: str,
    buttons: list | None = None,
    template: dict | None = None,
) -> dict:
    """
    Build WhatsApp Graph API message payload.
    Priority:
      1) If template is provided, send a template message
      2) Else if buttons are provided, send interactive button message
      3) Else send plain text message
    
    Args:
        to: Recipient phone number
        text: Message body text (used for text / interactive messages)
        buttons: Optional list of button dicts with 'id' and 'title' keys
        template: Optional dict with WhatsApp template payload
    
    Returns:
        Graph API payload dict
    """
    # 1) Template messages (for first prod contact / outside 24h window)
    if template:
        # Expect shape like:
        # { "name": "serve_welcome", "language": {"code": "en"}, ...optional components... }
        return {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": template,
        }

    # 2) No buttons -> plain text
    if not buttons:
        return {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        }
    
    # Validate and normalize buttons
    valid_buttons = []
    for btn in buttons[:3]:  # Max 3 buttons
        if not isinstance(btn, dict):
            continue
        btn_id = str(btn.get("id", "")).strip()
        btn_title = str(btn.get("title", "")).strip()
        
        if not btn_id or not btn_title:
            continue
        
        # Trim lengths: title max 20 chars, id max 256 chars (Graph API limits)
        btn_title = btn_title[:20]
        btn_id = btn_id[:256]
        
        valid_buttons.append({
            "type": "reply",
            "reply": {
                "id": btn_id,
                "title": btn_title
            }
        })
    
    if not valid_buttons:
        # Fallback to text if no valid buttons
        return {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        }
    
    return {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": text
            },
            "action": {
                "buttons": valid_buttons
            }
        }
    }



@app.on_event("startup")
async def startup():
    global producer, consumer
    log.info("Starting WA-Gateway from file: %s", pathlib.Path(__file__).resolve())
    log.info("ENV VERIFY_TOKEN loaded (raw) = %r", S.VERIFY_TOKEN)

    producer = AIOKafkaProducer(
        bootstrap_servers=S.KAFKA_BROKERS,
        value_serializer=_val_ser,
        key_serializer=_key_ser
    )
    await producer.start()

    consumer = AIOKafkaConsumer(
        S.TOPIC_OUT,
        bootstrap_servers=S.KAFKA_BROKERS,
        group_id="wa-gateway-out",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
    )
    await consumer.start()
    asyncio.create_task(outbound_loop())
    log.info("WA-Gateway started. Outbound topic=%s", S.TOPIC_OUT)

@app.on_event("shutdown")
async def shutdown():
    if consumer: await consumer.stop()
    if producer: await producer.stop()
    await client.aclose()

# ---------- HEALTH / DEBUG ----------
@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/_env")
async def env_echo():
    # MASK token
    vt = (S.VERIFY_TOKEN or "")
    masked = vt[:3] + "***" + vt[-3:] if len(vt) >= 6 else "***"
    return {"verify_token_loaded": masked}

# ---------- VERIFY (GET) ----------
@app.get("/wa/inbound")
async def verify(request: Request):
    # Read raw query dict so dotted keys work
    q = dict(request.query_params)
    log.info("VERIFY raw query: %r", q)

    mode = _normalize(q.get("hub.mode") or q.get("mode"))
    token_in = _normalize(q.get("hub.verify_token") or q.get("verify_token"))
    challenge = _normalize(q.get("hub.challenge") or q.get("challenge"))
    expected = _normalize(S.VERIFY_TOKEN)

    log.info("VERIFY compare -> mode=%r token_in=%r expected=%r challenge=%r",
             mode, token_in, expected, challenge)

    if mode == "subscribe" and token_in == expected:
        return int(challenge) if (challenge.isdigit()) else (challenge or "")
    raise HTTPException(status_code=403, detail="Verification failed")

# ---------- INBOUND (POST) ----------
@app.post("/wa/inbound")
async def inbound(req: Request):
    body = await req.json()
    log.info("Webhook received: %s", json.dumps(body, indent=2))
    entries = body.get("entry", [])
    for e in entries:
        for c in e.get("changes", []):
            v = c.get("value", {})
            
            # Handle message status updates (delivery, read, etc.)
            if "statuses" in v:
                for status in v.get("statuses", []):
                    log.info("Message status update: id=%s, status=%s, recipient=%s", 
                             status.get("id"), status.get("status"), status.get("recipient_id"))
                    if status.get("status") == "failed":
                        log.error("Message delivery FAILED: %s", json.dumps(status, indent=2))
            
            contacts = v.get("contacts", [])
            profile_name = (contacts[0].get("profile", {}).get("name") if contacts else None)
            for m in v.get("messages", []) or []:
                # Check for button reply
                interactive = m.get("interactive", {})
                button_reply = interactive.get("button_reply", {})
                button_id = button_reply.get("id")
                
                # If button reply exists, use its id as the text (for agent to switch on)
                # Otherwise use regular text body
                text_content = button_id if button_id else (m.get("text") or {}).get("body")
                
                msg = {
                    "type": "wa.inbound.v1",
                    "data": {
                        "from": m.get("from"),
                        "wamid": m.get("id"),
                        "text": text_content,
                        "timestamp": m.get("timestamp"),
                        "profile_name": profile_name
                    }
                }
                
                # Add button metadata if it's a button reply
                if button_id:
                    msg["data"]["button_id"] = button_id
                    msg["data"]["button_title"] = button_reply.get("title")
                
                key = msg["data"]["from"]
                await producer.send_and_wait(S.TOPIC_IN, key=key, value=msg)
                log.info("Inbound from %s: %s", key, msg["data"]["text"])
    return {"ok": True}

# ---------- OUTBOUND LOOP ----------
async def outbound_loop():
    async for rec in consumer:
        evt, key = rec.value, rec.key
        if evt.get("type") != "wa.outbound.v1":
            continue
        data = evt.get("data", {}) or {}
        to = data["to"]
        text = data.get("text") or ""
        buttons = data.get("buttons")  # Optional list of {id, title} dicts
        template = data.get("template")  # Optional WhatsApp template dict
        
        url = f"{S.GRAPH_BASE}/{S.WHATSAPP_PHONE_NUMBER_ID}/messages"
        payload = _build_message_payload(to, text, buttons, template)
        
        # Debug logging before POST
        p_type = payload.get("type")
        log.info("Sending message to %s: type=%s", to, p_type)
        if p_type == "interactive":
            button_info = [
                {"id": b.get("reply", {}).get("id"), "title": b.get("reply", {}).get("title")}
                for b in payload.get("interactive", {}).get("action", {}).get("buttons", [])
            ]
            log.info("Interactive buttons: %s", json.dumps(button_info, indent=2))
        elif p_type == "template":
            t = payload.get("template", {}) or {}
            log.info(
                "Template details: name=%s language=%s",
                t.get("name"),
                (t.get("language") or {}).get("code"),
            )
        
        headers = {
            "Authorization": f"Bearer {S.WHATSAPP_TOKEN}",
            "Content-Type": "application/json; charset=utf-8"
        }
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code >= 400:
            log.error("Send failed (%s): %s", r.status_code, r.text)
        else:
            log.info("Sent to %s (status=%s, response=%s): %s", to, r.status_code, r.text, text)
