import streamlit as st
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time
import requests

# Page config
st.set_page_config(
    page_title="Agentic Flow Tester",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'outbound_messages' not in st.session_state:
    st.session_state.outbound_messages = []
if 'consumer_running' not in st.session_state:
    st.session_state.consumer_running = False
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []
if 'last_poll_time' not in st.session_state:
    st.session_state.last_poll_time = None
if 'poll_count' not in st.session_state:
    st.session_state.poll_count = 0

# Configuration
KAFKA_BROKERS = st.sidebar.text_input(
    "Kafka Brokers",
    value="localhost:19092",
    help="Kafka broker address (e.g., localhost:19092)"
)

TOPIC_IN = st.sidebar.text_input(
    "Inbound Topic",
    value="serve.vm.whatsapp.in",
    help="Kafka topic for inbound messages"
)

TOPIC_OUT = st.sidebar.text_input(
    "Outbound Topic",
    value="serve.vm.whatsapp.out",
    help="Kafka topic for outbound messages"
)

AGENT_API_URL = st.sidebar.text_input(
    "Agent API Base URL",
    value="http://localhost:8001",
    help="Base URL for the agent API (e.g., http://localhost:8001)"
)

# Kafka producer helper
def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: (k or "").encode('utf-8') if k else None
        )
    except Exception as e:
        st.error(f"Failed to create Kafka producer: {e}")
        return None

# Kafka consumer helper - with fixed group ID for persistence
def get_consumer(use_earliest=False):
    try:
        return KafkaConsumer(
            TOPIC_OUT,
            bootstrap_servers=KAFKA_BROKERS.split(','),
            group_id="streamlit-test-consumer",  # Fixed group ID
            auto_offset_reset='earliest' if use_earliest else 'latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=3000  # Increased timeout
        )
    except Exception as e:
        error_msg = f"Failed to create Kafka consumer: {e}"
        st.session_state.debug_logs.insert(0, {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'level': 'ERROR',
            'message': error_msg
        })
        return None

# Function to consume messages (non-blocking) with debug logging
def consume_messages_once(use_earliest=False):
    poll_start = time.time()
    st.session_state.poll_count += 1
    st.session_state.last_poll_time = datetime.now().strftime('%H:%M:%S')
    
    # Only log debug messages occasionally to avoid spam
    if st.session_state.poll_count % 10 == 1:  # Log every 10th poll
        debug_msg = f"Poll #{st.session_state.poll_count} started at {st.session_state.last_poll_time}"
        st.session_state.debug_logs.insert(0, {
            'timestamp': st.session_state.last_poll_time,
            'level': 'INFO',
            'message': debug_msg
        })
    
    consumer = get_consumer(use_earliest=use_earliest)
    if not consumer:
        return 0
    
    messages_received = 0
    try:
        # Poll for messages with longer timeout to catch more messages
        messages = consumer.poll(timeout_ms=2000)
        poll_duration = (time.time() - poll_start) * 1000
        
        if not messages:
            # Only log if no messages for a while
            if st.session_state.poll_count % 20 == 0:
                debug_msg = f"Poll #{st.session_state.poll_count}: No messages (took {poll_duration:.0f}ms)"
                st.session_state.debug_logs.insert(0, {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'level': 'INFO',
                    'message': debug_msg
                })
        else:
            for topic_partition, records in messages.items():
                for record in records:
                    messages_received += 1
                    if record.value:
                        # Check if message already exists (by offset) to avoid duplicates
                        existing_offsets = [m.get('offset') for m in st.session_state.outbound_messages if m.get('partition') == record.partition]
                        
                        if record.offset not in existing_offsets:
                            msg_data = {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'topic': record.topic,
                                'partition': record.partition,
                                'offset': record.offset,
                                'key': record.key,
                                'value': record.value
                            }
                            st.session_state.outbound_messages.insert(0, msg_data)
                            # Keep only last 100 messages
                            if len(st.session_state.outbound_messages) > 100:
                                st.session_state.outbound_messages.pop()
                            
                            debug_msg = f"✅ New message: offset={record.offset}, key={record.key}, type={record.value.get('type', 'unknown')}"
                            st.session_state.debug_logs.insert(0, {
                                'timestamp': datetime.now().strftime('%H:%M:%S'),
                                'level': 'SUCCESS',
                                'message': debug_msg
                            })
            
            if messages_received > 0:
                debug_msg = f"Poll #{st.session_state.poll_count}: Received {messages_received} message(s) (took {poll_duration:.0f}ms)"
                st.session_state.debug_logs.insert(0, {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'level': 'SUCCESS',
                    'message': debug_msg
                })
        
        # Keep only last 50 debug logs
        if len(st.session_state.debug_logs) > 50:
            st.session_state.debug_logs.pop()
            
    except Exception as e:
        error_msg = f"Error consuming messages: {e}"
        st.session_state.debug_logs.insert(0, {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'level': 'ERROR',
            'message': error_msg
        })
    finally:
        try:
            consumer.close()
        except:
            pass
    
    return messages_received

# Function to check topic metadata
def check_topic_metadata():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKERS.split(','),
            consumer_timeout_ms=1000
        )
        metadata = consumer.list_topics(timeout_ms=5000)
        
        topic_info = {}
        if TOPIC_OUT in metadata.topics:
            partitions_info = metadata.topics[TOPIC_OUT]
            topic_info = {
                'exists': True,
                'partition_count': len(partitions_info),
                'partitions': list(partitions_info.keys())
            }
        else:
            topic_info = {'exists': False}
        
        consumer.close()
        return topic_info
    except Exception as e:
        return {'error': str(e)}

# Main UI
st.title("🤖 Agentic Flow Tester")
st.markdown("Test your agentic flow by sending messages to Kafka and monitoring responses")

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🎯 Unified Flow", "🤖 Trigger Agent", "📤 Send Messages", "📥 Monitor Responses", "📊 Message History"])

# Tab 1: Unified Flow - Everything in one place
with tab1:
    st.header("🎯 Unified Agent Flow")
    st.markdown("Trigger your agent and see responses in real-time - all in one place!")
    
    # Two column layout: Left for controls, Right for messages
    main_col1, main_col2 = st.columns([1, 1])
    
    with main_col1:
        st.subheader("⚙️ Controls")
        
        # Agent endpoint selection
        agent_endpoint = st.selectbox(
            "Agent Endpoint",
            options=[
                "/agents/onboarding/start",
                "/agents/custom",
            ],
            help="Select the agent endpoint to trigger"
        )
        
        # Custom endpoint input
        if agent_endpoint == "/agents/custom":
            agent_endpoint = st.text_input(
                "Custom Endpoint",
                value="/agents/onboarding/start",
                help="Enter custom agent endpoint path"
            )
        
        # Tabs for different actions
        action_tab1, action_tab2 = st.tabs(["💬 Send Message", "🚀 Trigger Agent"])
        
        with action_tab1:
            st.markdown("**Send a message to the agent (simulates WhatsApp inbound)**")
            
            message_phone = st.text_input(
                "Phone Number (From)",
                value="1234567890",
                help="Simulated phone number for the sender",
                key="msg_phone"
            )
            
            message_text = st.text_area(
                "Message Text",
                height=100,
                placeholder="Type your message here...",
                help="The message content to send",
                key="msg_text"
            )
            
            send_msg_btn = st.button("📤 Send Message", type="primary", use_container_width=True, key="send_msg")
            
            if send_msg_btn:
                if not message_text:
                    st.warning("Please enter a message text")
                else:
                    producer = get_producer()
                    if producer:
                        try:
                            msg = {
                                "type": "wa.inbound.v1",
                                "data": {
                                    "from": message_phone,
                                    "wamid": f"wamid.test.{int(time.time())}",
                                    "text": message_text,
                                    "timestamp": str(int(time.time())),
                                    "profile_name": "Test User"
                                }
                            }
                            
                            future = producer.send(
                                TOPIC_IN,
                                key=message_phone,
                                value=msg
                            )
                            
                            # Wait for send confirmation
                            record_metadata = future.get(timeout=10)
                            
                            st.success(f"✅ Message sent to Kafka!")
                            
                            # Add to message history
                            st.session_state.messages.insert(0, {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'direction': 'sent_message',
                                'phone': message_phone,
                                'text': message_text,
                                'metadata': {
                                    'topic': record_metadata.topic,
                                    'partition': record_metadata.partition,
                                    'offset': record_metadata.offset
                                }
                            })
                            
                            producer.flush()
                            producer.close()
                            
                            # Auto-start consumer if not running
                            if not st.session_state.consumer_running:
                                st.session_state.consumer_running = True
                                st.info("🔄 Consumer auto-started to receive responses")
                            
                        except KafkaError as e:
                            st.error(f"❌ Kafka error: {e}")
                        except Exception as e:
                            st.error(f"❌ Error sending message: {e}")
                    else:
                        st.error("Failed to create Kafka producer. Check your Kafka connection settings.")
        
        with action_tab2:
            st.markdown("**Trigger agent API endpoint**")
            
            phone_number = st.text_input(
                "Phone Number",
                value="1234567890",
                help="Phone number for the agent to process",
                key="trigger_phone"
            )
            
            # Additional payload (optional)
            with st.expander("Additional Payload (Optional)"):
                additional_payload = st.text_area(
                    "JSON Payload",
                    height=100,
                    value='{}',
                    help="Additional JSON payload to send with the request",
                    key="trigger_payload"
                )
            
            trigger_btn = st.button("🚀 Trigger Agent", type="primary", use_container_width=True, key="trigger_btn")
        
        clear_btn = st.button("🗑️ Clear Messages", use_container_width=True, key="clear_unified")
        
        if clear_btn:
            st.session_state.outbound_messages = []
            st.session_state.messages = []
            st.rerun()
        
        # Consumer controls
        st.markdown("---")
        st.subheader("📡 Consumer Controls")
        
        col_cons1, col_cons2 = st.columns([1, 1])
        with col_cons1:
            if st.button("▶️ Start", disabled=st.session_state.consumer_running, use_container_width=True, key="unified_start"):
                st.session_state.consumer_running = True
                st.session_state.poll_count = 0
                st.rerun()
        
        with col_cons2:
            if st.button("⏹️ Stop", disabled=not st.session_state.consumer_running, use_container_width=True, key="unified_stop"):
                st.session_state.consumer_running = False
                st.rerun()
        
        auto_refresh = st.checkbox("🔄 Auto-refresh", value=True, help="Automatically poll for new messages every 1 second", key="unified_auto")
        use_earliest = st.checkbox("📖 Read from beginning", value=False, help="Read messages from the beginning of the topic", key="unified_earliest")
        
        # Manual refresh button
        if st.button("🔄 Refresh Now", use_container_width=True, key="manual_refresh_unified"):
            if st.session_state.consumer_running:
                consume_messages_once(use_earliest=use_earliest)
            st.rerun()
        
        # Status
        if st.session_state.consumer_running:
            st.success(f"🟢 Consumer Running | Polls: {st.session_state.poll_count}")
            if st.session_state.last_poll_time:
                st.caption(f"Last poll: {st.session_state.last_poll_time}")
        else:
            st.info("⚪ Consumer Stopped")
    
    with main_col2:
        st.subheader("💬 Messages")
        
        # Trigger agent
        if trigger_btn:
            if not phone_number:
                st.warning("Please enter a phone number")
            else:
                try:
                    # Parse payload
                    try:
                        payload = json.loads(additional_payload) if additional_payload else {}
                    except json.JSONDecodeError:
                        st.error("Invalid JSON in additional payload. Using default.")
                        payload = {}
                    
                    payload['phone'] = phone_number
                    
                    # Make API request
                    url = f"{AGENT_API_URL}{agent_endpoint}"
                    
                    with st.spinner("Triggering agent..."):
                        response = requests.post(
                            url,
                            json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=30
                        )
                    
                    # Display response
                    if response.status_code == 200:
                        try:
                            response_data = response.json()
                            # Add trigger event to messages
                            st.session_state.messages.insert(0, {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'direction': 'agent_trigger',
                                'phone': phone_number,
                                'endpoint': agent_endpoint,
                                'response': response_data,
                                'status_code': response.status_code
                            })
                            st.success(f"✅ Agent triggered! Waiting for messages...")
                        except:
                            st.success(f"✅ Agent triggered! (Status: {response.status_code})")
                    else:
                        st.error(f"❌ Agent API error (Status: {response.status_code})")
                    
                    # Auto-start consumer if not running
                    if not st.session_state.consumer_running:
                        st.session_state.consumer_running = True
                        st.info("🔄 Consumer auto-started")
                    
                except requests.exceptions.ConnectionError:
                    st.error(f"❌ Could not connect to agent API at {AGENT_API_URL}")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        
        # Display messages in chat-like format
        messages_container = st.container()
        
        with messages_container:
            # Show all messages (both triggers and responses)
            all_messages = []
            
            # Add all messages from session state
            for msg in st.session_state.messages:
                if msg.get('direction') == 'agent_trigger':
                    all_messages.append({
                        'timestamp': msg['timestamp'],
                        'type': 'trigger',
                        'phone': msg.get('phone', ''),
                        'endpoint': msg.get('endpoint', ''),
                        'response': msg.get('response', {})
                    })
                elif msg.get('direction') == 'sent_message':
                    all_messages.append({
                        'timestamp': msg['timestamp'],
                        'type': 'sent',
                        'phone': msg.get('phone', ''),
                        'text': msg.get('text', '')
                    })
            
            # Add outbound messages (from agent)
            for msg in st.session_state.outbound_messages:
                all_messages.append({
                    'timestamp': msg['timestamp'],
                    'type': 'response',
                    'data': msg['value']
                })
            
            # Sort by timestamp (newest first)
            all_messages.sort(key=lambda x: x['timestamp'], reverse=True)
            
            if all_messages:
                for msg in all_messages[:50]:  # Show last 50
                    if msg['type'] == 'trigger':
                        with st.expander(f"🚀 Triggered Agent - {msg['timestamp']}", expanded=False):
                            st.write(f"**Phone:** {msg['phone']}")
                            st.write(f"**Endpoint:** `{msg['endpoint']}`")
                            if msg.get('response'):
                                st.json(msg['response'])
                    elif msg['type'] == 'sent':
                        # Sent message display
                        st.markdown(f"""
                        <div style="background-color: #e3f2fd; padding: 10px; border-radius: 10px; margin: 5px 0; text-align: right;">
                            <strong>📤 You</strong> - {msg['timestamp']}<br>
                            {msg['text']}
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        # Response message
                        msg_data = msg['data']
                        msg_type = msg_data.get('type', 'unknown')
                        msg_text = ""
                        
                        if msg_type == 'wa.outbound.v1':
                            msg_text = msg_data.get('data', {}).get('text', 'No text')
                            to_phone = msg_data.get('data', {}).get('to', 'Unknown')
                        else:
                            msg_text = str(msg_data)
                        
                        # Display as chat message
                        st.markdown(f"""
                        <div style="background-color: #f0f2f6; padding: 10px; border-radius: 10px; margin: 5px 0;">
                            <strong>🤖 Agent Response</strong> - {msg['timestamp']}<br>
                            {msg_text}
                        </div>
                        """, unsafe_allow_html=True)
                        
                        with st.expander("📋 Full Message Details", expanded=False):
                            st.json(msg_data)
            else:
                st.info("No messages yet. Trigger the agent to see responses here!")
        
        # Auto-refresh and consume messages - more aggressive polling
        if st.session_state.consumer_running:
            # Always consume when consumer is running
            messages_received = consume_messages_once(use_earliest=use_earliest)
            
            # If auto-refresh is enabled, rerun more frequently
            if auto_refresh:
                # Shorter sleep for more responsive updates
                time.sleep(1)
                st.rerun()

# Tab 2: Trigger Agent
with tab2:
    st.header("🤖 Trigger Agent API")
    st.markdown("Trigger your agent endpoints and see the messages flow through Kafka")
    
    # Agent endpoint selection
    agent_endpoint_t2 = st.selectbox(
        "Agent Endpoint",
        options=[
            "/agents/onboarding/start",
            "/agents/custom",
        ],
        help="Select the agent endpoint to trigger",
        key="endpoint_t2"
    )
    
    # Custom endpoint input
    if agent_endpoint_t2 == "/agents/custom":
        agent_endpoint_t2 = st.text_input(
            "Custom Endpoint",
            value="/agents/onboarding/start",
            help="Enter custom agent endpoint path",
            key="custom_t2"
        )
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        phone_number_t2 = st.text_input(
            "Phone Number",
            value="1234567890",
            help="Phone number for the agent to process",
            key="phone_t2"
        )
        
        # Additional payload (optional)
        st.markdown("### Additional Payload (Optional)")
        additional_payload_t2 = st.text_area(
            "JSON Payload",
            height=100,
            value='{}',
            help="Additional JSON payload to send with the request",
            key="payload_t2"
        )
    
    with col2:
        st.markdown("### Request Preview")
        try:
            payload = json.loads(additional_payload_t2)
            payload['phone'] = phone_number_t2
            st.json({
                "url": f"{AGENT_API_URL}{agent_endpoint_t2}",
                "method": "POST",
                "payload": payload
            })
        except json.JSONDecodeError:
            st.warning("Invalid JSON in additional payload")
            payload = {"phone": phone_number_t2}
    
    col_btn1, col_btn2 = st.columns([1, 1])
    
    with col_btn1:
        trigger_btn_t2 = st.button("🚀 Trigger Agent", type="primary", use_container_width=True, key="trigger_t2")
    
    with col_btn2:
        # Auto-start consumer when triggering
        auto_start_consumer = st.checkbox("Auto-start consumer", value=True, help="Automatically start monitoring for responses", key="auto_t2")
    
    if trigger_btn_t2:
        if not phone_number_t2:
            st.warning("Please enter a phone number")
        else:
            try:
                # Parse payload
                try:
                    payload = json.loads(additional_payload_t2) if additional_payload_t2 else {}
                except json.JSONDecodeError:
                    st.error("Invalid JSON in additional payload. Using default.")
                    payload = {}
                
                payload['phone'] = phone_number_t2
                
                # Make API request
                url = f"{AGENT_API_URL}{agent_endpoint_t2}"
                st.info(f"🔄 Calling: `POST {url}`")
                
                with st.spinner("Triggering agent..."):
                    response = requests.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=30
                    )
                
                # Display response
                if response.status_code == 200:
                    st.success(f"✅ Agent triggered successfully! (Status: {response.status_code})")
                    try:
                        response_data = response.json()
                        st.json(response_data)
                        
                        # Add to session state
                        st.session_state.messages.insert(0, {
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'direction': 'agent_trigger',
                            'phone': phone_number_t2,
                            'endpoint': agent_endpoint_t2,
                            'response': response_data,
                            'status_code': response.status_code
                        })
                    except:
                        st.text(f"Response: {response.text}")
                else:
                    st.error(f"❌ Agent API returned error (Status: {response.status_code})")
                    st.text(f"Response: {response.text}")
                
                # Auto-start consumer if enabled
                if auto_start_consumer:
                    st.session_state.consumer_running = True
                    st.info("✅ Consumer started. Monitoring for agent responses...")
                    st.markdown("💡 **Tip:** Check the 'Monitor Responses' tab to see messages from the agent!")
                
            except requests.exceptions.ConnectionError:
                st.error(f"❌ Could not connect to agent API at {AGENT_API_URL}. Make sure the agent server is running.")
            except requests.exceptions.Timeout:
                st.error("❌ Request timed out. The agent may be taking too long to respond.")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    st.markdown("---")
    st.markdown("### 📋 How it works:")
    st.markdown("""
    1. **Trigger Agent**: Click the button above to call your agent API
    2. **Agent Processes**: Your agent receives the request and processes it
    3. **Messages to Kafka**: The agent sends messages to the Kafka outbound topic
    4. **Monitor Responses**: Check the "Monitor Responses" tab to see messages in real-time
    """)

# Tab 2: Send Messages
with tab2:
    st.header("Send Test Message")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        phone_number = st.text_input(
            "Phone Number (From)",
            value="1234567890",
            help="Simulated phone number for the sender"
        )
        
        message_text = st.text_area(
            "Message Text",
            height=100,
            placeholder="Enter your test message here...",
            help="The message content to send"
        )
    
    with col2:
        st.markdown("### Message Preview")
        if message_text:
            preview_msg = {
                "type": "wa.inbound.v1",
                "data": {
                    "from": phone_number,
                    "wamid": f"wamid.test.{int(time.time())}",
                    "text": message_text,
                    "timestamp": str(int(time.time())),
                    "profile_name": "Test User"
                }
            }
            st.json(preview_msg)
    
    if st.button("🚀 Send Message", type="primary", use_container_width=True):
        if not message_text:
            st.warning("Please enter a message text")
        else:
            producer = get_producer()
            if producer:
                try:
                    msg = {
                        "type": "wa.inbound.v1",
                        "data": {
                            "from": phone_number,
                            "wamid": f"wamid.test.{int(time.time())}",
                            "text": message_text,
                            "timestamp": str(int(time.time())),
                            "profile_name": "Test User"
                        }
                    }
                    
                    future = producer.send(
                        TOPIC_IN,
                        key=phone_number,
                        value=msg
                    )
                    
                    # Wait for send confirmation
                    record_metadata = future.get(timeout=10)
                    
                    st.success(f"✅ Message sent successfully!")
                    st.info(f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                    
                    # Add to message history
                    st.session_state.messages.insert(0, {
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'direction': 'outbound',
                        'phone': phone_number,
                        'text': message_text,
                        'metadata': {
                            'topic': record_metadata.topic,
                            'partition': record_metadata.partition,
                            'offset': record_metadata.offset
                        }
                    })
                    
                    producer.flush()
                    producer.close()
                    
                except KafkaError as e:
                    st.error(f"❌ Kafka error: {e}")
                except Exception as e:
                    st.error(f"❌ Error sending message: {e}")
            else:
                st.error("Failed to create Kafka producer. Check your Kafka connection settings.")

# Tab 3: Monitor Responses
with tab3:
    st.header("Monitor Outbound Messages")
    
    # Status and controls
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        auto_refresh = st.checkbox("🔄 Auto-refresh (every 2 seconds)", value=False)
        use_earliest = st.checkbox("📖 Read from beginning (earliest)", value=False, 
                                   help="Read messages from the beginning of the topic instead of latest")
    
    with col2:
        if st.button("🔄 Refresh Now"):
            if st.session_state.consumer_running:
                consume_messages_once(use_earliest=use_earliest)
            st.rerun()
    
    with col3:
        if st.button("🔍 Check Topic"):
            with st.spinner("Checking topic..."):
                topic_info = check_topic_metadata()
                if 'error' in topic_info:
                    st.error(f"Error: {topic_info['error']}")
                elif topic_info.get('exists'):
                    st.success(f"✅ Topic exists: {topic_info['partition_count']} partition(s)")
                else:
                    st.warning(f"⚠️ Topic '{TOPIC_OUT}' not found!")
    
    col_btn1, col_btn2 = st.columns([1, 1])
    
    with col_btn1:
        if st.button("▶️ Start Consumer", disabled=st.session_state.consumer_running, use_container_width=True):
            st.session_state.consumer_running = True
            st.session_state.poll_count = 0
            st.session_state.debug_logs = []
            st.success("✅ Consumer started. Messages will appear below as they arrive.")
            st.rerun()
    
    with col_btn2:
        if st.button("⏹️ Stop Consumer", disabled=not st.session_state.consumer_running, use_container_width=True):
            st.session_state.consumer_running = False
            st.info("Consumer stopped.")
            st.rerun()
    
    # Status display
    if st.session_state.consumer_running:
        status_col1, status_col2, status_col3 = st.columns(3)
        with status_col1:
            st.metric("Status", "🟢 Running")
        with status_col2:
            st.metric("Polls", st.session_state.poll_count)
        with status_col3:
            last_poll = st.session_state.last_poll_time or "Never"
            st.metric("Last Poll", last_poll)
    
    # Display outbound messages
    if st.session_state.outbound_messages:
        st.markdown(f"### 📨 Received Messages ({len(st.session_state.outbound_messages)})")
        
        for idx, msg in enumerate(st.session_state.outbound_messages[:20]):  # Show last 20
            with st.expander(f"Message {idx + 1} - {msg['timestamp']}"):
                st.json(msg['value'])
                st.caption(f"Key: {msg['key']} | Partition: {msg['partition']} | Offset: {msg['offset']}")
    else:
        st.info("No outbound messages received yet. Start the consumer and trigger the agent.")
    
    # Debug logs section
    with st.expander("🔍 Debug Logs", expanded=False):
        if st.button("🗑️ Clear Logs"):
            st.session_state.debug_logs = []
            st.rerun()
        
        if st.session_state.debug_logs:
            for log in st.session_state.debug_logs[:30]:  # Show last 30
                level_emoji = {
                    'INFO': 'ℹ️',
                    'SUCCESS': '✅',
                    'ERROR': '❌',
                    'WARNING': '⚠️'
                }
                emoji = level_emoji.get(log['level'], '📝')
                color = {
                    'INFO': '',
                    'SUCCESS': ':green[',
                    'ERROR': ':red[',
                    'WARNING': ':orange['
                }.get(log['level'], '')
                st.text(f"{emoji} [{log['timestamp']}] {color}{log['message']}{']' if color else ''}")
        else:
            st.info("No debug logs yet. Start the consumer to see activity.")
    
    # Auto-refresh logic
    if auto_refresh and st.session_state.consumer_running:
        consume_messages_once(use_earliest=use_earliest)
        time.sleep(2)
        st.rerun()
    
    # Manual consume on refresh (if consumer is running)
    if st.session_state.consumer_running and not auto_refresh:
        consume_messages_once(use_earliest=use_earliest)

# Tab 4: Message History
with tab4:
    st.header("Message History")
    
    if st.button("🗑️ Clear History"):
        st.session_state.messages = []
        st.session_state.outbound_messages = []
        st.rerun()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📤 Sent Messages")
        if st.session_state.messages:
            for msg in st.session_state.messages[:50]:  # Show last 50
                if msg.get('direction') == 'outbound':
                    with st.expander(f"📤 {msg['timestamp']} - {msg['phone']}"):
                        st.write(f"**Text:** {msg['text']}")
                        if 'metadata' in msg:
                            st.caption(f"Topic: {msg['metadata'].get('topic')} | Partition: {msg['metadata'].get('partition')} | Offset: {msg['metadata'].get('offset')}")
        else:
            st.info("No sent messages yet.")
    
    with col2:
        st.subheader("📥 Received Messages")
        if st.session_state.outbound_messages:
            for msg in st.session_state.outbound_messages[:50]:  # Show last 50
                with st.expander(f"📥 {msg['timestamp']}"):
                    st.json(msg['value'])
                    st.caption(f"Key: {msg['key']} | Partition: {msg['partition']} | Offset: {msg['offset']}")
        else:
            st.info("No received messages yet.")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### Connection Status")
try:
    producer_test = get_producer()
    if producer_test:
        producer_test.close()
        st.sidebar.success("✅ Kafka connection OK")
    else:
        st.sidebar.error("❌ Kafka connection failed")
except Exception as e:
    st.sidebar.error(f"❌ Connection error: {e}")

# Note: Consumer runs on each refresh/rerun when enabled

