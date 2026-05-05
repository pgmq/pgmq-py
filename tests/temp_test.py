from pgmq import PGMQueue
from dotenv import load_dotenv

load_dotenv()
# Connect using environment variables or defaults
queue = PGMQueue()

# Create a queue
queue.create_queue("my_queue")

# Send a message
msg_id = queue.send("my_queue", {"task": "send_email", "to": "user@example.com"})
print(f"Sent message {msg_id}")

# Read a message (invisible for 30 seconds)
msg = queue.read("my_queue", vt=30)
print(f"Received: {msg.message}")

# Archive it when done
queue.archive("my_queue", msg.msg_id)

print(queue.host)
