import socket

# Define host and port
HOST = "localhost"
PORT = 9999

# Create a TCP socket server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Bind and listen for a connection from Spark
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print("======================================")
print("Socket Server Started")
print("Waiting for Spark to connect...")
print("Host:", HOST)
print("Port:", PORT)
print("======================================")

# Accept connection from Spark Structured Streaming
connection, address = server_socket.accept()
print("Spark connected from:", address)
print("Start entering messages.")
print("Type 'exit' to stop.")
print("--------------------------------------")

try:
    while True:
        message = input("Enter message: ")
        if message.lower() == "exit":
            break
        # Send message with trailing newline
        connection.sendall((message + "\n").encode("utf-8"))
finally:
    print("Closing connection...")
    connection.close()
    server_socket.close()
    print("Socket server stopped.")
