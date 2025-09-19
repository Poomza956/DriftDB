#!/usr/bin/env python3
"""Test time-travel queries via PostgreSQL protocol"""

import socket
import struct

def send_query(sock, query):
    """Send a query and get results"""
    send_message(sock, 'Q', (query + '\x00').encode('utf-8'))
    results = []

    while True:
        msg_type, data = recv_message(sock)
        if msg_type == 'T':  # Row description
            pass
        elif msg_type == 'D':  # Data row
            results.append(data)
        elif msg_type == 'C':  # Command complete
            tag = data.decode('utf-8', errors='ignore').rstrip('\x00')
            print(f"  Command: {tag}")
        elif msg_type == 'E':  # Error
            print(f"  Error: {data.decode('utf-8', errors='ignore')}")
        elif msg_type == 'Z':  # Ready for query
            break

    return results

def send_message(sock, msg_type, data):
    if msg_type:
        length = len(data) + 4
        msg = msg_type.encode('ascii') + struct.pack('!I', length) + data
    else:
        msg = data
    sock.send(msg)

def recv_message(sock):
    header = sock.recv(5)
    if len(header) < 5:
        return None, None
    msg_type = chr(header[0])
    length = struct.unpack('!I', header[1:5])[0]
    data = sock.recv(length - 4) if length > 4 else b''
    return msg_type, data

def main():
    # Connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 5433))

    # SSL negotiation
    sock.send(struct.pack('!II', 8, 80877103))
    sock.recv(1)  # Should be 'N'

    # Startup
    params = b'user\x00test\x00database\x00driftdb\x00\x00'
    sock.send(struct.pack('!II', 8 + len(params), 196608) + params)

    # Read until ready
    while True:
        msg_type, _ = recv_message(sock)
        if msg_type == 'Z':
            break

    print("🚀 DriftDB Time-Travel Demo via PostgreSQL")
    print("=" * 50)

    # Show current data
    print("\n📊 Current data in test_table:")
    send_query(sock, "SELECT * FROM test_table")

    # Insert more data
    print("\n➕ Adding more records...")
    send_query(sock, "INSERT INTO test_table (id, name) VALUES (2, 'Bob')")
    send_query(sock, "INSERT INTO test_table (id, name) VALUES (3, 'Charlie')")

    # Show updated data
    print("\n📊 After inserts:")
    send_query(sock, "SELECT * FROM test_table")

    # Query all tables in users (from our demo)
    print("\n📊 Users table (from demo data):")
    send_query(sock, "SELECT * FROM users")

    # Time travel query (if supported)
    print("\n⏰ Attempting time-travel query (AS OF @seq:1):")
    send_query(sock, "SELECT * FROM users AS OF @seq:1")

    print("\n✅ Demo complete!")

    # Terminate
    send_message(sock, 'X', b'')
    sock.close()

if __name__ == "__main__":
    main()