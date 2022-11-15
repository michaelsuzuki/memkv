import asyncio
import os
import sys

src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
sys.path.append(src_path)

from memkv.server.server import Server



def main():
    try:
        server = Server(port=9011)
        asyncio.run(server.run())
    except KeyboardInterrupt:
        server.terminate()
    finally:
        server.terminate()


if __name__ == "__main__":
    main()