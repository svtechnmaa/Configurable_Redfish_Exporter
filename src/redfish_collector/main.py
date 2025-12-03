from fastapi import FastAPI
from .routers import prometheus
import sys
import uvicorn
import argparse
from os import path

REDFISH_DATA = '/tmp/redfish-data/'

app = FastAPI(title="Redfish Collector", description="Redfish DMTF Collector using for physical server monitoring")
app.include_router(prometheus.router)

def main():
    parser = argparse.ArgumentParser(description='Physical Server state Exporter for Prometheus')

    parser.add_argument('--host', type=str, dest='host', default='0.0.0.0', help='address to serve on')
    parser.add_argument('--port', type=int, dest='port', default=9814, help='port to bind')
    parser.add_argument('--rotate', type=int, dest='rotate', default=300, help='log rotate interval in seconds')
    parser.add_argument('--workers', type=int, dest='workers', default=4, help='Number of worker processes to run Uvicorn')

    args = parser.parse_args()
    config_path = path.join(path.dirname(__file__), 'logging/logging.yml')
    try:
        uvicorn.run(
            "redfish_collector.main:app", 
            host=args.host, 
            port=args.port, 
            log_config=config_path,
            workers=args.workers,
            reload=False 
        )
        
    except KeyboardInterrupt:
        print("\nRedfish Exporter stopped.")
        sys.exit(0)
    except Exception as e:
        print(f"An unexpected error occurred during run execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
