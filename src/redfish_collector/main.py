from contextlib import asynccontextmanager

from fastapi import FastAPI
from .core.config.profile import ProfileValidationError
from .routers import prometheus
import logging
import sys
import uvicorn
import argparse
from os import path


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load and validate EVERY deployment profile before serving any
    # request, enforcing process-wide Tuning equality across all of them —
    # otherwise whichever config a request happens to name first would
    # silently pick the process-wide limits every other target is also
    # bound by (Migration & Rollback / contracts/deployment-profile-contract.md
    # "Process-wide consistency across profile files").
    try:
        _, shutdown_grace_seconds = prometheus.startup_validate_and_build_registry()
    except ProfileValidationError as exc:
        logging.error("FATAL: profile validation failed at startup: %s", exc)
        raise SystemExit(2) from exc
    try:
        yield
    finally:
        registry = prometheus._TARGET_REGISTRY
        if registry is not None:
            await registry.shutdown_drain(grace_seconds=shutdown_grace_seconds)


app = FastAPI(
    title="Redfish Collector", description="Redfish DMTF Collector using for physical server monitoring",
    lifespan=lifespan,
)
app.include_router(prometheus.router)

def main():
    parser = argparse.ArgumentParser(description='Physical Server state Exporter for Prometheus')

    parser.add_argument('--host', type=str, dest='host', default='0.0.0.0', help='address to serve on')
    parser.add_argument('--port', type=int, dest='port', default=9814, help='port to bind')
    # NOTE: --rotate is accepted for backward compatibility but not yet wired to
    # any behaviour. Kept as a no-op so existing launch commands don't break.
    parser.add_argument('--rotate', type=int, dest='rotate', default=300, help='log rotate interval in seconds (currently unused)')
    # Compatibility-relevant runtime change (Migration & Rollback in
    # plan.md): the default moves from 4 to exactly 1 — this feature's
    # entire per-process TargetRegistry/BmcCoordinator concurrency model
    # assumes one worker. Any other explicit value is rejected before
    # startup so a manifest that forgets to override this default cannot
    # silently multiply per-target state across worker processes.
    parser.add_argument('--workers', type=int, dest='workers', default=1, help='Number of worker processes to run Uvicorn (must be 1)')

    args = parser.parse_args()
    if args.workers != 1:
        print(f"Error: --workers must be 1 (got {args.workers}); this exporter's per-process "
              "target registry/dispatcher requires exactly one worker.", file=sys.stderr)
        sys.exit(2)
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
