from setuptools import setup, find_packages, find_namespace_packages

# Single source of truth for the package version — both CI.yml and main.yml
# read it dynamically via `python3 setup.py --version` rather than holding
# their own copy, so `version=VERSION` below (not a second literal) is what
# actually prevents this constant from drifting out of sync with itself.
VERSION = "2.3.0"
PACKAGE_NAME = "redfish-exporter"

setup(
    name=PACKAGE_NAME,
    description="The Exporter for Physical Server Components Monitoring via RedFish API using FastAPI",
    version=VERSION,
    author="Freezing",
    url="https://github.com/svtechnmaa/Configurable_Redfish_Exporter.git",
    package_dir = {"": "src"},
    packages = find_packages(where="src"),
    include_package_data=True,
    zip_safe=False,
    license='MIT',
    classifiers=[
        'Development Status :: 2 - Beta',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP',
    ],
    # `setup.py`'s install_requires — not `requirements.txt` — is what
    # actually resolves dependencies for both the Docker build (`setup.py
    # sdist` + `pip install <tarball>`) and the shared test environment
    # (`pip install --editable .`); `requirements.txt` has no consumer in
    # this repository. Every entry below is therefore exact-pinned to the
    # version actually verified working in this checkout (full local
    # unit/in-process-HTTP suite passing), closing the reproducibility gap
    # that once let pip resolve an unpinned "aiohttp" down to an ancient,
    # syntactically-incompatible pre-async/await release.
    install_requires=[
        "prometheus-client==0.26.0",
        # 6.0.2, not the newer 6.0.3 (2025-09-25) — this codebase only ever
        # calls `yaml.safe_load`/`safe_dump` (unchanged across both), and
        # 6.0.2 is old/ubiquitous enough to already be mirrored everywhere,
        # unlike 6.0.3 which some package indexes/proxies may not have
        # synced yet (observed: Docker build failing with "Could not find a
        # version that satisfies the requirement pyyaml==6.0.3 ... from
        # versions: none" on the Alpine runtime stage).
        "pyyaml==6.0.2",
        # jsonpath-ng's `full_path` parenthesization behavior (relied on by
        # dataReconstruction.py's paren-stripping) is version-specific —
        # exact-pinned, not a floor.
        "jsonpath-ng==1.8.0",
        "Jinja2==3.1.6",
        "fastapi==0.141.1",
        "pydantic==2.13.5",
        "uvicorn==0.52.4",
        "starlette==1.6.0",
        "aiohttp==3.14.3",
        "python-dotenv==1.2.3"
    ],
    entry_points={
        'console_scripts': [
            'redfish-exporter = redfish_collector.main:main',
        ],
    }
)
