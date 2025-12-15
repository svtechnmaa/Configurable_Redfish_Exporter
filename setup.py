from setuptools import setup, find_packages, find_namespace_packages

VERSION = "2.2.1"
PACKAGE_NAME = "redfish-exporter"

setup(
    name=PACKAGE_NAME,
    description="The Exporter for Physical Server Components Monitoring via RedFish API using FastAPI",
    version="2.2.1",
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
    install_requires=[
        "prometheus-client",
        "pyyaml",
        "requests",
        "jsonpath-ng",
        "Jinja2",
        "fastapi",
        "pydantic",
        "uvicorn",
        "starlette",
        "aiohttp",
        "python-dotenv"
    ],
    entry_points={
        'console_scripts': [
            'redfish-exporter = redfish_collector.main:main',
        ],
    }
)
