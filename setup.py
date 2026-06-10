from setuptools import setup, find_packages

setup(
    name="fleet-daemon",
    version="0.1.0",
    description="Real-time MQTT agent daemon for the SuperInstance fleet C2 matrix",
    packages=find_packages(),
    install_requires=[
        "paho-mqtt>=1.6",
        "pyyaml>=5.1",
    ],
    entry_points={
        "console_scripts": [
            "fleet-daemon=fleet_daemon.daemon:main",
        ],
    },
    python_requires=">=3.10",
)
