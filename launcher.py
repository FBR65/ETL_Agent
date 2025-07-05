"""
Launcher für ETL-Agen                "command":                 "command": [
                    "uv",
                    "run",
                    "uvicorn",
                    "etl_agent.agent_to_a2a:app",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8091",
                    "--reload",
                ],            "uv",
                    "run",
                    "uvicorn",
                    "etl_agent.mcp_server:app",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8090",
                    "--reload",
                ],- Windows-kompatible Version
Startet alle Services mit verbesserter Konfiguration und Windows-Unterstützung
"""

import subprocess
import time
import sys
import signal
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class ServiceManager:
    """
    Verwaltet alle ETL-Agent Services
    Windows-kompatible Implementierung ohne Unicode-Probleme
    """

    def __init__(self):
        self.services: Dict[str, subprocess.Popen] = {}
        self.service_configs = {
            "mcp_server": {
                "name": "MCP Server (Enhanced)",
                "command": [
                    "uv",
                    "run",
                    "uvicorn",
                    "etl_agent.mcp_server:app",  # Originaler Name nach Umbenennung
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8090",
                    "--reload",
                ],
                "port": 8090,
                "description": "Model Context Protocol Server mit erweiterten Tools",
                "health_endpoint": "/health",
            },
            "a2a_server": {
                "name": "A2A Server (Enhanced)",
                "command": [
                    "uv",
                    "run",
                    "uvicorn",
                    "etl_agent.agent_to_a2a:app",  # Originaler Name nach Umbenennung
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8091",
                    "--reload",
                ],
                "port": 8091,
                "description": "Agent-to-Agent Communication mit PydanticAI Integration",
                "health_endpoint": "/health",
            },
            "gradio_interface": {
                "name": "Gradio Web UI (Enhanced)",
                "command": [
                    "uv",
                    "run",
                    "python",
                    "-m",
                    "etl_agent.gradio_interface",  # Originaler Name nach Umbenennung
                ],
                "port": 7860,
                "description": "Verbesserte Web-UI mit erweiterten Features",
                "health_endpoint": None,
            },
        }

    def start_service(self, service_name: str) -> bool:
        """Startet einen einzelnen Service mit Windows-kompatibler Ausgabe"""
        if service_name not in self.service_configs:
            logger.error(f"[ERROR] Unbekannter Service: {service_name}")
            return False

        if service_name in self.services:
            logger.warning(f"[WARNING] Service {service_name} läuft bereits")
            return True

        config = self.service_configs[service_name]

        try:
            logger.info(f"[START] Starte {config['name']}...")

            # Port-Verfügbarkeit prüfen
            if self._is_port_in_use(config["port"]):
                logger.warning(
                    f"[WARNING] Port {config['port']} bereits belegt für {service_name}"
                )
                return False

            process = subprocess.Popen(
                config["command"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            self.services[service_name] = process

            # Kurz warten und Prozess-Status prüfen
            time.sleep(2)

            if process.poll() is None:
                logger.info(
                    f"[SUCCESS] {config['name']} gestartet (PID: {process.pid}, Port: {config['port']})"
                )
                return True
            else:
                stdout, stderr = process.communicate()
                logger.error(f"[ERROR] {config['name']} konnte nicht gestartet werden")
                logger.error(f"STDOUT: {stdout}")
                logger.error(f"STDERR: {stderr}")
                del self.services[service_name]
                return False

        except Exception as e:
            logger.error(f"[ERROR] Fehler beim Starten von {service_name}: {e}")
            if service_name in self.services:
                del self.services[service_name]
            return False

    def stop_service(self, service_name: str) -> bool:
        """Stoppt einen Service mit Windows-kompatibler Ausgabe"""
        if service_name not in self.services:
            logger.warning(f"[WARNING] Service {service_name} läuft nicht")
            return True

        try:
            process = self.services[service_name]
            config = self.service_configs[service_name]

            logger.info(f"[STOP] Stoppe {config['name']}...")

            # Graceful shutdown versuchen
            process.terminate()

            # Warten auf graceful shutdown
            try:
                process.wait(timeout=10)
                logger.info(f"[SUCCESS] {config['name']} ordnungsgemäß gestoppt")
            except subprocess.TimeoutExpired:
                # Force kill wenn graceful shutdown fehlschlägt
                logger.warning(f"[WARNING] Force-Kill für {config['name']}")
                process.kill()
                process.wait()

            del self.services[service_name]
            return True

        except Exception as e:
            logger.error(f"[ERROR] Fehler beim Stoppen von {service_name}: {e}")
            return False

    def start_all_services(self) -> bool:
        """Startet alle Services in der richtigen Reihenfolge"""
        logger.info("[START] Starte alle ETL-Agent Services...")

        # Reihenfolge für Abhängigkeiten
        service_order = ["mcp_server", "a2a_server", "gradio_interface"]

        success_count = 0
        total_services = len(service_order)

        for service_name in service_order:
            if self.start_service(service_name):
                success_count += 1
                # Kurze Pause zwischen Services
                time.sleep(3)
            else:
                logger.error(f"[ERROR] Fehler beim Starten von {service_name}")

        logger.info(f"[STATUS] Services gestartet: {success_count}/{total_services}")

        if success_count == total_services:
            self._display_service_status()
            return True
        else:
            logger.warning("[WARNING] Nicht alle Services konnten gestartet werden")
            return False

    def stop_all_services(self):
        """Stoppt alle Services"""
        logger.info("[STOP] Stoppe alle Services...")

        for service_name in list(self.services.keys()):
            self.stop_service(service_name)

        logger.info("[SUCCESS] Alle Services gestoppt")

    def get_service_status(self) -> Dict[str, Dict]:
        """Gibt Status aller Services zurück"""
        status = {}

        for service_name, config in self.service_configs.items():
            if service_name in self.services:
                process = self.services[service_name]
                is_running = process.poll() is None
                pid = process.pid if is_running else None

                status[service_name] = {
                    "name": config["name"],
                    "running": is_running,
                    "pid": pid,
                    "port": config["port"],
                    "description": config["description"],
                }
            else:
                status[service_name] = {
                    "name": config["name"],
                    "running": False,
                    "pid": None,
                    "port": config["port"],
                    "description": config["description"],
                }

        return status

    def _display_service_status(self):
        """Zeigt Service-Status in der Konsole (Windows-kompatibel)"""
        print("\n" + "=" * 80)
        print("ETL AGENT - SERVICE STATUS")
        print("=" * 80)

        status = self.get_service_status()

        for service_name, info in status.items():
            status_icon = "[OK]" if info["running"] else "[ERROR]"
            pid_info = f"(PID: {info['pid']})" if info["pid"] else ""

            print(f"{status_icon} {info['name']}")
            print(f"   Port: {info['port']} {pid_info}")
            print(f"   Beschreibung: {info['description']}")

            if info["running"] and service_name != "gradio_interface":
                print(f"   URL: http://localhost:{info['port']}")
            elif info["running"] and service_name == "gradio_interface":
                print(f"   URL: http://localhost:{info['port']}")
            print()

        print("ZUGRIFF AUF DIE SERVICES:")
        print("   Gradio Web UI: http://localhost:7860")
        print("   MCP Server:    http://localhost:8090")
        print("   A2A Server:    http://localhost:8091")
        print("\n   Logs werden in Echtzeit angezeigt...")
        print("   Drücken Sie Ctrl+C zum Beenden")
        print("=" * 80 + "\n")

    def _is_port_in_use(self, port: int) -> bool:
        """Prüft ob ein Port bereits verwendet wird"""
        try:
            import socket

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                return s.connect_ex(("localhost", port)) == 0
        except Exception:
            return False

    def monitor_services(self):
        """Überwacht Services und startet sie bei Bedarf neu"""
        logger.info("[MONITOR] Service-Monitoring gestartet...")

        try:
            while True:
                for service_name in list(self.services.keys()):
                    process = self.services[service_name]

                    if process.poll() is not None:
                        # Service ist gestoppt
                        logger.warning(
                            f"[WARNING] Service {service_name} unerwartet gestoppt"
                        )
                        del self.services[service_name]

                        # Automatischer Neustart
                        logger.info(f"[RESTART] Starte {service_name} neu...")
                        self.start_service(service_name)

                time.sleep(30)  # Alle 30 Sekunden prüfen

        except KeyboardInterrupt:
            logger.info("[STOP] Service-Monitoring gestoppt")

    def handle_shutdown(self, signum, frame):
        """Behandelt Shutdown-Signale"""
        logger.info("[SHUTDOWN] Shutdown-Signal empfangen")
        self.stop_all_services()
        sys.exit(0)


def main():
    """Hauptfunktion zum Starten aller Services"""
    # Logging konfigurieren - Windows-kompatibel ohne UTF-8 Emojis
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("etl_agent_launcher.log", encoding="utf-8"),
        ],
    )

    # Service Manager erstellen
    service_manager = ServiceManager()

    # Signal Handler für graceful shutdown
    signal.signal(signal.SIGINT, service_manager.handle_shutdown)
    signal.signal(signal.SIGTERM, service_manager.handle_shutdown)

    try:
        # Alle Services starten
        if service_manager.start_all_services():
            logger.info("[SUCCESS] Alle Services erfolgreich gestartet")

            # Service-Monitoring starten
            service_manager.monitor_services()
        else:
            logger.error("[ERROR] Nicht alle Services konnten gestartet werden")
            service_manager.stop_all_services()
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Beende ETL Agent...")
        service_manager.stop_all_services()
    except Exception as e:
        logger.error(f"[ERROR] Unerwarteter Fehler: {e}")
        service_manager.stop_all_services()
        sys.exit(1)


if __name__ == "__main__":
    main()