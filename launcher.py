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

    def _find_available_port(self, preferred_port: int, service_name: str) -> int:
        """Findet einen verfügbaren Port, beginnend mit dem bevorzugten Port"""
        for port_offset in range(10):  # Prüfe 10 Ports
            test_port = preferred_port + port_offset
            if not self._is_port_in_use(test_port):
                if port_offset > 0:
                    logger.info(
                        f"[INFO] Port {preferred_port} belegt, verwende Port {test_port} für {service_name}"
                    )
                return test_port

        logger.error(
            f"[ERROR] Keine verfügbaren Ports gefunden für {service_name} (getestet: {preferred_port}-{preferred_port + 9})"
        )
        return None

    def _kill_processes_on_ports(self, ports: list):
        """Beendet alle Prozesse auf den angegebenen Ports"""
        logger.info(f"[CLEANUP] Prüfe und beende Prozesse auf Ports: {ports}")

        try:
            import psutil

            killed_processes = []

            for port in ports:
                for proc in psutil.process_iter(["pid", "name", "connections"]):
                    try:
                        for conn in proc.info["connections"]:
                            if conn.laddr.port == port:
                                proc_name = proc.info["name"]
                                proc_pid = proc.info["pid"]
                                logger.info(
                                    f"[KILL] Beende Prozess {proc_name} (PID: {proc_pid}) auf Port {port}"
                                )
                                proc.terminate()
                                killed_processes.append((proc_name, proc_pid, port))
                                time.sleep(1)
                                if proc.is_running():
                                    proc.kill()
                    except (
                        psutil.NoSuchProcess,
                        psutil.AccessDenied,
                        psutil.ZombieProcess,
                    ):
                        continue

            if killed_processes:
                logger.info(f"[SUCCESS] {len(killed_processes)} Prozesse beendet")
            else:
                logger.info("[INFO] Keine Prozesse auf den Ports gefunden")

        except ImportError:
            logger.warning(
                "[WARNING] psutil nicht verfügbar, verwende netstat-Fallback"
            )
            self._kill_processes_on_ports_fallback(ports)
        except Exception as e:
            logger.error(f"[ERROR] Fehler beim Beenden der Prozesse: {e}")

    def _kill_processes_on_ports_fallback(self, ports: list):
        """Fallback-Methode ohne psutil"""
        for port in ports:
            try:
                # Windows netstat + taskkill
                result = subprocess.run(
                    f'netstat -ano | findstr ":{port}"',
                    shell=True,
                    capture_output=True,
                    text=True,
                )

                for line in result.stdout.split("\n"):
                    if f":{port}" in line and "LISTENING" in line:
                        parts = line.split()
                        if len(parts) > 4:
                            pid = parts[-1]
                            if pid.isdigit():
                                logger.info(
                                    f"[KILL] Beende Prozess PID {pid} auf Port {port}"
                                )
                                subprocess.run(f"taskkill /F /PID {pid}", shell=True)

            except Exception as e:
                logger.error(
                    f"[ERROR] Fallback-Kill für Port {port} fehlgeschlagen: {e}"
                )

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

            # Finde verfügbaren Port
            available_port = self._find_available_port(config["port"], service_name)
            if available_port is None:
                return False

            # Aktualisiere Command mit dem tatsächlichen Port
            command = config["command"].copy()

            # Ersetze Port in Command (für uvicorn Services)
            if any("--port" in str(arg) for arg in command):
                for i, arg in enumerate(command):
                    if arg == "--port" and i + 1 < len(command):
                        command[i + 1] = str(available_port)
                        break
            elif service_name == "gradio_interface":
                # Für Gradio Interface: Übergebe Port als Umgebungsvariable
                import os

                os.environ["ETL_GRADIO_PORT"] = str(available_port)

            process = subprocess.Popen(
                command,  # Verwende das aktualisierte Command
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Vereinige stderr mit stdout
                text=True,
                bufsize=0,  # Unbepuffert für sofortige Ausgabe
                universal_newlines=True,
            )

            # Für Gradio Interface: Starte Thread für Live-Ausgabe
            if service_name == "gradio_interface":
                import threading
                import os

                def stream_output():
                    """Stream die Ausgabe des Gradio-Prozesses live"""
                    try:
                        # Kontinuierlich Ausgabe lesen
                        while process.poll() is None:
                            try:
                                # Lese verfügbare Daten
                                if process.stdout and process.stdout.readable():
                                    line = process.stdout.readline()
                                    if line:
                                        line = line.strip()
                                        # Leite ALLE Enhanced ETL Logs an Konsole weiter
                                        if any(
                                            keyword in line
                                            for keyword in [
                                                "[ETL ENHANCED]",
                                                "[ETL DEBUG]",
                                                "[ETL ERROR]",
                                                "[ETL WARNING]",
                                            ]
                                        ):
                                            print(
                                                f"\033[92m[GRADIO LOG]\033[0m {line}",
                                                flush=True,
                                            )
                                        elif "CRITICAL AI FAILURE" in line:
                                            print(
                                                f"\033[91m[GRADIO CRITICAL]\033[0m {line}",
                                                flush=True,
                                            )
                                        elif "USER ACTION" in line:
                                            print(
                                                f"\033[94m[GRADIO ACTION]\033[0m {line}",
                                                flush=True,
                                            )
                                        elif any(
                                            keyword in line
                                            for keyword in [
                                                "Running on",
                                                "Gradio",
                                                "Interface",
                                            ]
                                        ):
                                            # Wichtige Gradio-Startmeldungen
                                            print(
                                                f"\033[96m[GRADIO]\033[0m {line}",
                                                flush=True,
                                            )
                                        else:
                                            # Normale Gradio-Ausgabe (weniger prominent)
                                            print(f"[GRADIO] {line}", flush=True)
                                    else:
                                        # Kurze Pause wenn keine Daten verfügbar
                                        time.sleep(0.1)
                                else:
                                    time.sleep(0.1)
                            except Exception as e:
                                logger.debug(f"Output streaming inner error: {e}")
                                time.sleep(0.1)
                    except Exception as e:
                        logger.debug(f"Output streaming error for {service_name}: {e}")

                # Starte den Output-Stream-Thread SOFORT
                output_thread = threading.Thread(target=stream_output, daemon=True)
                output_thread.start()
                logger.info(
                    f"[DEBUG] Output streaming thread started for {service_name}"
                )

            self.services[service_name] = process

            # Aktualisiere die tatsächlich verwendete Port-Nummer
            self.service_configs[service_name]["actual_port"] = available_port

            # Kurz warten und Prozess-Status prüfen
            if service_name == "gradio_interface":
                # Gradio braucht länger zum Starten - warte 5 Sekunden
                time.sleep(5)
            else:
                time.sleep(2)

            if process.poll() is None:
                logger.info(
                    f"[SUCCESS] {config['name']} gestartet (PID: {process.pid}, Port: {available_port})"
                )
                return True
            else:
                # Für Gradio: Stdout wurde bereits gestreamed, lese nur Rest
                if service_name == "gradio_interface":
                    remaining_output = process.stdout.read() if process.stdout else ""
                    logger.error(
                        f"[ERROR] {config['name']} konnte nicht gestartet werden"
                    )
                    if remaining_output:
                        logger.error(f"Output: {remaining_output}")
                else:
                    stdout, stderr = process.communicate()
                    logger.error(
                        f"[ERROR] {config['name']} konnte nicht gestartet werden"
                    )
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

        # Schritt 1: Cleanup - Beende alle Prozesse auf unseren Ports
        required_ports = [config["port"] for config in self.service_configs.values()]
        self._kill_processes_on_ports(required_ports)

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

                # Für alle Services: Konsistente PID-basierte Erkennung
                is_running = process.poll() is None
                pid = process.pid if is_running else None

                status[service_name] = {
                    "name": config["name"],
                    "running": is_running,
                    "pid": pid,
                    "port": config.get(
                        "actual_port", config["port"]
                    ),  # Verwende tatsächlichen Port
                    "description": config["description"],
                }
            else:
                status[service_name] = {
                    "name": config["name"],
                    "running": False,
                    "pid": None,
                    "port": config.get(
                        "actual_port", config["port"]
                    ),  # Verwende tatsächlichen Port
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
                s.settimeout(1)  # 1 Sekunde Timeout
                result = s.connect_ex(("127.0.0.1", port))
                return result == 0
        except Exception as e:
            logger.debug(f"Port check error for {port}: {e}")
            return False

    def monitor_services(self):
        """Überwacht Services und startet sie bei Bedarf neu"""
        logger.info("[MONITOR] Service-Monitoring gestartet...")

        try:
            while True:
                for service_name in list(self.services.keys()):
                    process = self.services[service_name]

                    # Spezielle Behandlung für Gradio Interface
                    if service_name == "gradio_interface":
                        # Für Gradio: Nur prüfen ob der Hauptprozess läuft
                        # Port-Check ist bei Gradio nicht zuverlässig, da es länger dauert bis der Server bereit ist
                        main_process_running = process.poll() is None

                        if main_process_running:
                            # Hauptprozess läuft - das reicht für Gradio
                            continue
                        else:
                            # Hauptprozess ist tatsächlich gestoppt
                            logger.warning(
                                f"[WARNING] Gradio Interface unerwartet gestoppt (PID {process.pid} nicht mehr aktiv)"
                            )
                            del self.services[service_name]

                            # Automatischer Neustart
                            logger.info("[RESTART] Starte Gradio Interface neu...")
                            self.start_service(service_name)
                    else:
                        # Für alle anderen Services: Direkte PID-basierte Prüfung
                        if process.poll() is not None:
                            # Prozess ist gestoppt
                            logger.warning(
                                f"[WARNING] Service {service_name} unerwartet gestoppt (PID {process.pid} nicht mehr aktiv)"
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
        level=logging.INFO,  # Zurück zu INFO-Level
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
