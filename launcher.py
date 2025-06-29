"""
ETL Agent Launcher - Startet alle Systemkomponenten
Vereinfachte Version ohne externe Dependencies
"""

import os
import sys
import subprocess
import signal
import time
import argparse
from typing import Dict, List, Optional
import socket
import psutil  # Für besseres Process-Management

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("etl_agent.launcher")


class ServiceManager:
    """Verwaltet alle ETL-Agent Services"""

    def __init__(self):
        self.services: Dict[str, subprocess.Popen] = {}
        self.service_configs = {
            "mcp_server": {
                "name": "MCP Server",
                "command": [
                    "uv",
                    "run",
                    "uvicorn",
                    "etl_agent.mcp_server:app",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8090",  # Geändert von 8080 auf 8090
                ],
                "port": 8090,
                "description": "Model Context Protocol Server",
            },
            "a2a_server": {
                "name": "A2A Server",
                "command": ["uv", "run", "python", "-m", "etl_agent.agent_to_a2a"],
                "port": 8091,  # Geändert von 8081 auf 8091
                "description": "Agent-to-Agent Communication Server",
            },
            "gradio_interface": {
                "name": "Gradio Web UI",
                "command": ["uv", "run", "python", "-m", "etl_agent.gradio_interface"],
                "port": 7860,
                "description": "Web Interface für ETL-Prozesse",
            },
            "scheduler": {
                "name": "ETL Scheduler",
                "command": ["uv", "run", "python", "-m", "etl_agent.scheduler_service"],
                "port": 8092,  # Geändert von 8082 auf 8092
                "description": "Job Scheduler für ETL-Pipelines",
            },
        }

    def is_port_available(self, port: int) -> bool:
        """Prüft ob Port verfügbar ist"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                result = sock.connect_ex(("localhost", port))
                available = result != 0
                if not available:
                    print(f"🔍 Port {port} ist belegt")
                return available
        except Exception as e:
            print(f"⚠️  Port-Check Fehler: {e}")
            return False

    def start_service(self, service_name: str) -> bool:
        """Startet einen einzelnen Service"""
        if service_name not in self.service_configs:
            print(f"❌ Unbekannter Service: {service_name}")
            return False

        if service_name in self.services:
            print(f"⚠️  Service {service_name} läuft bereits")
            return True

        config = self.service_configs[service_name]

        # Port-Verfügbarkeit prüfen
        if not self.is_port_available(config["port"]):
            print(f"❌ Port {config['port']} für {config['name']} ist bereits belegt")
            return False

        try:
            print(f"🚀 Starte {config['name']}...")

            process = subprocess.Popen(
                config["command"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Stderr zu Stdout umleiten
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            self.services[service_name] = process

            # Logs anzeigen
            if service_name == "gradio_interface":
                print(f"🔍 Gradio Logs werden in etl_agent_gradio.log geschrieben")
                print(
                    f"🔍 Verwenden Sie 'tail -f etl_agent_gradio.log' um Logs zu verfolgen"
                )

            # Kurz warten und prüfen ob Service läuft
            time.sleep(2)
            if process.poll() is None:
                print(
                    f"✅ {config['name']} gestartet (PID: {process.pid}, Port: {config['port']})"
                )
                return True
            else:
                print(f"❌ {config['name']} konnte nicht gestartet werden")
                stdout, stderr = process.communicate()
                if stderr:
                    print(f"Fehler: {stderr}")
                return False

        except Exception as e:
            print(f"❌ Fehler beim Starten von {config['name']}: {e}")
            return False

    def stop_service(self, service_name: str) -> bool:
        """Stoppt einen einzelnen Service"""
        if service_name not in self.services:
            print(f"⚠️  Service {service_name} läuft nicht")
            return True

        config = self.service_configs[service_name]
        process = self.services[service_name]

        try:
            print(f"🛑 Stoppe {config['name']}...")

            # Erweiterte Process-Beendigung
            if process.poll() is None:  # Process läuft noch
                # 1. Graceful shutdown versuchen
                process.terminate()

                # 2. Warten bis zu 5 Sekunden
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 3. Forceful kill
                    process.kill()
                    process.wait()

                # 4. Zusätzlich: Port-basierte Process-Cleanup
                self._kill_process_on_port(config["port"])

            del self.services[service_name]
            print(f"✅ {config['name']} gestoppt")
            return True

        except Exception as e:
            print(f"❌ Fehler beim Stoppen von {config['name']}: {e}")
            # Versuche trotzdem Port-Cleanup
            self._kill_process_on_port(config["port"])
            if service_name in self.services:
                del self.services[service_name]
            return False

    def _kill_process_on_port(self, port: int):
        """Tötet alle Prozesse die einen bestimmten Port verwenden"""
        try:
            for proc in psutil.process_iter():
                try:
                    # Direkte Verbindungsabfrage ohne attrs Parameter
                    connections = proc.connections()
                    for conn in connections:
                        if (
                            hasattr(conn, "laddr")
                            and conn.laddr
                            and conn.laddr.port == port
                        ):
                            print(f"🔪 Töte Process PID {proc.pid} auf Port {port}")
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except psutil.TimeoutExpired:
                                proc.kill()
                            return  # Erfolgreich beendet, raus aus der Funktion
                except (
                    psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess,
                ):
                    continue
                except Exception:
                    # Alle anderen Fehler ignorieren und weitermachen
                    continue
        except Exception as e:
            print(f"⚠️  Port-Cleanup Warnung: {e}")

    def cleanup_all_ports(self):
        """Bereinigt alle Service-Ports vor dem Start"""
        print("🧹 Bereinige alle Service-Ports...")
        for service_name, config in self.service_configs.items():
            port = config["port"]
            if not self.is_port_available(port):
                print(f"🔧 Port {port} wird bereinigt...")
                self._kill_process_on_port(port)
                # Warten damit Port freigegeben wird
                time.sleep(2)

                # Erneut prüfen ob Port jetzt frei ist
                if not self.is_port_available(port):
                    print(
                        f"⚠️  Port {port} konnte nicht freigegeben werden - verwende netstat:"
                    )
                    self._netstat_kill_port(port)

    def _netstat_kill_port(self, port: int):
        """Alternative Port-Cleanup mit netstat (Windows)"""
        try:
            import subprocess

            # Windows netstat command um Prozess auf Port zu finden
            result = subprocess.run(
                ["netstat", "-ano"], capture_output=True, text=True, timeout=10
            )

            for line in result.stdout.split("\n"):
                if f":{port}" in line and "LISTENING" in line:
                    parts = line.split()
                    if len(parts) >= 5:
                        pid = parts[-1]
                        try:
                            pid = int(pid)
                            print(
                                f"🔪 Töte Process PID {pid} auf Port {port} (netstat)"
                            )
                            subprocess.run(
                                ["taskkill", "/F", "/PID", str(pid)], timeout=5
                            )
                            time.sleep(1)
                            break
                        except (ValueError, subprocess.TimeoutExpired):
                            continue
        except Exception as e:
            print(f"⚠️  Netstat-Cleanup Fehler: {e}")

    def get_service_status(self):
        """Zeigt Status aller Services"""
        print("\n" + "=" * 60)
        print("ETL Agent Services Status")
        print("=" * 60)
        print(f"{'Service':<15} {'Name':<15} {'Status':<10} {'PID':<8} {'Port':<6}")
        print("-" * 60)

        for service_id, config in self.service_configs.items():
            if service_id in self.services:
                process = self.services[service_id]
                if process.poll() is None:
                    status = "🟢 Running"
                    pid = str(process.pid)
                else:
                    status = "🔴 Stopped"
                    pid = "N/A"
            else:
                status = "⚪ Not Started"
                pid = "N/A"

            print(
                f"{service_id:<15} {config['name']:<15} {status:<10} {pid:<8} {config['port']:<6}"
            )

    def stop_all_services(self):
        """Stoppt alle Services - auch ohne PID-Tracking"""
        print("🛑 Stoppe alle Services...")

        # 1. Versuche gespeicherte Services zu stoppen
        for service_name in list(self.services.keys()):
            self.stop_service(service_name)

        # 2. Zusätzlich: Alle Service-Ports force-cleanen
        print("🧹 Force-Cleanup aller Service-Ports...")
        for service_name, config in self.service_configs.items():
            port = config["port"]
            if not self.is_port_available(port):
                print(f"🔧 Force-Kill auf Port {port}...")
                self._kill_process_on_port(port)
                self._netstat_kill_port(port)
                time.sleep(1)

    def start_all_services(self):
        """Startet alle Services in der richtigen Reihenfolge"""
        print("🚀 Starte alle ETL-Agent Services...")

        # Cleanup vor dem Start
        self.cleanup_all_ports()

        # Reihenfolge wichtig: MCP Server zuerst, dann A2A, dann UI
        service_order = ["mcp_server", "a2a_server", "scheduler", "gradio_interface"]

        for service_name in service_order:
            success = self.start_service(service_name)
            if not success:
                print(
                    f"❌ Fehler beim Starten von {service_name}. Stoppe alle Services."
                )
                self.stop_all_services()
                return False

            # Kurze Pause zwischen Services
            time.sleep(1)

        print("\n🎉 Alle Services erfolgreich gestartet!")
        print("\n📱 Verfügbare Interfaces:")
        print("• Gradio Web UI: http://localhost:7860")
        print("• MCP Server: http://localhost:8090")
        print("• A2A Server: http://localhost:8091")
        print("• Scheduler API: http://localhost:8092")

        return True


# Global Service Manager
service_manager = ServiceManager()


def cleanup_handler(signum, frame):
    """Signal Handler für sauberes Beenden"""
    print("\n🛑 Shutdown Signal empfangen...")
    service_manager.stop_all_services()
    sys.exit(0)


# Signal Handler registrieren
signal.signal(signal.SIGINT, cleanup_handler)
signal.signal(signal.SIGTERM, cleanup_handler)


def main():
    """Hauptfunktion mit einfachem Argument Parsing"""
    parser = argparse.ArgumentParser(description="🚀 ETL Agent System Launcher")
    parser.add_argument(
        "action",
        choices=["start", "stop", "status", "restart"],
        help="Aktion",
    )
    parser.add_argument(
        "service",
        nargs="?",
        default="all",
        help="Service Name oder 'all'",
    )
    parser.add_argument(
        "--watch",
        "-w",
        action="store_true",
        help="Services überwachen",
    )

    args = parser.parse_args()

    print("🚀 ETL Agent System Launcher")
    print("Intelligente Datenverarbeitung mit PydanticAI, A2A und MCP")
    print("-" * 60)

    if args.action == "start":
        # Cleanup vor jedem Start
        service_manager.cleanup_all_ports()

        if args.service == "all":
            success = service_manager.start_all_services()
            if not success:
                sys.exit(1)
        else:
            success = service_manager.start_service(args.service)
            if not success:
                sys.exit(1)

        if args.watch:
            print("\n📊 Service Monitoring (Ctrl+C zum Beenden)")
            try:
                while True:
                    service_manager.get_service_status()
                    time.sleep(5)
                    print("\n" + "=" * 30 + " REFRESH " + "=" * 30)
            except KeyboardInterrupt:
                print("\nMonitoring beendet")

    elif args.action == "stop":
        if args.service == "all":
            service_manager.stop_all_services()
        else:
            service_manager.stop_service(args.service)

    elif args.action == "status":
        service_manager.get_service_status()

    elif args.action == "restart":
        if args.service == "all":
            print("🔄 Starte alle Services neu...")
            service_manager.stop_all_services()
            time.sleep(2)
            service_manager.start_all_services()
        else:
            print(f"🔄 Starte {args.service} neu...")
            service_manager.stop_service(args.service)
            time.sleep(1)
            service_manager.start_service(args.service)


if __name__ == "__main__":
    main()
