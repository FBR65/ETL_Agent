"""
Gradio Web Interface für ETL-Agent - CLEAN & ASYNC Implementation
✅ Löst Verbindungstest-Hängen mit asynchroner Ausführung (asyncio.to_thread)
✅ Löst fehlende Verbindungsanzeige mit korrekter Persistierung
✅ Optimiert für PydanticAI und stabiles User Experience
"""

import gradio as gr
import asyncio
import logging
import time
import os
import json
from typing import List, Tuple

from .etl_agent_core import ETLAgent, ETLRequest
from .database_manager import DatabaseManager
from .scheduler import ETLScheduler

# Logging für Gradio konfigurieren
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_agent_gradio.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


class ETLGradioInterface:
    """
    Gradio Web Interface für ETL-Agent - Asynchrone Implementierung
    """

    _shared_db_manager = None

    def __init__(self, db_manager=None):
        if db_manager:
            self.db_manager = db_manager
            ETLGradioInterface._shared_db_manager = db_manager
        elif ETLGradioInterface._shared_db_manager:
            self.db_manager = ETLGradioInterface._shared_db_manager
        else:
            self.db_manager = DatabaseManager()
            ETLGradioInterface._shared_db_manager = self.db_manager

        self.etl_agent = ETLAgent()
        self.etl_agent.db_manager = self.db_manager
        self.scheduler = ETLScheduler()

        logger.info(
            f"ETL Gradio Interface initialisiert - DB Manager geteilt (Verbindungen: {len(self.db_manager.connection_configs)})"
        )

    def create_interface(self) -> gr.Blocks:
        """Erstellt das vollständige Gradio Interface."""
        with gr.Blocks(
            title="ETL Agent - Intelligente Datenverarbeitung",
            theme=gr.themes.Soft(),
        ) as interface:
            gr.Markdown("# 🚀 ETL Agent - Intelligente Datenverarbeitung")
            gr.Markdown(
                """
                **KI-basierte ETL-Code-Generierung mit PydanticAI**  
                Beschreiben Sie Ihren ETL-Prozess in natürlicher Sprache und lassen Sie den Agenten Python-Code generieren.
                """
            )

            with gr.Tabs():
                with gr.Tab("🔗 Datenbankverbindungen", id="db_tab"):
                    self._create_database_tab()
                with gr.Tab("⚙️ ETL-Prozess Designer", id="etl_tab"):
                    self._create_etl_tab()
                with gr.Tab("⏰ Job-Scheduler", id="scheduler_tab"):
                    self._create_scheduler_tab()
                with gr.Tab("📊 Monitoring", id="monitoring_tab"):
                    self._create_monitoring_tab()
                with gr.Tab("🤖 Agent-Status", id="agent_tab"):
                    self._create_agent_status_tab()

        return interface

    def _create_etl_tab(self):
        """Erstellt den 'ETL-Prozess Designer' Tab."""
        gr.Markdown("## 🎯 ETL-Prozess in natürlicher Sprache beschreiben")

        with gr.Row():
            with gr.Column(scale=2):
                gr.Markdown("### 📝 ETL-Beschreibung")
                etl_description = gr.Textbox(
                    label="ETL-Prozess Beschreibung",
                    placeholder="Beispiel: 'Lade alle Kunden aus der MongoDB, filtere aktive Kunden, füge Altersberechnung hinzu und speichere als CSV'",
                    lines=6,
                )
                with gr.Row():
                    generate_btn = gr.Button(
                        "🤖 ETL-Code generieren", variant="primary"
                    )
                    clear_btn = gr.Button("🗑️ Leeren")

            with gr.Column(scale=3):
                gr.Markdown("### 💻 Generierter ETL-Code")
                generated_code = gr.Code(
                    label="ETL Pipeline Code", language="python", lines=25
                )
                execution_log = gr.Textbox(
                    label="🔍 Generierungslog & Status", lines=6, interactive=False
                )

        with gr.Accordion(
            "🎯 ETL-Konfiguration (Für beste Ergebnisse ausfüllen!)", open=True
        ):
            with gr.Row():
                with gr.Column():
                    source_conn = gr.Dropdown(
                        choices=[],  # Leer starten
                        label="📊 Quell-Datenbank",
                        info="Wichtig für Schema-Erkennung und spezifischen Code!",
                        interactive=True,
                    )
                    transformation_hints = gr.CheckboxGroup(
                        choices=[
                            "Data Filtering",
                            "Table Joins",
                            "Data Aggregation",
                            "Date Transformations",
                        ],
                        label="🔧 Gewünschte Transformationen",
                    )
                with gr.Column():
                    target_conn = gr.Dropdown(
                        choices=[],  # Leer starten
                        label="💾 Ziel-Datenbank (optional)",
                        interactive=True,
                    )
                    output_format = gr.Radio(
                        choices=["Auto", "CSV", "Excel", "Database", "JSON"],
                        value="Auto",
                        label="Ausgabeformat",
                    )
            refresh_conn_btn = gr.Button("🔄 Verbindungen aktualisieren", size="sm")

        # Event-Handler für ETL-Tab
        generate_btn.click(
            fn=self.generate_etl_code_async,
            inputs=[
                etl_description,
                source_conn,
                target_conn,
                transformation_hints,
                output_format,
            ],
            outputs=[generated_code, execution_log],
        )
        refresh_conn_btn.click(
            fn=self._refresh_connection_choices_etl_only,
            outputs=[source_conn, target_conn],
        )
        clear_btn.click(fn=lambda: ("", ""), outputs=[etl_description, execution_log])

        # FRESH CONNECTIONS: Dropdown-Klicks laden automatisch frische Verbindungen
        source_conn.focus(
            fn=lambda: gr.update(choices=self._get_fresh_connections()),
            outputs=[source_conn],
        )
        target_conn.focus(
            fn=lambda: gr.update(choices=self._get_fresh_connections()),
            outputs=[target_conn],
        )

    def _create_database_tab(self):
        """Erstellt den 'Datenbankverbindungen' Tab."""
        gr.Markdown("## 🔗 Datenbankverbindungen verwalten")

        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### ➕ Neue Verbindung hinzufügen")
                conn_name = gr.Textbox(
                    label="Verbindungsname", placeholder="z.B. 'main_postgres_db'"
                )
                db_type = gr.Dropdown(
                    choices=[
                        "postgresql",
                        "mysql",
                        "mariadb",
                        "mongodb",
                        "sqlite",
                        "sqlserver",
                    ],
                    label="Datenbanktyp",
                    value="postgresql",
                )
                conn_string = gr.Textbox(
                    label="Connection String",
                    lines=2,
                    placeholder="postgresql://user:password@localhost:5432/database",
                )
                with gr.Row():
                    add_btn = gr.Button("💾 SOFORT Speichern", variant="primary")
                    test_btn = gr.Button("🔍 Verbindung Testen")
                connection_status = gr.Textbox(label="Status", interactive=False)

            with gr.Column(scale=2):
                gr.Markdown("### 📋 Bestehende Verbindungen")
                connections_list = gr.DataFrame(
                    headers=["Name", "Typ", "Details"],
                    label="Verbindungen",
                    interactive=False,
                    value=self._get_connections_for_display_sync(),
                )
                with gr.Row():
                    refresh_btn = gr.Button("🔄 Aktualisieren")
                    delete_conn_name = gr.Dropdown(
                        choices=[],  # Leer starten
                        label="Zu löschende Verbindung",
                    )
                    delete_btn = gr.Button("🗑️ Löschen", variant="stop")

        # Einfache Event-Handler ohne Cross-Tab-Updates
        add_btn.click(
            fn=self.add_connection_simple,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status],
        ).then(
            fn=self._refresh_connections_display_full,
            outputs=[connections_list, delete_conn_name],
        )
        test_btn.click(
            fn=self.test_connection_async,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status],
        )
        refresh_btn.click(
            fn=self._refresh_connections_display_full,
            outputs=[connections_list, delete_conn_name],
        )
        delete_btn.click(
            fn=self.delete_connection_async,
            inputs=[delete_conn_name],
            outputs=[connection_status, connections_list, delete_conn_name],
        )

        # FRESH CONNECTIONS: Delete-Dropdown lädt automatisch frische Verbindungen
        delete_conn_name.focus(
            fn=lambda: gr.update(choices=self._get_fresh_connections()),
            outputs=[delete_conn_name],
        )

    def _create_scheduler_tab(self):
        """Erstellt den 'Job-Scheduler' Tab."""
        gr.Markdown("## ⏰ ETL-Job Scheduler (zukünftige Funktion)")

    def _create_monitoring_tab(self):
        """Erstellt den 'Monitoring' Tab."""
        gr.Markdown("## 📊 ETL-Monitoring & Statistiken")
        gr.JSON(
            label="System Status",
            value={
                "etl_agent": "Bereit",
                "database_manager": "Bereit",
                "connections": len(self.db_manager.connection_configs),
            },
        )

    def _create_agent_status_tab(self):
        """Erstellt den 'Agent-Status' Tab."""
        gr.Markdown("## 🤖 AI-Agent Status & Konfiguration")
        gr.JSON(
            label="Large Language Model",
            value={
                "model": self.etl_agent.llm_model_name,
                "endpoint": self.etl_agent.llm_endpoint,
                "provider": "OpenAI-kompatibel",
            },
        )
        test_llm_btn = gr.Button("🧪 LLM-Verbindung testen")
        llm_test_result = gr.Textbox(label="LLM Test Ergebnis", interactive=False)
        test_llm_btn.click(fn=self.test_llm_connection_async, outputs=[llm_test_result])

    # --- Asynchrone Event-Handler ---

    async def add_connection_async(
        self,
        name: str,
        db_type: str,
        conn_string: str,
        progress=gr.Progress(track_tqdm=True),
    ) -> Tuple[str, List, gr.Dropdown]:
        """Fügt eine Verbindung asynchron hinzu - OHNE Test für maximale Geschwindigkeit"""
        if not name or not conn_string:
            msg = "❌ Name und Connection String sind erforderlich."
            return (
                msg,
                self._get_connections_for_display(),
                gr.Dropdown(choices=self._get_connection_choices()),
            )

        # Sichere Prüfung auf bestehende Verbindungen
        try:
            progress(0.2, desc="Prüfe auf doppelte Namen...")
            connection_exists = await asyncio.wait_for(
                asyncio.to_thread(self.db_manager.connection_exists, name), timeout=1.0
            )

            if connection_exists:
                msg = f"❌ Verbindung '{name}' existiert bereits."
                return (
                    msg,
                    self._get_connections_for_display(),
                    gr.Dropdown(choices=self._get_connection_choices()),
                )

            progress(0.5, desc="Speichere Verbindung...")
            logger.info(
                f"Speichere Verbindung '{name}' OHNE Verbindungstest für maximale Geschwindigkeit"
            )

            config = {"connection_string": conn_string, "type": db_type}

            # Direktes Speichern ohne Test
            add_result = await asyncio.wait_for(
                asyncio.to_thread(self.db_manager.add_connection, name, config),
                timeout=3.0,
            )

            if not add_result:
                msg = f"❌ Speichern der Verbindung '{name}' fehlgeschlagen."
                logger.error(msg)
                return (
                    msg,
                    self._get_connections_for_display(),
                    gr.Dropdown(choices=self._get_connection_choices()),
                )

            progress(1.0, desc="Fertig!")
            status_msg = f"✅ Verbindung '{name}' erfolgreich gespeichert. Nutzen Sie 'Nur Testen' um die Verbindung zu prüfen."
            logger.info(status_msg)

            return (
                status_msg,
                self._get_connections_for_display(),
                gr.Dropdown(choices=self._get_connection_choices()),
            )

        except asyncio.TimeoutError:
            msg = "❌ Timeout beim Speichern der Verbindung."
            logger.warning(f"Speichern von '{name}' fehlgeschlagen wegen Timeout.")
            return (
                msg,
                self._get_connections_for_display(),
                gr.Dropdown(choices=self._get_connection_choices()),
            )
        except Exception as e:
            msg = f"❌ Unerwarteter Fehler: {e}"
            logger.error(
                f"Unerwarteter Fehler beim Hinzufügen von '{name}': {e}", exc_info=True
            )
            return (
                msg,
                self._get_connections_for_display(),
                gr.Dropdown(choices=self._get_connection_choices()),
            )

    async def test_connection_async(
        self,
        name: str,
        db_type: str,
        conn_string: str,
        progress=gr.Progress(track_tqdm=True),
    ) -> str:
        """Testet eine Verbindung asynchron, ohne die UI zu blockieren."""
        if not conn_string:
            return "❌ Connection String ist erforderlich"

        progress(0.1, desc="Starte Verbindungstest...")
        logger.info(f"Starte asynchronen Verbindungstest für '{name}'")

        try:
            test_result = await asyncio.wait_for(
                asyncio.to_thread(
                    self.db_manager.test_connection, name, db_type, conn_string
                ),
                timeout=5.0,  # Konsistenter 5-Sekunden-Timeout
            )
            logger.info(
                f"Asynchroner Test für '{name}' abgeschlossen: {test_result['status']}"
            )
            return test_result["message"]
        except asyncio.TimeoutError:
            logger.warning(
                f"Verbindungstest für '{name}' hat Timeout (5s) überschritten."
            )
            return "❌ Timeout nach 5 Sekunden. Der Datenbankserver antwortet nicht oder ist sehr langsam."
        except Exception as e:
            logger.error(
                f"Fehler beim asynchronen Verbindungstest für '{name}': {e}",
                exc_info=True,
            )
            return f"❌ Unerwarteter Fehler: {str(e)}"

    async def delete_connection_async(self, name: str) -> Tuple[str, List, gr.Dropdown]:
        """Löscht eine Verbindung asynchron."""
        if not name:
            msg = "⚠️ Bitte wählen Sie eine Verbindung zum Löschen aus."
            return (
                msg,
                self._get_connections_for_display_sync(),
                gr.Dropdown(choices=self._get_connection_choices_sync(), value=None),
            )

        logger.info(f"Versuche, Verbindung '{name}' zu löschen.")
        try:
            success = await asyncio.to_thread(self.db_manager.remove_connection, name)
            if success:
                status_msg = f"✅ Verbindung '{name}' erfolgreich gelöscht."
            else:
                status_msg = f"❌ Löschen von '{name}' fehlgeschlagen. Siehe Logs."

            # Nach dem Löschen: Neue Choice-Liste holen und Dropdown zurücksetzen
            new_choices = self._get_connection_choices_sync()
            return (
                status_msg,
                self._get_connections_for_display_sync(),
                gr.Dropdown(choices=new_choices, value=None),
            )
        except Exception as e:
            msg = f"❌ Fehler beim Löschen: {e}"
            logger.error(f"Fehler beim Löschen von '{name}': {e}", exc_info=True)
            return (
                msg,
                self._get_connections_for_display_sync(),
                gr.Dropdown(choices=self._get_connection_choices_sync(), value=None),
            )

    async def generate_etl_code_async(
        self,
        description: str,
        source_conn: str,
        target_conn: str,
        transformation_hints: List[str],
        output_format: str,
        progress=gr.Progress(track_tqdm=True),
    ) -> Tuple[str, str]:
        """Generiert ETL-Code asynchron mit einem robusten Timeout."""
        start_time = time.time()
        if not description.strip():
            return ("", "❌ Bitte geben Sie eine ETL-Beschreibung ein.")

        progress(0.1, desc="Analysiere Anfrage...")
        logger.info(f"Generiere ETL-Code für: {description}")

        try:
            available_connections = self.db_manager.list_connections()
            etl_request = ETLRequest(
                description=description,
                source_config={"connection_name": source_conn} if source_conn else None,
                target_config={"connection_name": target_conn} if target_conn else None,
                transformation_rules=transformation_hints,
                metadata={
                    "output_format": output_format,
                    "available_connections": [str(c) for c in available_connections],
                },
            )

            progress(0.4, desc="Kontaktiere AI-Agent...")
            result = await asyncio.wait_for(
                self.etl_agent.process_etl_request(etl_request), timeout=45.0
            )

            elapsed = time.time() - start_time
            logger.info(f"ETL-Code-Generierung abgeschlossen in {elapsed:.1f}s")

            if result.status == "success":
                log_message = (
                    f"✅ ETL-Code erfolgreich generiert! (Dauer: {elapsed:.1f}s)"
                )
                return (result.generated_code, log_message)
            else:
                error_details = f"❌ AI-Agent Fehler: {result.error_message}"
                logger.error(f"ETL-Agent-Error: {result.error_message}")
                return ("", error_details)

        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            msg = f"❌ Timeout nach {elapsed:.1f}s. Der AI-Agent antwortet nicht. Prüfen Sie den Ollama-Server."
            logger.error(msg)
            return ("", msg)
        except Exception as e:
            elapsed = time.time() - start_time
            msg = f"❌ Unerwarteter Fehler nach {elapsed:.1f}s: {e}"
            logger.error(msg, exc_info=True)
            return ("", msg)

    async def test_llm_connection_async(self) -> str:
        """Testet die LLM-Verbindung asynchron."""
        logger.info("Teste LLM-Verbindung...")
        try:
            test_prompt = "Antworte nur mit 'OK'."

            response = await asyncio.wait_for(
                self.etl_agent.agent.run(test_prompt), timeout=15.0
            )

            msg = f"✅ LLM-Verbindung erfolgreich\n- Modell: {self.etl_agent.llm_model_name}\n- Antwort: {response}"
            logger.info(msg)
            return msg

        except asyncio.TimeoutError:
            msg = f"❌ LLM-Verbindung Timeout (15s).\n- Endpoint: {self.etl_agent.llm_endpoint}\n- Prüfen Sie, ob der LLM-Server (Ollama) läuft."
            logger.error(msg)
            return msg
        except Exception as e:
            msg = f"❌ LLM-Verbindung fehlgeschlagen.\n- Fehler: {e}"
            logger.error(msg, exc_info=True)
            return msg

    # --- Hilfsfunktionen für die UI ---

    def _get_connections_for_display(self) -> List[List[str]]:
        """Holt alle Verbindungen für die Anzeige in der DataFrame - sichere Implementierung."""
        try:
            # Verwende ausschließlich die sichere list_connections() Methode
            connections = self.db_manager.list_connections()
            display_list = []

            for conn_info in connections:
                conn_name = conn_info.get("name", "Unbekannt")
                db_type = conn_info.get("type", "Unbekannt")
                conn_string = conn_info.get("connection_string", "")

                # Anonymisiere Passwort im Connection String für die Anzeige
                if "@" in conn_string:
                    parts = conn_string.split("@")
                    if len(parts) > 1:
                        host_part = parts[-1]
                        protocol_user = parts[0].split(":")
                        if len(protocol_user) > 1:
                            protocol = protocol_user[0]
                            user = protocol_user[1].lstrip("/")
                            conn_string = f"{protocol}://{user}:***@{host_part}"

                display_list.append([conn_name, db_type.upper(), conn_string])

            if not display_list:
                return [
                    [
                        "Keine Verbindungen",
                        "N/A",
                        "Bitte fügen Sie eine neue Verbindung hinzu.",
                    ]
                ]
            return display_list
        except Exception as e:
            logger.error(f"Fehler beim Laden der Verbindungen für die Anzeige: {e}")
            return [["Fehler", "N/A", str(e)]]

    def _get_connection_choices(self) -> List[str]:
        """Holt die Namen aller Verbindungen für Dropdowns."""
        try:
            connections = self.db_manager.list_connections()
            if not connections:
                return ["Keine Verbindungen konfiguriert"]
            return [conn.get("name") for conn in connections if conn.get("name")]
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Verbindungs-Auswahl: {e}")
            return ["Fehler beim Laden"]

    def _refresh_connections_display_full(self) -> Tuple[List[List[str]], List]:
        """ULTRA-SCHNELL - Liest direkt aus JSON ohne jegliche Verarbeitung."""
        logger.info("DEBUG: _refresh_connections_display_full - direkte JSON-Lese")

        try:
            # Direkter JSON-Zugriff - KEINE Hilfsfunktionen
            json_file = "db_connections.json"

            if not os.path.exists(json_file):
                logger.warning("DEBUG: JSON-Datei nicht gefunden")
                return [["Keine Verbindungen", "N/A", "JSON nicht gefunden"]], [
                    "Keine Verbindungen"
                ]

            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            display_list = []
            choice_list = []

            # Minimale Verarbeitung - nur das Nötigste
            for name, config in data.items():
                db_type = config.get("type", "unknown").upper()
                conn_str = config.get("connection_string", "")

                # Einfache Passwort-Maskierung
                if "://" in conn_str and "@" in conn_str:
                    try:
                        protocol = conn_str.split("://")[0]
                        rest = conn_str.split("://")[1]
                        if "@" in rest:
                            host_part = rest.split("@")[1]
                            conn_str = f"{protocol}://***@{host_part}"
                    except Exception:
                        conn_str = "***"

                display_list.append([name, db_type, conn_str])
                choice_list.append(name)

            logger.info(f"DEBUG: Erfolgreich {len(display_list)} Verbindungen geladen")
            return display_list, choice_list

        except Exception as e:
            logger.error(f"DEBUG: Fehler beim JSON-Lesen: {e}")
            return [["FEHLER", "N/A", str(e)]], ["Fehler"]

    def _refresh_all_connection_dropdowns(self) -> Tuple[List, List, List, List]:
        """Aktualisiert ALLE Verbindungs-Dropdowns im Interface - für Tab-übergreifende Updates."""
        choices = self._get_connection_choices_sync()

        # Returniert: [DataBase Tab DataFrame, Database Tab Delete Dropdown, ETL Tab Source, ETL Tab Target]
        return (
            self._get_connections_for_display_sync(),  # Database Tab DataFrame
            choices,  # Database Tab Delete Dropdown
            choices,  # ETL Tab Source Dropdown
            choices,  # ETL Tab Target Dropdown
        )

    def _refresh_connection_choices_etl_only(self) -> Tuple[List, List]:
        """Aktualisiert nur die ETL Tab Dropdowns - ULTRA-SCHNELL."""
        choices = self._get_connection_choices_sync()
        return choices, choices

    def add_connection_simple(self, name: str, db_type: str, conn_string: str) -> str:
        """Ultra-einfaches synchrones Hinzufügen - SOFORT ohne async"""
        logger.info(f"DEBUG: add_connection_simple gestartet für '{name}'")

        if not name or not conn_string:
            logger.info("DEBUG: Name oder Connection String fehlt")
            return "❌ Name und Connection String sind erforderlich."

        try:
            # Direkte, synchrone Speicherung
            logger.info("DEBUG: Rufe db_manager.add_connection_simple auf")
            success = self.db_manager.add_connection_simple(name, db_type, conn_string)

            if success:
                status_msg = f"✅ Verbindung '{name}' sofort gespeichert!"
                logger.info(f"DEBUG: [OK] Verbindung '{name}' sofort gespeichert!")
                return status_msg
            else:
                msg = f"❌ Verbindung '{name}' existiert bereits oder Fehler beim Speichern."
                logger.info(f"DEBUG: [ERROR] Verbindung '{name}' existiert bereits")
                return msg

        except Exception as e:
            msg = f"❌ Unerwarteter Fehler: {e}"
            logger.error(f"DEBUG: [ERROR] Fehler beim einfachen Hinzufuegen: {e}")
            return msg

    def _get_connections_for_display_sync(self) -> List[List[str]]:
        """ULTRA-SCHNELLE synchrone Version - liest direkt aus JSON"""
        try:
            # Lese direkt aus der JSON-Datei - KEINE Datenbankmanager-Aufrufe
            if not os.path.exists(self.db_manager.config_file):
                return [["Keine Verbindungen", "N/A", "JSON-Datei nicht gefunden"]]

            with open(self.db_manager.config_file, "r", encoding="utf-8") as f:
                connections_data = json.load(f)

            display_list = []
            for name, config in connections_data.items():
                db_type = config.get("type", "unknown")
                conn_string = config.get("connection_string", "")

                # Anonymisiere Passwort für Anzeige
                if "@" in conn_string and ":" in conn_string:
                    parts = conn_string.split("@")
                    if len(parts) > 1:
                        protocol_user = parts[0].split(":")
                        if len(protocol_user) >= 3:
                            protocol = protocol_user[0]
                            user = protocol_user[1].split("//")[-1]
                            host_part = parts[1]
                            conn_string = f"{protocol}://{user}:***@{host_part}"

                display_list.append([name, db_type.upper(), conn_string])

            if not display_list:
                return [["Keine Verbindungen", "N/A", "Keine Daten in JSON"]]

            return display_list

        except Exception as e:
            logger.error(f"Fehler beim direkten JSON-Lesen: {e}")
            return [["FEHLER", "N/A", str(e)]]

    def _get_connection_choices_sync(self) -> List[str]:
        """ULTRA-SCHNELLE synchrone Version - liest direkt aus JSON"""
        try:
            if not os.path.exists(self.db_manager.config_file):
                return ["Keine Verbindungen"]

            with open(self.db_manager.config_file, "r", encoding="utf-8") as f:
                connections_data = json.load(f)

            choices = list(connections_data.keys())
            return choices if choices else ["Keine Verbindungen"]

        except Exception as e:
            logger.error(f"Fehler beim direkten JSON-Lesen für Choices: {e}")
            return ["Fehler beim Laden"]

    def _get_fresh_connections(self):
        """Lädt immer die aktuellsten Verbindungen direkt aus der JSON-Datei."""
        try:
            json_file = "db_connections.json"
            if not os.path.exists(json_file):
                return []

            with open(json_file, "r", encoding="utf-8") as f:
                connections_data = json.load(f)

            fresh_choices = list(connections_data.keys())
            logger.info(f"Fresh connections geladen: {fresh_choices}")
            return fresh_choices if fresh_choices else []
        except Exception as e:
            logger.error(f"Fehler beim Laden der frischen Verbindungen: {e}")
            return []


def launch():
    """Startet das Gradio Interface."""
    # Stelle sicher, dass eine Instanz des Managers erstellt wird
    # und über die Klasse geteilt wird.
    interface_manager = ETLGradioInterface()
    interface = interface_manager.create_interface()
    interface.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        debug=True,
    )


if __name__ == "__main__":
    launch()
