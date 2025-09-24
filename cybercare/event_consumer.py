import argparse
import json
import sqlite3
from datetime import datetime
from typing import Dict, Any

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ValidationError


class EventModel(BaseModel):
    """The service should only accept payloads matching this JSON template:"""
    event_type: str
    event_payload: str


class DatabaseManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_path = config.get('path', 'events.db')
        self.init_database()

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS events
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           event_type
                           TEXT
                           NOT
                           NULL,
                           event_payload
                           TEXT
                           NOT
                           NULL,
                           received_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           created_at
                           DATETIME
                           DEFAULT
                           CURRENT_TIMESTAMP
                       )
                       ''')

        conn.commit()
        conn.close()
        print(f'Database initialized: {self.db_path}')

    def save_event(self, event_type: str, event_payload: str) -> int:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        current_time = datetime.now().isoformat()

        cursor.execute('''
                       INSERT INTO events (event_type, event_payload, received_at)
                       VALUES (?, ?, ?)
                       ''', (event_type, event_payload, current_time))

        event_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return event_id


class EventConsumer:
    """The second service (Event consumer)"""

    def __init__(self, config_file="config.yaml"):
        self.config = self.load_config(config_file)
        self.db_manager = DatabaseManager(self.config['database'])
        self.app = FastAPI(title="Event Consumer", version="1.0.0")
        self.setup_routes()

    def load_config(self, config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                return config['event_consumer']
        except FileNotFoundError:
            print(f'Configuration file {config_file} not found. Using default values.')
            return {
                "port": 8001,
                "database": {
                    "type": "sqlite",
                    "path": "events.db"
                }
            }

    def setup_routes(self):
        @self.app.post("/event")
        async def receive_event(request: Request):
            """The service should expose a HTTP API endpoint that accepts incoming POST requests on the path `/event`"""
            try:
                event_data = await request.json()

                try:
                    event = EventModel(**event_data)
                except ValidationError as e:
                    error_msg = f'Invalid data format: {str(e)}'
                    print(f'Validation error: {error_msg}')
                    print(f'Received data: {event_data}')
                    raise HTTPException(
                        status_code=400,
                        detail=error_msg
                    )

                event_id = self.db_manager.save_event(
                    event.event_type,
                    event.event_payload
                )

                print(f'Event saved (ID: {event_id}): {event.event_type} - {event.event_payload}')

                return {
                    "status": "success",
                    "message": "Event saved successfully",
                    "event_id": event_id
                }

            except json.JSONDecodeError:
                error_msg = "Invalid JSON format"
                print(f'JSON error: {error_msg}')
                raise HTTPException(
                    status_code=400,
                    detail=error_msg
                )
            except HTTPException:
                raise
            except Exception as e:
                error_msg = f'Internal server error: {str(e)}'
                print(f'Unknown error: {error_msg}')
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error"
                )

        @self.app.get("/")
        async def root():
            return {
                "service": "Event Consumer",
                "status": "running",
                "database": self.config['database']['path']
            }


def parse_arguments():
    """Processes command line arguments"""
    parser = argparse.ArgumentParser(description='Event consumer')
    parser.add_argument('--config', default='config.yaml',
                        help='Configuration file path')
    parser.add_argument('--port', type=int,
                        help='Server port (overrides configuration)')
    parser.add_argument('--db-path',
                        help='Database file path (overrides configuration)')
    return parser.parse_args()


def main():
    args = parse_arguments()
    consumer = EventConsumer(args.config)

    # The persistent storage parameters for the incoming payloads should be configurable via a configuration file
    if args.port:
        consumer.config['port'] = args.port
    if args.db_path:
        consumer.config['database']['path'] = args.db_path
        consumer.db_manager = DatabaseManager(consumer.config['database'])

    port = consumer.config['port']

    print(f'Starting event consumer...')
    print(f'Server: http://localhost:{port}')
    print(f'API endpoint: http://localhost:{port}/event')
    print(f'Database: {consumer.config['database']['path']}')

    uvicorn.run(
        consumer.app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )


if __name__ == "__main__":
    main()
