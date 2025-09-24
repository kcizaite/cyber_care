# event_propagator.py
import argparse
import asyncio
import json
import random

import httpx
import yaml


class EventPropagator:
    """The first service (Event propagator)"""

    def __init__(self, config_file="config.yaml"):
        self.config = self.load_config(config_file)
        self.events = self.load_events()

    def load_config(self, config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                return config['event_propagator']
        except FileNotFoundError:
            print(f'Configuration file {config_file} not found. Using default values.')
            return {
                "interval_seconds": 5,
                "api_endpoint": "http://localhost:8001/event",
                "events_file": "events.json"
            }

    def load_events(self):
        """The predefined JSON objects(events) that can be sent should be read from a file"""
        events_file = self.config['events_file']
        try:
            with open(events_file, 'r', encoding='utf-8') as file:
                events = json.load(file)
                print(f'Loaded {len(events)} events from {events_file}')
                return events
        except FileNotFoundError:
            print(f'Events file {events_file} not found!')
            return []
        except json.JSONDecodeError:
            print(f'Error reading JSON file {events_file}')
            return []

    def get_random_event(self):
        """
        The algorithm for choosing a specific JSON object(event) to send at each period
        from all the objects read from file, should be random
        """
        if not self.events:
            return None
        return random.choice(self.events)

    async def send_event(self, event):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config['api_endpoint'],
                    json=event,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    print(f'Event sent successfully: {event}')
                else:
                    print(f'Error sending event: {response.status_code} - {response.text}')
        except Exception as e:
            print(f'Failed to send event: {e}')

    async def start(self):
        if not self.events:
            print(f'No events available to send!')
            return

        print(f'Starting event propagator')
        print(f'Sending to: {self.config['api_endpoint']}')
        print(f'Interval: {self.config['interval_seconds']} seconds')
        print(f'Available events: {len(self.events)}')

        try:
            while True:
                event = self.get_random_event()
                if event:
                    await self.send_event(event)

                # The period of time should be measured in seconds
                await asyncio.sleep(self.config['interval_seconds'])

        except KeyboardInterrupt:
            print('\nEvent propagator stopped')


def parse_arguments():
    parser = argparse.ArgumentParser(description='Event propagator')
    parser.add_argument('--config', default='config.yaml',
                        help='Configuration file path')
    parser.add_argument('--interval', type=int,
                        help='Interval in seconds (overrides configuration)')
    parser.add_argument('--endpoint',
                        help='API endpoint (overrides configuration)')
    parser.add_argument('--events-file',
                        help='Events file path (overrides configuration)')
    return parser.parse_args()


async def main():
    args = parse_arguments()
    propagator = EventPropagator(args.config)

    if args.interval:
        propagator.config['interval_seconds'] = args.interval
    if args.endpoint:
        propagator.config['api_endpoint'] = args.endpoint
    if args.events_file:
        propagator.config['events_file'] = args.events_file
        propagator.events = propagator.load_events()

    await propagator.start()


if __name__ == "__main__":
    asyncio.run(main())
