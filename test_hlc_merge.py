import typing
from enum import Enum
from hlc_merger import HLCMerger, HLCMergerEvent, HLCMergerKind
import random
import json
import argparse
from collections import deque

from hlc_merger.hlc_merger import MergerOutputEvent


class TimeSource:
    def __init__(self):
        self.state: float = 0.0


class NetworkChannelParams:
    def __init__(self, base_latency_ms: float = 100.0, jitter_ms: float = 20.0, packet_loss_rate: float = 0.01):
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.packet_loss_rate = packet_loss_rate


class NetworkPacket:
    def __init__(self, packet_id: int, sender_id: str, timestamp: float):
        self.packet_id = packet_id
        self.sender_id = sender_id
        self.sender_timestamp = timestamp

    def __str__(self):
        return f"NetworkPacket(packet_id={self.packet_id}, sender_id={self.sender_id}, sender_timestamp={self.sender_timestamp})"

    def as_dict(self):
        return {
            'packet_id': self.packet_id,
            'sender_id': self.sender_id,
            'sender_timestamp': self.sender_timestamp
        }


class NetworkChannelEmulator:
    def __init__(self, random_gen, sender, receiver, params: NetworkChannelParams):
        self.time_source = TimeSource()
        self.random_gen = random_gen
        self.sender = sender
        self.receiver = receiver
        self.params = params
        self.sent_packets = 0
        self.dropped_packets = 0
        self.received_packets = 0
        self.scheduled_packets: typing.List[typing.Tuple[float, NetworkPacket]] = []

    def send(self, packet: NetworkPacket):
        self.sent_packets += 1
        r = self.random_gen.random()
        if r < self.params.packet_loss_rate:
            self.dropped_packets += 1
            # drop packet
            return
        latency = self.params.base_latency_ms + self.random_gen.uniform(-self.params.jitter_ms, self.params.jitter_ms)
        if latency < 0.0:
            print("Warning: negative latency adjusted to zero")
            latency = 0.0
        delivery_time = self.time_source.state + latency / 1000.0
        self.scheduled_packets.append((delivery_time, packet))

    def recv(self) -> typing.List[NetworkPacket]:
        current_time = self.time_source.state
        ready_packets = [p for (t, p) in self.scheduled_packets if t <= current_time]
        self.scheduled_packets = [(t, p) for (t, p) in self.scheduled_packets if t > current_time]
        self.received_packets += len(ready_packets)
        return ready_packets

    def get_channel_stats(self):
        return {
            'sent_packets': self.sent_packets,
            'dropped_packets': self.dropped_packets,
            'received_packets': self.received_packets,
            'pending_packets': len(self.scheduled_packets)
        }

    def has_pending_data(self) -> bool:
        return len(self.scheduled_packets) > 0


class FrameTickData:
    def __init__(self, frame_counter: int, delta_seconds: float, real_delta_seconds: float):
        self.frame_counter = frame_counter
        self.delta_seconds = delta_seconds
        self.real_delta_seconds = real_delta_seconds

    def __str__(self):
        return f"FrameTickData(frame_counter={self.frame_counter}, delta_seconds={self.delta_seconds} , real_delta_seconds={self.real_delta_seconds})"  # noqa E501

    def as_dict(self):
        return {
            'frame_counter': self.frame_counter,
            'delta_seconds': self.delta_seconds,
            'real_delta_seconds': self.real_delta_seconds
        }


class TraceEventKind(Enum):
    NetworkPacketReceived = 1
    FrameTick = 2
    NetworkPacketSent = 3


class TraceEvent:
    def __init__(self, kind: TraceEventKind, node_id: str,
                 event_counter: int, frame_counter: int,
                 timestamp: float, data: typing.Any):
        self.kind = kind
        self.node_id = node_id
        self.event_counter = event_counter
        self.frame_counter = frame_counter
        self.timestamp = timestamp
        self.data = data

    def __str__(self):
        return f"TraceEvent(kind={self.kind}, node_id={self.node_id}, event_counter={self.event_counter}, frame_counter={self.frame_counter}, timestamp={self.timestamp}, data={self.data})"  # noqa E501

    def as_dict(self):
        result = {
            'kind': self.kind.name,
            'node_id': self.node_id,
            'event_counter': self.event_counter,
            'frame_counter': self.frame_counter,
            'timestamp': self.timestamp
        }
        result.update(self.data.as_dict())
        return result


class NodeSchedulerParams:
    def __init__(self,
                 start_offset: float = 0.0,
                 fps: float = 60.0,
                 tick_fast_correction: float = -0.006,
                 tick_slow_correction: float = 0.003):
        if fps <= 0.0:
            raise ValueError("FPS must be positive")
        if start_offset < 0.0:
            raise ValueError("start_offset must be non-negative")
        if tick_fast_correction >= 0.0:
            raise ValueError("tick_fast_correction must be negative")
        if tick_slow_correction <= 0.0:
            raise ValueError("tick_slow_correction must be positive")
        self.start_offset = start_offset
        self.fps = fps
        self.target_delta_seconds = 1.0 / fps
        self.tick_fast_correction = tick_fast_correction
        self.tick_slow_correction = tick_slow_correction

    def get_delta_seconds(self, random_gen) -> float:
        delta_seconds = self.target_delta_seconds + random_gen.uniform(self.tick_fast_correction,
                                                                       self.tick_slow_correction)
        # calculate expected delta seconds: wait if tick is too fast and use actual value if tick too long
        if delta_seconds <= self.target_delta_seconds:
            real_delta_seconds = self.target_delta_seconds
        else:
            real_delta_seconds = delta_seconds
        return delta_seconds, real_delta_seconds


class Node:
    def __init__(self, node_id: str, random_gen: random.Random, scheduler_params: NodeSchedulerParams):  # noqa E501
        self.time_source = TimeSource()
        self.node_id = node_id
        self.random_gen = random_gen
        self.scheduler_params = scheduler_params
        self.target_time = scheduler_params.start_offset
        self.send_channels: typing.List[NetworkChannelEmulator] = []
        self.receiver_channels: typing.List[NetworkChannelEmulator] = []
        self.packet_id = 0
        self.event_counter = 0
        self.frame_counter = 0
        self.event_queue = deque()
        self.last_event_timestamp = None

    def get_packet_count(self):
        r = self.random_gen.random()
        PACKET_COUNT_THRESHOLD_0 = 0.10
        PACKET_COUNT_THRESHOLD_1 = 0.90
        if r < PACKET_COUNT_THRESHOLD_0:
            return 0
        elif r < PACKET_COUNT_THRESHOLD_1:
            return 1
        else:
            return 2

    def create_event(self, event_kind: TraceEventKind, event_timestamp: float, data: typing.Any):
        self.event_counter += 1
        return TraceEvent(event_kind, self.node_id, self.event_counter, self.frame_counter, event_timestamp, data)

    def tick(self, is_stopping: bool) -> typing.Generator[TraceEvent, None, None]:
        # skip ticks until time source reaches scheduled target time
        if self.target_time > self.time_source.state:
            return []

        # receive network packets
        for channel in self.receiver_channels:
            # channel used world time source to simulate network delay
            for network_packet in channel.recv():
                yield self.create_event(TraceEventKind.NetworkPacketReceived, self.time_source.state, network_packet)

        delta_seconds, real_delta_seconds = self.scheduler_params.get_delta_seconds(self.random_gen)
        if delta_seconds <= 0.0 or real_delta_seconds <= 0.0:
            raise ValueError("Delta seconds must be positive")

        yield self.create_event(TraceEventKind.FrameTick, self.time_source.state + delta_seconds * 0.1,
                                FrameTickData(self.frame_counter, delta_seconds, real_delta_seconds))

        start_tick_time = self.time_source.state
        end_tick_time = start_tick_time + delta_seconds

        # stop sending packets if simulation is stopping
        if not is_stopping:
            packet_send_idx = 0
            packet_send_time = end_tick_time - delta_seconds * 0.1
            for channel in self.send_channels:
                packet_count = self.get_packet_count()
                for i in range(packet_count):
                    self.packet_id += 1
                    # use small offset to differentiate multiple packets sent in the same tick
                    sent_time = packet_send_time + packet_send_idx * 0.0001
                    packet_send_idx += 1
                    packet = NetworkPacket(self.packet_id, self.node_id, sent_time)
                    channel.send(packet)
                    yield self.create_event(TraceEventKind.NetworkPacketSent, sent_time, packet)

        # simulate tick rate scheduler
        self.target_time = start_tick_time + real_delta_seconds

        self.frame_counter += 1

    def get_target_time(self) -> float:
        return self.target_time

    def get_remaining_time(self) -> float:
        return max(0.0, self.target_time - self.time_source.state)

    def has_pending_data(self) -> bool:
        return False


class QueueSource:
    """Wrapper to use a deque as a source for HLCMerger and validate order."""
    def __init__(self, id: str, queue: typing.Deque[TraceEvent]):
        self.id = id
        self.queue = queue
        self.last_event_timestamp = None
        self.local_index = 0

    def EventKindToHLCMergerKind(self, event: TraceEvent) -> HLCMergerKind:
        if event.kind == TraceEventKind.FrameTick:
            return HLCMergerKind.LOCAL
        elif event.kind == TraceEventKind.NetworkPacketSent:
            return HLCMergerKind.SEND
        elif event.kind == TraceEventKind.NetworkPacketReceived:
            return HLCMergerKind.RECEIVE
        else:
            raise ValueError(f"Unknown TraceEventKind: {event.kind}")

    def fetch(self) -> typing.Optional[HLCMergerEvent]:
        if not self.queue:
            return None
        event: TraceEvent = self.queue.popleft()
        if self.last_event_timestamp is not None:
            if event.timestamp < self.last_event_timestamp:
                print(f"Node {self.id} event timestamp decreased: "
                      f"last={self.last_event_timestamp}, current={event.timestamp}, "
                      f"event={event}")
        self.last_event_timestamp = event.timestamp
        kind = self.EventKindToHLCMergerKind(event)
        if kind == HLCMergerKind.SEND or kind == HLCMergerKind.RECEIVE:
            message_id = event.data.packet_id
            sender = event.data.sender_id
        else:
            message_id = None
            sender = event.node_id
        hlc_event = HLCMergerEvent(event.timestamp, kind, sender, message_id, self.local_index, event)
        self.local_index += 1
        return hlc_event


class Simulation:
    def __init__(self, duration: float, seed: int, forced_tick_rate: float = 10.0, verbose: bool = False):
        self.main_time_source = TimeSource()
        self.duration = duration
        self.nodes = []
        self.network_channels = []
        self.tickers = []
        self.random = random.Random(seed)
        self.forced_delta_seconds = 1.0 / forced_tick_rate
        self.main_tick_fast_correction_koeff = 0.9
        self.main_tick_slow_correction_koeff = 1.1
        self.verbose = verbose
        self.original_events = []
        self.merged_events = []
        self.hlc_merger = None
        self.previous_hybrid_clock = None
        self.has_causality_violation = False

    def add_node(self, node_id: str, scheduler_params: NodeSchedulerParams) -> Node:
        node = Node(node_id, self.random, scheduler_params)
        self.nodes.append(node)
        self.tickers = self.nodes + self.network_channels
        return node

    def link_nodes(self, sender: Node, receiver: Node, params: NetworkChannelParams):
        channel = NetworkChannelEmulator(self.random, sender, receiver, params)
        sender.send_channels.append(channel)
        receiver.receiver_channels.append(channel)
        self.network_channels.append(channel)
        self.tickers = self.nodes + self.network_channels

    def has_pending_tickers(self) -> bool:
        for ticker in self.tickers:
            if ticker.has_pending_data():
                return True
        return False

    def _handle_merged_event(self, event: MergerOutputEvent):
        if self.previous_hybrid_clock is not None:
            if event.hybrid_clock_l < self.previous_hybrid_clock:
                print(f"Merged event timestamp decreased: "
                      f"last={self.previous_hybrid_clock}, current={event.hybrid_clock_l} "
                      f"event={event}")
                self.has_causality_violation = True
        self.previous_hybrid_clock = event.hybrid_clock_l
        self.merged_events.append(event)

    def handle_merged_events(self, flush: bool = False):
        for output_event in self.hlc_merger.step():
            self._handle_merged_event(output_event)
        if flush:
            # finish processing remaining events
            for output_event in self.hlc_merger.flush():
                self._handle_merged_event(output_event)

    def run(self):
        self.hlc_merger = HLCMerger([QueueSource(node.node_id, node.event_queue) for node in self.nodes])
        while self.main_time_source.state < self.duration or self.has_pending_tickers():
            # determine minimum remaining time to next tick
            min_remaining_time = self.forced_delta_seconds
            for node in self.nodes:
                remaining_time = node.get_remaining_time()
                if remaining_time < min_remaining_time:
                    min_remaining_time = remaining_time

            if self.verbose:
                print(f"Simulation time {self.main_time_source.state:.6f}s advancing by {min_remaining_time:.6f}s")
            for ticker in self.tickers:
                # advance each ticker time source with some random variation
                # we want to simulate that each ticker may run slightly faster or slower
                ticker.time_source.state += min_remaining_time * self.random.uniform(self.main_tick_fast_correction_koeff,
                                                                                     self.main_tick_slow_correction_koeff)

            is_stopping = self.main_time_source.state >= self.duration

            for node in self.nodes:
                for event in node.tick(is_stopping):
                    self.original_events.append(event)
                    node.event_queue.append(event)

            self.main_time_source.state += min_remaining_time

            self.handle_merged_events()
            if self.has_causality_violation:
                if self.verbose:
                    print("Causality violation detected. Stopping simulation.")
                break

        # process remaining merged events
        self.handle_merged_events(True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--events-output-file", type=str, default=None, help="Output file for simulation events in JSON format")
    parser.add_argument("--merged-events-output-file", type=str, default=None, help="Output file for merged events in JSON format")
    parser.add_argument("--seed", type=int, default=42, help="Random seed, default 42")
    parser.add_argument("--forced-tick-rate", type=int, default=10, help="Forced tick rate for the simulation trace (ticks per second), default is 10")  # noqa E501
    parser.add_argument("--target-server-fps", type=int, default=60, help="Target server FPS, default 60")
    parser.add_argument("--target-client-fps", type=int, default=60, help="Target client FPS, default 60")
    parser.add_argument("--start-delay-ms", type=int, default=500, help="Client start delay, default 500ms")
    parser.add_argument("--duration", type=int, default=120, help="Simulation duration in seconds, default 120s")
    parser.add_argument("--client-count", type=int, default=2, help="Client count, default 2")
    parser.add_argument("--server-latency-ms", type=int, default=50, help="Server->client latency, ms. Default 50")  # noqa E501
    parser.add_argument("--client-latency-ms", type=int, default=75, help="Client->server latency, ms. Default 75")  # noqa E501
    parser.add_argument("--server-jitter-ms", type=int, default=20, help="Server->client latency jitter, ms. Default 20")  # noqa E501
    parser.add_argument("--client-jitter-ms", type=int, default=35, help="Client->server latency jitter, ms. Default 35")  # noqa E501
    parser.add_argument("--server-loss-probability", type=int, default=1, help="Loss probability percentage for server to client packets, default 1%")  # noqa E501
    parser.add_argument("--client-loss-probability", type=int, default=2, help="Loss probability percentage for client to server packets, default 2%")  # noqa E501
    parser.add_argument("--tick-fast-correction-ms", type=float, default=-6, help="Tick fast correction in milliseconds, default -6ms")  # noqa E501
    parser.add_argument("--tick-slow-correction-ms", type=float, default=3, help="Tick slow correction in milliseconds, default 3ms")  # noqa E
    config, unknown = parser.parse_known_args()
    if config.verbose:
        print(config)

    simulation = Simulation(config.duration, config.seed, config.forced_tick_rate, config.verbose)
    server_scheduler_params = NodeSchedulerParams(start_offset=0.0,
                                                  fps=config.target_server_fps,
                                                  tick_fast_correction=config.tick_fast_correction_ms / 1000.0,
                                                  tick_slow_correction=config.tick_slow_correction_ms / 1000.0)
    server = simulation.add_node("Server", server_scheduler_params)
    for i in range(config.client_count):
        client_scheduler_params = NodeSchedulerParams(start_offset=config.start_delay_ms * (i+1) / 1000.0,
                                                      fps=config.target_client_fps,
                                                      tick_fast_correction=config.tick_fast_correction_ms / 1000.0,
                                                      tick_slow_correction=config.tick_slow_correction_ms / 1000.0)
        client = simulation.add_node(f"Client{i}", client_scheduler_params)
        simulation.link_nodes(
            server,
            client,
            NetworkChannelParams(base_latency_ms=config.server_latency_ms,
                                 jitter_ms=config.server_jitter_ms,
                                 packet_loss_rate=config.server_loss_probability / 100.0)
        )
        simulation.link_nodes(
            client,
            server,
            NetworkChannelParams(base_latency_ms=config.client_latency_ms,
                                 jitter_ms=config.client_jitter_ms,
                                 packet_loss_rate=config.client_loss_probability / 100.0)
        )

    simulation.run()
    if config.events_output_file:
        json.dump([e.as_dict() for e in simulation.original_events], open(config.events_output_file, "w"), indent=2)
    if config.merged_events_output_file:
        json.dump([e.as_dict() for e in simulation.merged_events], open(config.merged_events_output_file, "w"), indent=2)
    if config.verbose:
        print("----- Original Events -----")
        for event in simulation.original_events:
            print(event)
        print("----- Merged Events -----")
        for event in simulation.merged_events:
            print(event)

    print("Simulation completed.")
    if simulation.has_causality_violation:
        print("Causality violation detected in merged events!")

    print(f" Total events generated: {len(simulation.original_events)}")
    print(f" Merged events generated: {len(simulation.merged_events)}")
    for node in simulation.nodes:
        print(f"Node {node.node_id} statistics:")
        print(f" Time:{node.time_source.state:.6f} Total events: {node.event_counter}, Total frames: {node.frame_counter}")
        for channel in node.send_channels:
            print(f" Channel to {channel.receiver.node_id} statistics: {channel.get_channel_stats()}")
        for channel in node.receiver_channels:
            print(f" Channel from {channel.sender.node_id} statistics: {channel.get_channel_stats()}")

    if simulation.has_causality_violation:
        raise RuntimeError("Causality violation detected in merged events!")


if __name__ == '__main__':
    main()
