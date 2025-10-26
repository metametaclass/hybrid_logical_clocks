import typing
from enum import Enum
from collections import deque
import heapq


class HLCMergerKind(Enum):
    """Enumeration for different kinds of HLCMerger events."""
    LOCAL = 1
    SEND = 2
    RECEIVE = 3


class HLCMergerEvent:
    """Class representing an event with physical timestamp and original data."""
    def __init__(self, timestamp, kind: HLCMergerKind, sender, message_id, local_index: int, data):
        self.timestamp = timestamp
        self.kind = kind
        self.sender = sender
        self.message_id = message_id
        self.local_index = local_index
        self.data = data

    def __str__(self):
        return f"HLCMergerEvent(timestamp={self.timestamp}, kind={self.kind}, sender={self.sender}, message_id={self.message_id} data={self.data})"  # noqa E501

    def as_dict(self):
        result = {
            'timestamp': self.timestamp,
            'kind': self.kind.name,
            'sender': self.sender,
            'message_id': self.message_id,
            'local_index': self.local_index,
        }
        result.update(self.data.as_dict())
        return result


class MergerOutputEvent:
    def __init__(self, hybrid_clock_l, source, event: HLCMergerEvent):
        self.source_id = source
        self.hybrid_clock_l = hybrid_clock_l
        self.event = event

    def __str__(self):
        return f"MergerOutputEvent(source_id={self.source_id}, hybrid_clock_l={self.hybrid_clock_l}, event={self.event})"

    def as_dict(self):
        result = {
            'source_id': self.source_id,
            'hybrid_clock_l': self.hybrid_clock_l,
        }
        result.update(self.event.as_dict())
        return result


class HLCMergerState:
    """Class representing the state of node in the HLCMerger."""
    def __init__(self, source_id: str, time_koef_a: float = 1.0, time_offset_b: float = 0.0):
        self.source_id = source_id
        self.time_koef_a = time_koef_a
        self.time_offset_b = time_offset_b
        self.clock_l = -1.0
        self.clock_c = 0
        self.queue = deque()

    def project_time(self, physical_time: float) -> float:
        """Project the physical time using the linear transformation."""
        return self.time_koef_a * physical_time + self.time_offset_b

    def assign_clock(self, clock_l: float, clock_c: int):
        self.clock_l = clock_l
        self.clock_c = clock_c


class HLCMerger():
    """Class to merge events with Hybrid Logical Clocks from different sources."""
    def __init__(self, sources):
        self.sources = sources
        # TODO: guest initial time offset
        self.states = {source.id: HLCMergerState(source.id) for source in sources}
        self.send_ids = {}
        self.min_heap = []
        self.EPSILON = 1e-6

    def _push_heap(self, clock_l: float, clock_c: int, source: str, event: HLCMergerEvent):
        """Push an event onto the min-heap for merging.
        Args:
            clock_l (float): The hybrid physical clock part of the event.
            clock_c (int): The hybrid logical clock part of the event.
            source (str): The source identifier of the event.
            local_event_idx (int): The local index of the event in its source.
            event (HLCMergerEvent): The event to be pushed onto the heap.
        """
        heapq.heappush(self.min_heap, (clock_l, clock_c, source, event.local_index, event))

    def get_next_event(self, source) -> typing.Tuple[HLCMergerState, typing.Optional[HLCMergerEvent]]:
        """Fetch the next event from the given source or its buffer."""
        state = self.states.get(source.id, None)
        if state is None:
            raise ValueError(f"Unknown source id: {source.id}")
        event = None
        if state.queue:
            # peek at the next event
            event = state.queue[0]
        else:
            event = source.fetch()
            if event is not None:
                state.queue.append(event)
        # event is left/front element in the queue
        return state, event

    def low_watermark(self):
        """Calculate the low watermark across all sources."""
        lw = None
        for source in self.sources:
            state, event = self.get_next_event(source)
            if event is None:
                continue
            projected_time = state.project_time(event.timestamp)
            if lw is None:
                lw = projected_time
            else:
                lw = min(lw, projected_time)
        return lw

    # def _get_key(heap_item):
    #     """Extract the unique key from a heap item."""
    #     clock_l, clock_c, source, local_index, _ = heap_item
    #     return (clock_l, clock_c, source, local_index)

    def _send_ids_key(event):
        """Extract the unique key for send_ids dictionary."""
        return (event.sender, event.message_id)

    def step(self):
        """Perform a single step of merging events from all sources."""
        while True:
            argmin_local_time = None
            argmin_state = None
            for source in self.sources:
                state, event = self.get_next_event(source)
                if event is None:
                    # no data in buffers or input queue
                    continue
                if argmin_local_time is None:
                    argmin_state = state
                    argmin_local_time = event.timestamp
                elif event is not None and event.timestamp < argmin_local_time:
                    argmin_state = state
                    argmin_local_time = event.timestamp
            if argmin_state is None:
                # no events
                return

            event: HLCMergerEvent = argmin_state.queue.popleft()
            if event.kind == HLCMergerKind.SEND:
                clock_l = max(argmin_state.project_time(event.timestamp), argmin_state.clock_l)
                # TODO: use clearer logic for clock_c increment instead of clever `and` tricks
                clock_c = (clock_l == argmin_state.clock_l) and (argmin_state.clock_c + 1) or 0
                argmin_state.assign_clock(clock_l, clock_c)
                self.send_ids[HLCMerger._send_ids_key(event)] = (argmin_state.clock_l, argmin_state.clock_c)
                self._push_heap(clock_l, clock_c, argmin_state.source_id, event)
            elif event.kind == HLCMergerKind.RECEIVE:
                send_clock = self.send_ids.get(HLCMerger._send_ids_key(event), None)
                if send_clock is None:
                    argmin_state.queue.appendleft(event)
                    continue  # wait for send event

                send_clock_l, send_clock_c = send_clock
                projected_time = argmin_state.project_time(event.timestamp)
                # discipline: enforce pr >= ls + eps by bumping b_i if needed
                if projected_time < send_clock_l + self.EPSILON:
                    argmin_state.time_offset_b += (send_clock_l + self.EPSILON - projected_time)  # / argmin_state.time_koef_a ?
                    projected_time = argmin_state.project_time(event.timestamp)

                # Original HLC logic for reference: https://cse.buffalo.edu/tech-reports/2014-04.pdf
                # pr = projected_time
                # ls = send_clock_l
                # state[i] = argmin_state
                # cs = send_clock_c
                #
                # l_candidate = max(pr, ls)
                # l = max(l_candidate, state[i].l)
                # if l == ls and l > pr: c = cs + 1
                # elif l == state[i].l:  c = state[i].c + 1
                # else:                  c = 0

                # NOTE: projected_time used in calculation of clock_l
                clock_l = max(argmin_state.clock_l, send_clock_l, projected_time)
                if clock_l == argmin_state.clock_l and clock_l == send_clock_l:
                    clock_c = max(argmin_state.clock_c, send_clock_c) + 1
                elif clock_l == argmin_state.clock_l:
                    clock_c = argmin_state.clock_c + 1
                elif clock_l == send_clock_l:
                    clock_c = send_clock_c + 1
                else:
                    clock_c = 0

                argmin_state.assign_clock(clock_l, clock_c)
                self._push_heap(clock_l, clock_c, argmin_state.source_id, event)
            elif event.kind == HLCMergerKind.LOCAL:
                clock_l = max(argmin_state.project_time(event.timestamp), argmin_state.clock_l)
                clock_c = (clock_l == argmin_state.clock_l) and (argmin_state.clock_c + 1) or 0
                argmin_state.assign_clock(clock_l, clock_c)
                self._push_heap(clock_l, clock_c, argmin_state.source_id, event)
            else:
                raise ValueError(f"Unknown event kind: {event.kind}")

            lw = self.low_watermark()
            if lw is None:
                # print("HLCMerger: low watermark is None, return...")
                continue
            while self.min_heap and (self.min_heap[0][0] <= lw):
                clock, _, source, _, event = heapq.heappop(self.min_heap)
                yield MergerOutputEvent(clock, source, event)
