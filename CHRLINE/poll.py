import threading
import traceback
from typing import Callable, Generator, Optional

from CHRLINE.services.thrift.ttypes import OpType


class Poll(object):
    op_interrupt: dict[int, Callable[[dict, object], None]]

    def __init__(self) -> None:
        self.subscriptionId = 0
        self.eventSyncToken = None
        self.op_interrupt = {}

    def __fetchOps(self, count: int = 100) -> Generator[dict, None, None]:
        fetchOps = self.fetchOps
        if self.DEVICE_TYPE in self.SYNC_SUPPORT:
            fetchOps = self.sync
        ops = fetchOps(self.revision, count)
        if "error" in ops:
            raise Exception(ops["error"])
        for op in ops:
            op_type = self.checkAndGetValue(op, "type", 3)
            if op_type != -1:
                self.setRevision(self.checkAndGetValue(op, "revision", 1))
            yield op

    def __fetchMyEvents(
        self, count: int = 100, initLastSyncToken: bool = False
    ) -> Generator[dict, None, None]:
        resp = self.fetchMyEvents(
            self.subscriptionId, self.eventSyncToken, count
        )
        events = self.checkAndGetValue(resp, "events", 2)
        for event in events:
            syncToken = self.checkAndGetValue(event, "syncToken", 5)
            self.setEventSyncToken(syncToken)
            yield event
        if initLastSyncToken:
            syncToken = self.checkAndGetValue(resp, "syncToken", 3)
            self.setEventSyncToken(syncToken)

    def __execute(
        self, op: dict, func: Callable[[dict, object], None]
    ) -> None:
        try:
            func(op, self)
        except Exception:
            self.log(traceback.format_exc())

    def setRevision(self, revision: Optional[int]) -> None:
        if revision is None:
            self.log("revision is None!!")
            revision = 0
        self.revision = max(revision, self.revision)

    def setEventSyncToken(self, syncToken: Optional[int]) -> None:
        if syncToken is None:
            self.log("syncToken is None!!")
            syncToken = 0
        if self.eventSyncToken is None:
            self.eventSyncToken = syncToken
        else:
            self.eventSyncToken = max(int(syncToken), int(self.eventSyncToken))

    def add_op_interrupt(
        self, op_type: int, func: Callable[[dict, object], None]
    ) -> None:
        self.op_interrupt[op_type] = func

    def add_op_interrupt_with_dict(
        self, d: dict[int, Callable[[dict, object], None]]
    ) -> None:
        self.op_interrupt.update(d)

    def trace(self, isThreading: bool = True) -> None:
        while self.is_login:
            for op in self.__fetchOps():
                op_type: int = self.checkAndGetValue(op, "type", "val_3", 3)
                if op_type in [-1, OpType.END_OF_OPERATION]:
                    continue

                func = self.op_interrupt.get(op_type)
                if func is None:
                    continue

                if isThreading:
                    _td = threading.Thread(
                        target=self.__execute, args=(op, func)
                    )
                    _td.daemon = True
                    _td.start()
                else:
                    self.__execute(op, func)
