from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from encadeador.modelos.runstatus import RunStatus


class Run(BaseModel):
    """
    Class for defining the execution of a case, specifying the
    version of model that was used and the run status.
    """

    runId: Optional[int]
    status: Optional[RunStatus]
    name: Optional[str]
    jobId: Optional[str]
    jobWorkingDirectory: Optional[str]
    jobStartTime: Optional[datetime]
    jobEndTime: Optional[datetime]
    jobReservedSlots: Optional[int]
    jobArgs: Optional[List[str]]
    programName: Optional[str]
    programVersion: Optional[str]

    @property
    def active(self) -> bool:
        return self.status not in [
            RunStatus.SUCCESS,
            RunStatus.INFEASIBLE,
            RunStatus.DATA_ERROR,
            RunStatus.RUNTIME_ERROR,
            RunStatus.COMMUNICATION_ERROR,
            RunStatus.UNKNOWN,
        ]
