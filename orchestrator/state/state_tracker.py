import copy
import pyodbc
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List

@dataclass
class FileState:
    """
    Stateful tracking for a single file through its ORB load process
    """
    RunID: int
    FileID: bytes
    FileName_orig: str
    Source_Name: str
    Status: str
    Detected_at: datetime
    Completion_detected_at: Optional[datetime] = None
    Processing_started_at: Optional[datetime] = None
    Processing_completed_at: Optional[datetime] = None
    Control_Strategy: Optional[str] = None
    Control_File_Path: Optional[str] = None
    Current_Path: Optional[str] = None
    Processing_Path: Optional[str] = None
    Final_Path: Optional[str] = None

    # Timeout Management
    timeout_seconds = 300

    # Error Management
    error_message: Optional[str] = None
    quarentine_reason: Optional[str] = None

    def is_timeout(self, now: Optional[datetime] = None) -> bool:
        """
        Check if the file has exceeded the timeout limit waiting for completion
        """
        if self.Status.upper() != "AWAITING_COMPLETION":
            return False
        if now is None:
            now = datetime.now()

        detected = self.Detected_at
        if isinstance(detected, str):
            detected = datetime.fromisoformat(detected)

        elapsed = (now - detected).total_seconds()
        return elapsed > self.timeout_seconds


class StateTracker:
    """
    Thread-safe tracker for file processing state.

    Supports both local JSON and MSSQL admin.Files table storage.
    """

    def __init__(
        self,
        state_file: Path = Path("state_tracker.json"),
        db_connection_string: Optional[str] = None,
    ):
        self.state_file = state_file
        self.lock = threading.Lock()
        self.file_states: List[FileState] = []
        self.db_connection_string = db_connection_string
        self.db_connection = None

        if self.db_connection_string:
            self.connect_db()

    def connect_db(self):
        if self.db_connection is None:
            self.db_connection = pyodbc.connect(self.db_connection_string, autocommit=True)
        return self.db_connection

    def close_db(self):
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None

    def _row_to_filestate(self, row) -> FileState:
        return FileState(
            RunID=row.RunID,
            FileID=row.FileID,
            FileName_orig=row.FileName_orig,
            Source_Name=row.Source_Name,
            Status=row.Status,
            Detected_at=row.Detected_at,
            Completion_detected_at=row.Completion_detected_at,
            Processing_started_at=row.Processing_started_at,
            Processing_completed_at=row.Processing_completed_at,
            Control_Strategy=row.Control_Strategy,
            Control_File_Path=row.Control_File_Path,
            Current_Path=row.Current_Path,
            Processing_Path=row.Processing_Path,
            Final_Path=row.Final_Path,
        )

    def load_from_db(self) -> List[FileState]:
        """Load current state from admin.Files table."""
        if not self.db_connection:
            raise RuntimeError("Database connection is not configured")

        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            SELECT RunID, FileID, FileName_orig, Source_Name, Status,
                   Detected_at, Completion_detected_at, Processing_started_at,
                   Processing_completed_at, Control_Strategy, Control_File_Path,
                   Current_Path, Processing_Path, Final_Path
            FROM admin.Files
            """
        )

        rows = cursor.fetchall()
        with self.lock:
            self.file_states = [self._row_to_filestate(row) for row in rows]

        return self.file_states

    def get_by_fileid(self, file_id: bytes) -> Optional[FileState]:
        """Get a FileState by FileID from memory or DB."""
        with self.lock:
            for state in self.file_states:
                if state.FileID == file_id:
                    return state

        if self.db_connection:
            cursor = self.db_connection.cursor()
            cursor.execute(
                """
                SELECT RunID, FileID, FileName_orig, Source_Name, Status,
                       Detected_at, Completion_detected_at, Processing_started_at,
                       Processing_completed_at, Control_Strategy, Control_File_Path,
                       Current_Path, Processing_Path, Final_Path
                FROM admin.Files
                WHERE FileID = ?
                """,
                file_id,
            )
            row = cursor.fetchone()
            if row:
                state = self._row_to_filestate(row)
                with self.lock:
                    self.file_states.append(state)
                return state

        return None

    def upsert_file_state(self, state: FileState):
        """Insert or update a FileState in admin.Files and memory."""
        if not self.db_connection:
            raise RuntimeError("Database connection is not configured")

        cursor = self.db_connection.cursor()

        cursor.execute(
            """
            UPDATE admin.Files SET
                RunID = ?,
                FileName_orig = ?,
                Source_Name = ?,
                Status = ?,
                Detected_at = ?,
                Completion_detected_at = ?,
                Processing_started_at = ?,
                Processing_completed_at = ?,
                Control_Strategy = ?,
                Control_File_Path = ?,
                Current_Path = ?,
                Processing_Path = ?,
                Final_Path = ?
            WHERE FileID = ?
            """,
            state.RunID,
            state.FileName_orig,
            state.Source_Name,
            state.Status,
            state.Detected_at,
            state.Completion_detected_at,
            state.Processing_started_at,
            state.Processing_completed_at,
            state.Control_Strategy,
            state.Control_File_Path,
            state.Current_Path,
            state.Processing_Path,
            state.Final_Path,
            state.FileID,
        )

        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO admin.Files (
                    RunID, FileID, FileName_orig, Source_Name, Status,
                    Detected_at, Completion_detected_at, Processing_started_at,
                    Processing_completed_at, Control_Strategy, Control_File_Path,
                    Current_Path, Processing_Path, Final_Path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                state.RunID,
                state.FileID,
                state.FileName_orig,
                state.Source_Name,
                state.Status,
                state.Detected_at,
                state.Completion_detected_at,
                state.Processing_started_at,
                state.Processing_completed_at,
                state.Control_Strategy,
                state.Control_File_Path,
                state.Current_Path,
                state.Processing_Path,
                state.Final_Path,
            )

        with self.lock:
            found = False
            for idx, existing in enumerate(self.file_states):
                if existing.FileID == state.FileID:
                    self.file_states[idx] = state
                    found = True
                    break
            if not found:
                self.file_states.append(state)

    def remove_file(self, file_id: bytes):
        """Delete a file state from admin.Files and memory cache."""
        if not self.db_connection:
            raise RuntimeError("Database connection is not configured")

        cursor = self.db_connection.cursor()
        cursor.execute("DELETE FROM admin.Files WHERE FileID = ?", file_id)

        with self.lock:
            self.file_states = [s for s in self.file_states if s.FileID != file_id]

    def refresh_state(self) -> List[FileState]:
        """Reload the in-memory cache from DB state."""
        return self.load_from_db()
